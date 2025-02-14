import os
import random
import weakref

from .client import StrictRedis
from .connection import Connection
from .exceptions import ConnectionError  # pylint: disable=redefined-builtin
from .exceptions import ReadOnlyError
from .exceptions import ResponseError
from .exceptions import TimeoutError  # pylint: disable=redefined-builtin
from .pool import ConnectionPool
from .utils import nativestr


class MasterNotFoundError(ConnectionError):
    pass


class SlaveNotFoundError(ConnectionError):
    pass


class SentinelManagedConnection(Connection):
    def __init__(self, **kwargs):
        self.connection_pool = kwargs.pop('connection_pool')
        super().__init__(**kwargs)

    def __repr__(self):
        pool = self.connection_pool
        if self.host:
            host_info = f',host={self.host},port={self.port}'
        else:
            host_info = ''
        s = f'{type(self).__name__}<service={pool.service_name}{host_info}>'
        return s

    async def connect_to(self, address):
        self.host, self.port = address
        await super().connect()
        if self.connection_pool.check_connection:
            await self.send_command('PING')
            if nativestr(await self.read_response()) != 'PONG':
                raise ConnectionError('PING failed')

    async def connect(self):
        if self._reader and self._writer:
            return  # already connected

        if self.connection_pool.is_master:
            await self.connect_to(
                await self.connection_pool.get_master_address())
        else:
            for slave in await self.connection_pool.rotate_slaves():
                try:
                    return await self.connect_to(slave)
                except ConnectionError:
                    continue
            raise SlaveNotFoundError  # Never be here

    async def read_response(self):
        try:
            return await super().read_response()
        except ReadOnlyError as e:
            if self.connection_pool.is_master:
                # When talking to a master, a ReadOnlyError when likely
                # indicates that the previous master that we're still connected
                # to has been demoted to a slave and there's a new master.
                # calling disconnect will force the connection to re-query
                # sentinel during the next connect() attempt.
                self.disconnect()
                raise ConnectionError(
                    'The previous master is now a slave') from e
            raise


class SentinelConnectionPool(ConnectionPool):
    """
    Sentinel backed connection pool.

    If ``check_connection`` flag is set to True, SentinelManagedConnection
    sends a PING command right after establishing the connection.
    """

    def __init__(self, service_name, sentinel_manager, **kwargs):
        kwargs['connection_class'] = kwargs.get(
            'connection_class', SentinelManagedConnection)
        self.is_master = kwargs.pop('is_master', True)
        self.check_connection = kwargs.pop('check_connection', False)
        super().__init__(**kwargs)
        self.connection_kwargs['connection_pool'] = weakref.proxy(self)
        self.service_name = service_name
        self.sentinel_manager = sentinel_manager

    def __repr__(self):
        kind = 'master' if self.is_master else 'slave'
        return f'{type(self).__name__}<service={self.service_name}({kind})'

    def reset(self):
        super().reset()
        self.master_address = None
        self.slave_rr_counter = None

    async def get_master_address(self):
        master_address = await self.sentinel_manager.discover_master(
            self.service_name)
        if self.is_master:
            if self.master_address is None:
                self.master_address = master_address
            elif master_address != self.master_address:
                # Master address changed, disconnect all clients in this pool
                self.disconnect()
        return master_address

    async def rotate_slaves(self):
        """Round-robin slave balancer"""
        slaves = await self.sentinel_manager.discover_slaves(self.service_name)
        slave_address = []
        if slaves:
            if self.slave_rr_counter is None:
                self.slave_rr_counter = random.randint(0, len(slaves) - 1)
            for _ in range(len(slaves)):
                self.slave_rr_counter = (
                    self.slave_rr_counter + 1) % len(slaves)
                slave_address.append(slaves[self.slave_rr_counter])
            return slave_address
        # Fallback to the master connection
        try:
            return [await self.get_master_address()]
        except MasterNotFoundError:
            pass
        raise SlaveNotFoundError(f'No slave found for {self.service_name}')

    def _checkpid(self):
        if self.pid != os.getpid():
            self.disconnect()
            self.reset()
            # TODO: wtf?
            # pylint: disable=unnecessary-dunder-call
            self.__init__(self.service_name, self.sentinel_manager,
                          is_master=self.is_master,
                          check_connection=self.check_connection,
                          connection_class=self.connection_class,
                          max_connections=self.max_connections,
                          **self.connection_kwargs)


class Sentinel:
    """
    Redis Sentinel cluster client

    from yaaredis.sentinel import Sentinel
    sentinel = Sentinel([('localhost', 26379)], stream_timeout=0.1)
    async def test():
        master = await sentinel.master_for('mymaster', stream_timeout=0.1)
        await master.set('foo', 'bar')
        slave = await sentinel.slave_for('mymaster', stream_timeout=0.1)
        await slave.get('foo')

    ``sentinels`` is a list of sentinel nodes. Each node is represented by
    a pair (hostname, port).

    ``min_other_sentinels`` defined a minimum number of peers for a sentinel.
    When querying a sentinel, if it doesn't meet this threshold, responses
    from that sentinel won't be considered valid.

    ``sentinel_kwargs`` is a dictionary of connection arguments used when
    connecting to sentinel instances. Any argument that can be passed to
    a normal Redis connection can be specified here. If ``sentinel_kwargs`` is
    not specified, any stream_timeout and socket_keepalive options specified
    in ``connection_kwargs`` will be used.

    ``connection_kwargs`` are keyword arguments that will be used when
    establishing a connection to a Redis server.
    """

    def __init__(self, sentinels, min_other_sentinels=0, sentinel_kwargs=None,
                 **connection_kwargs):
        # if sentinel_kwargs isn't defined, use the socket_* options from
        # connection_kwargs
        if sentinel_kwargs is None:
            sentinel_kwargs = {k: v for k, v in iter(connection_kwargs.items())
                               if k.startswith('socket_')}
        self.sentinel_kwargs = sentinel_kwargs

        self.sentinels = [StrictRedis(hostname, port, **self.sentinel_kwargs)
                          for hostname, port in sentinels]
        self.min_other_sentinels = min_other_sentinels
        self.connection_kwargs = connection_kwargs

    def __repr__(self):
        sentinel_addresses = []
        for sentinel in self.sentinels:
            sentinel_addresses.append(':'.join((
                sentinel.connection_pool.connection_kwargs['host'],
                sentinel.connection_pool.connection_kwargs['port'],
            )))
        return (f'{type(self).__name__}'
                f'<sentinels=[{",".join(sentinel_addresses)}]>')

    def check_master_state(self, state, service_name):
        if not state['is_master'] or state['is_sdown'] or state['is_odown']:
            return False
        # Check if our sentinel doesn't see other nodes
        if state['num-other-sentinels'] < self.min_other_sentinels:
            return False
        return True

    async def discover_master(self, service_name):
        """
        Asks sentinel servers for the Redis master's address corresponding
        to the service labeled ``service_name``.

        Returns a pair (address, port) or raises MasterNotFoundError if no
        master is found.
        """
        for sentinel_no, sentinel in enumerate(self.sentinels):
            try:
                masters = await sentinel.sentinel_masters()
            except (ConnectionError, TimeoutError):
                continue
            state = masters.get(service_name)
            if state and self.check_master_state(state, service_name):
                # Put this sentinel at the top of the list
                self.sentinels[0], self.sentinels[sentinel_no] = (
                    sentinel, self.sentinels[0])
                return state['ip'], state['port']
        raise MasterNotFoundError(
            f'No master found for {service_name!r}')

    @staticmethod
    def filter_slaves(slaves):
        """Removes slaves that are in an ODOWN or SDOWN state"""
        slaves_alive = []
        for slave in slaves:
            if slave['is_odown'] or slave['is_sdown']:
                continue
            slaves_alive.append((slave['ip'], slave['port']))
        return slaves_alive

    async def discover_slaves(self, service_name):
        """Returns a list of alive slaves for service ``service_name``"""
        for sentinel in self.sentinels:
            try:
                slaves = await sentinel.sentinel_slaves(service_name)
            except (ConnectionError, ResponseError, TimeoutError):
                continue
            slaves = self.filter_slaves(slaves)
            if slaves:
                return slaves
        return []

    def master_for(self, service_name, redis_class=StrictRedis,
                   connection_pool_class=SentinelConnectionPool, **kwargs):
        """
        Returns a redis client instance for the ``service_name`` master.

        A SentinelConnectionPool class is used to retrive the master's
        address before establishing a new connection.

        NOTE: If the master's address has changed, any cached connections to
        the old master are closed.

        By default clients will be a redis.StrictRedis instance. Specify a
        different class to the ``redis_class`` argument if you desire
        something different.

        The ``connection_pool_class`` specifies the connection pool to use.
        The SentinelConnectionPool will be used by default.

        All other keyword arguments are merged with any connection_kwargs
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        kwargs['is_master'] = True
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)
        return redis_class(connection_pool=connection_pool_class(
            service_name, self, **connection_kwargs))

    def slave_for(self, service_name, redis_class=StrictRedis,
                  connection_pool_class=SentinelConnectionPool, **kwargs):
        """
        Returns redis client instance for the ``service_name`` slave(s).

        A SentinelConnectionPool class is used to retrive the slave's
        address before establishing a new connection.

        By default clients will be a redis.StrictRedis instance. Specify a
        different class to the ``redis_class`` argument if you desire
        something different.

        The ``connection_pool_class`` specifies the connection pool to use.
        The SentinelConnectionPool will be used by default.

        All other keyword arguments are merged with any connection_kwargs
        passed to this class and passed to the connection pool as keyword
        arguments to be used to initialize Redis connections.
        """
        kwargs['is_master'] = False
        connection_kwargs = dict(self.connection_kwargs)
        connection_kwargs.update(kwargs)
        return redis_class(connection_pool=connection_pool_class(
            service_name, self, **connection_kwargs))
