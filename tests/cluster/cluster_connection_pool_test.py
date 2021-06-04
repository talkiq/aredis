# pylint: disable=protected-access
import asyncio
import os
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from yaaredis.connection import ClusterConnection
from yaaredis.exceptions import RedisClusterException
from yaaredis.pool import ClusterConnectionPool


class SampleConnection:
    description_format = 'SampleConnection<>'

    def __init__(self, host='localhost', port=7000, socket_timeout=None,
                 **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()
        self.host = host
        self.port = port
        self.socket_timeout = socket_timeout
        self.awaiting_response = False


async def get_pool(connection_kwargs=None, max_connections=None,
                   max_connections_per_node=None,
                   connection_class=SampleConnection):
    connection_kwargs = connection_kwargs or {}
    pool = ClusterConnectionPool(
        connection_class=connection_class,
        max_connections=max_connections,
        max_connections_per_node=max_connections_per_node,
        startup_nodes=[{'host': '127.0.0.1', 'port': 7000}],
        **connection_kwargs)
    await pool.initialize()
    return pool


@pytest.mark.asyncio()
async def test_in_use_not_exists():
    """
    Test that if for some reason, the node that it tries to get the connection
    for do not exists in the _in_use_connection variable.
    """
    pool = await get_pool()
    pool._in_use_connections = {}
    await pool.get_connection('pubsub', channel='foobar')


@pytest.mark.asyncio()
async def test_connection_creation():
    connection_kwargs = {'foo': 'bar', 'biz': 'baz'}
    pool = await get_pool(connection_kwargs=connection_kwargs)
    connection = pool.get_connection_by_node(
        {'host': '127.0.0.1', 'port': 7000})
    assert isinstance(connection, SampleConnection)
    for key in connection_kwargs:
        assert connection.kwargs[key] == connection_kwargs[key]


@pytest.mark.asyncio()
async def test_multiple_connections():
    pool = await get_pool()
    c1 = pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7000})
    c2 = pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7001})
    assert c1 != c2


@pytest.mark.asyncio()
async def test_max_connections():
    pool = await get_pool(max_connections=2)
    pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7000})
    pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7001})
    with pytest.raises(RedisClusterException):
        pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7000})


@pytest.mark.asyncio()
async def test_max_connections_per_node():
    pool = await get_pool(max_connections=2, max_connections_per_node=True)
    pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7000})
    pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7001})
    pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7000})
    pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7001})
    with pytest.raises(RedisClusterException):
        pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7000})


@pytest.mark.asyncio()
async def test_max_connections_default_setting():
    pool = await get_pool(max_connections=None)
    assert pool.max_connections == 2 ** 31


@pytest.mark.asyncio()
async def test_reuse_previously_released_connection():
    pool = await get_pool()
    c1 = pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7000})
    pool.release(c1)
    c2 = pool.get_connection_by_node({'host': '127.0.0.1', 'port': 7000})
    assert c1 == c2


@pytest.mark.asyncio()
async def test_repr_contains_db_info_tcp():
    """
    Note: init_slot_cache muts be set to false otherwise it will try to
          query the test server for data and then it can't be predicted reliably
    """
    connection_kwargs = {'host': '127.0.0.1', 'port': 7000}
    pool = await get_pool(connection_kwargs=connection_kwargs,
                          connection_class=ClusterConnection)
    expected = 'ClusterConnectionPool<ClusterConnection<host=127.0.0.1,port=7000>>'
    assert repr(pool) == expected


@pytest.mark.asyncio()
async def test_get_connection_by_key():
    """
    This test assumes that when hashing key 'foo' will be sent to server with
    port 7002
    """
    pool = await get_pool(connection_kwargs={})

    # Patch the call that is made inside the method to allow control of the
    # returned connection object
    with patch.object(ClusterConnectionPool, 'get_connection_by_slot',
                      autospec=True) as pool_mock:
        def side_effect(*_args, **_kwargs):
            return SampleConnection(port=1337)

        pool_mock.side_effect = side_effect

        connection = pool.get_connection_by_key('foo')
        assert connection.port == 1337

    with pytest.raises(RedisClusterException) as ex:
        pool.get_connection_by_key(None)
        assert str(ex.value).startswith('No way to dispatch this command to '
                                        'Redis Cluster.')


@pytest.mark.asyncio()
async def test_get_connection_by_slot():
    """
    This test assumes that when doing keyslot operation on "foo" it will
    return 12182
    """
    pool = await get_pool(connection_kwargs={})

    # Patch the call that is made inside the method to allow control of the
    # returned connection object
    with patch.object(ClusterConnectionPool, 'get_connection_by_node',
                      autospec=True) as pool_mock:
        def side_effect(*_args, **_kwargs):
            return SampleConnection(port=1337)

        pool_mock.side_effect = side_effect

        connection = pool.get_connection_by_slot(12182)
        assert connection.port == 1337

    m = Mock()
    pool.get_random_connection = m

    # If None value is provided then a random node should be tried/returned
    pool.get_connection_by_slot(None)
    m.assert_called_once_with()


@pytest.mark.asyncio()
async def test_get_connection_blocked():
    """
    Currently get_connection() should only be used by pubsub command.
    All other commands should be blocked and exception raised.
    """
    pool = await get_pool()

    with pytest.raises(RedisClusterException) as ex:
        await pool.get_connection('GET')
        assert str(ex.value).startswith("Only 'pubsub' commands can use "
                                        'get_connection()')


@pytest.mark.asyncio()
async def test_master_node_by_slot():
    pool = await get_pool(connection_kwargs={})
    node = pool.get_master_node_by_slot(0)
    node['port'] = 7000
    node = pool.get_master_node_by_slot(12182)
    node['port'] = 7002


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_connection_idle_check():
    pool = ClusterConnectionPool(
        startup_nodes=[{'host': '127.0.0.1', 'port': 7000}], max_idle_time=0.2,
        idle_check_interval=0.1)
    conn = pool.get_connection_by_node({
        'name': '127.0.0.1:7000',
        'host': '127.0.0.1',
        'port': 7000,
        'server_type': 'master',
    })
    name = conn.node['name']
    assert len(pool._in_use_connections[name]) == 1
    # not ket could be found in dict for now
    assert not pool._available_connections

    pool.release(conn)
    assert len(pool._in_use_connections[name]) == 0
    assert len(pool._available_connections[name]) == 1

    await asyncio.sleep(0.21)
    assert len(pool._in_use_connections[name]) == 0
    assert len(pool._available_connections[name]) == 0

    last_active_at = conn.last_active_at
    assert last_active_at == conn.last_active_at
    assert conn._writer is None and conn._reader is None
