from aredis.client import (StrictRedis, StrictRedisCluster)
from aredis.connection import (
    Connection,
    UnixDomainSocketConnection,
    ClusterConnection
)
from aredis.pool import ConnectionPool, ClusterConnectionPool, BlockingConnectionPool
from aredis.exceptions import (
    AuthenticationFailureError, AuthenticationRequiredError, BusyLoadingError,
    CacheError, ClusterCrossSlotError, ClusterDownError, ClusterDownException,
    ClusterError, CompressError, ConnectionError, DataError, ExecAbortError,
    InvalidResponse, LockError, NoPermissionError, NoScriptError, PubSubError,
    ReadOnlyError, RedisClusterError, RedisClusterException, RedisError,
    ResponseError, TimeoutError, WatchError
)


__version__ = '1.1.8'

VERSION = tuple(map(int, __version__.split('.')))


__all__ = [
    'StrictRedis', 'StrictRedisCluster',
    'Connection', 'UnixDomainSocketConnection', 'ClusterConnection',
    'ConnectionPool', 'ClusterConnectionPool', 'BlockingConnectionPool',
    'AuthenticationFailureError', 'AuthenticationRequiredError',
    'BusyLoadingError', 'CacheError', 'ClusterCrossSlotError',
    'ClusterDownError', 'ClusterDownException', 'ClusterError',
    'CompressError', 'ConnectionError', 'DataError', 'ExecAbortError',
    'InvalidResponse', 'LockError', 'NoPermissionError', 'NoScriptError',
    'PubSubError', 'ReadOnlyError', 'RedisClusterError',
    'RedisClusterException', 'RedisError', 'ResponseError', 'TimeoutError',
    'WatchError'
]
