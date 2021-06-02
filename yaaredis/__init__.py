from yaaredis.client import (StrictRedis, StrictRedisCluster)
from yaaredis.connection import (
    Connection,
    UnixDomainSocketConnection,
    ClusterConnection
)
from yaaredis.pool import ConnectionPool, ClusterConnectionPool, BlockingConnectionPool
from yaaredis.exceptions import (
    AuthenticationFailureError, AuthenticationRequiredError, BusyLoadingError,
    CacheError, ClusterCrossSlotError, ClusterDownError, ClusterDownException,
    ClusterError, CompressError, ConnectionError, DataError, ExecAbortError,
    InvalidResponse, LockError, NoPermissionError, NoScriptError, PubSubError,
    ReadOnlyError, RedisClusterError, RedisClusterException, RedisError,
    ResponseError, TimeoutError, WatchError
)


__version__ = '2.0.0-alpha.0'

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
