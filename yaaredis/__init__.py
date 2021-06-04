from pkg_resources import get_distribution
__version__ = get_distribution('yaaredis').version

from .client import StrictRedis
from .client import StrictRedisCluster
from .connection import ClusterConnection
from .connection import Connection
from .connection import UnixDomainSocketConnection
from .exceptions import AuthenticationFailureError
from .exceptions import AuthenticationRequiredError
from .exceptions import BusyLoadingError
from .exceptions import CacheError
from .exceptions import ClusterCrossSlotError
from .exceptions import ClusterDownError
from .exceptions import ClusterDownException
from .exceptions import ClusterError
from .exceptions import CompressError
from .exceptions import ConnectionError  # pylint: disable=redefined-builtin
from .exceptions import DataError
from .exceptions import ExecAbortError
from .exceptions import InvalidResponse
from .exceptions import LockError
from .exceptions import NoPermissionError
from .exceptions import NoScriptError
from .exceptions import PubSubError
from .exceptions import ReadOnlyError
from .exceptions import RedisClusterError
from .exceptions import RedisClusterException
from .exceptions import RedisError
from .exceptions import ResponseError
from .exceptions import TimeoutError  # pylint: disable=redefined-builtin
from .exceptions import WatchError
from .pool import BlockingConnectionPool
from .pool import ClusterConnectionPool
from .pool import ConnectionPool


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
    'WatchError',
]
