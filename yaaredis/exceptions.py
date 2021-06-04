class RedisError(Exception):
    pass


class AuthenticationFailureError(RedisError):
    pass


class AuthenticationRequiredError(RedisError):
    pass


class NoPermissionError(RedisError):
    pass


class ConnectionError(RedisError):  # pylint: disable=redefined-builtin
    pass


class TimeoutError(RedisError):  # pylint: disable=redefined-builtin
    pass


class BusyLoadingError(ConnectionError):
    pass


class InvalidResponse(RedisError):
    pass


class ResponseError(RedisError):
    pass


class DataError(RedisError):
    pass


class PubSubError(RedisError):
    pass


class WatchError(RedisError):
    pass


class NoScriptError(ResponseError):
    pass


class ExecAbortError(ResponseError):
    pass


class ReadOnlyError(ResponseError):
    pass


class LockError(RedisError, ValueError):
    """Errors acquiring or releasing a lock"""
    # NOTE: For backwards compatability, this class derives from ValueError.
    # This was originally chosen to behave like threading.Lock.


class CacheError(RedisError):
    """Basic error of yaaredis.cache"""


class SerializeError(CacheError):
    pass


class CompressError(CacheError):
    pass


class RedisClusterException(Exception):
    pass


class RedisClusterError(Exception):
    pass


class ClusterDownException(Exception):
    pass


class ClusterError(RedisError):
    pass


class ClusterCrossSlotError(ResponseError):
    message = "Keys in request don't hash to the same slot"


class ClusterDownError(ClusterError, ResponseError):
    def __init__(self, resp):
        super().__init__(resp)
        self.args = (resp,)
        self.message = resp


class ClusterTransactionError(ClusterError):
    def __init__(self, msg):
        super().__init__(msg)
        self.msg = msg


class AskError(ResponseError):
    """
    src node: MIGRATING to dst node
        get > ASK error
        ask dst node > ASKING command
    dst node: IMPORTING from src node
        asking command only affects next command
        any op will be allowed after asking command
    """

    def __init__(self, resp):
        """should only redirect to master node"""
        super().__init__(resp)
        self.args = (resp,)
        self.message = resp
        slot_id, new_node = resp.split(' ')
        host, port = new_node.rsplit(':', 1)
        self.slot_id = int(slot_id)
        self.node_addr = self.host, self.port = host, int(port)


class TryAgainError(ResponseError):
    pass


class MovedError(AskError):
    pass
