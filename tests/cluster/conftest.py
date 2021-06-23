import asyncio
import json
import os
import sys
from distutils.version import StrictVersion

import pytest

from yaaredis import __version__
from yaaredis import StrictRedis
from yaaredis import StrictRedisCluster

# put our path in front so we can be sure we are testing locally not against the global package
basepath = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(1, basepath)

_REDIS_VERSIONS = {}


def get_versions(**kwargs):
    key = json.dumps(kwargs)
    if key not in _REDIS_VERSIONS:
        client = _get_client(**kwargs)
        loop = asyncio.get_event_loop()
        info = loop.run_until_complete(client.info())
        _REDIS_VERSIONS[key] = {key: value['redis_version']
                                for key, value in info.items()}
    return _REDIS_VERSIONS[key]


def _get_client(cls=None, **kwargs):
    if not cls:
        cls = StrictRedisCluster

    params = {
        'startup_nodes': [{
            'host': '127.0.0.1', 'port': 7000,
        }],
        'stream_timeout': 10,
    }
    params.update(kwargs)
    return cls(**params)


def _init_mgt_client(request, cls=None, **kwargs):
    client = _get_client(cls=cls, **kwargs)
    if request:
        def teardown():
            client.connection_pool.disconnect()
        request.addfinalizer(teardown)
    return client


def skip_if_not_password_protected_nodes():
    return pytest.mark.skipif('TEST_PASSWORD_PROTECTED' not in os.environ, reason='')


def skip_if_server_version_lt(min_version):
    versions = get_versions()
    for version in versions.values():
        if StrictVersion(version) < StrictVersion(min_version):
            return pytest.mark.skipif(True, reason='')
    return pytest.mark.skipif(False, reason='')


def skip_if_redis_py_version_lt(min_version):
    version = __version__.replace('-alpha.', 'a')
    if StrictVersion(version) < StrictVersion(min_version):
        return pytest.mark.skipif(True, reason='')
    return pytest.mark.skipif(False, reason='')


def skip_python_version_lt(min_version):
    min_version = tuple(map(int, min_version.split('.')))
    check = sys.version_info[:2] < min_version
    return pytest.mark.skipif(check, reason='')


@pytest.fixture(scope='function')
def o(**kwargs):
    """
    Create a StrictRedisCluster instance with decode_responses set to True.
    """
    params = {'decode_responses': True}
    params.update(kwargs)
    return _get_client(cls=StrictRedisCluster, **params)


@pytest.fixture(scope='function')
def r(**kwargs):
    """
    Create a StrictRedisCluster instance with default settings.
    """
    return _get_client(cls=StrictRedisCluster, **kwargs)


@pytest.fixture(scope='function')
def ro(**kwargs):
    """
    Create a StrictRedisCluster instance with readonly mode
    """
    params = {'readonly': True}
    params.update(kwargs)
    return _get_client(cls=StrictRedisCluster, **params)


@pytest.fixture(scope='function')
def s(**kwargs):
    """
    Create a StrictRedisCluster instance with 'init_slot_cache' set to false
    """
    s_ = _get_client(**kwargs)
    assert s_.connection_pool.nodes.slots == {}
    assert s_.connection_pool.nodes.nodes == {}
    return s_


@pytest.fixture(scope='function')
def t(*args, **kwargs):
    """
    Create a regular StrictRedis object instance
    """
    return StrictRedis(*args, **kwargs)


@pytest.fixture(scope='function')
def sr(**kwargs):
    """
    Returns a instance of StrictRedisCluster
    """
    return _get_client(reinitialize_steps=1, cls=StrictRedisCluster, **kwargs)
