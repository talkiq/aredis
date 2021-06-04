from yaaredis.connection import UnixDomainSocketConnection
from yaaredis.pool import ConnectionPool


def test_defaults():
    pool = ConnectionPool.from_url('unix:///socket')
    assert pool.connection_class == UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': None,
        'password': None,
    }


def test_username():
    pool = ConnectionPool.from_url('unix://myusername:@/socket')
    assert pool.connection_class == UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': 'myusername',
        'password': '',
    }


def test_password():
    pool = ConnectionPool.from_url('unix://:mypassword@/socket')
    assert pool.connection_class == UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': '',
        'password': 'mypassword',
    }


def test_username_and_password():
    pool = ConnectionPool.from_url('unix://myusername:mypassword@/socket')
    assert pool.connection_class == UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': 'myusername',
        'password': 'mypassword',
    }


def test_db_as_argument():
    pool = ConnectionPool.from_url('unix:///socket', db=1)
    assert pool.connection_class == UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 1,
        'username': None,
        'password': None,
    }


def test_db_in_querystring():
    pool = ConnectionPool.from_url('unix:///socket?db=2', db=1)
    assert pool.connection_class == UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 2,
        'username': None,
        'password': None,
    }


def test_extra_querystring_options():
    pool = ConnectionPool.from_url('unix:///socket?a=1&b=2')
    assert pool.connection_class == UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': None,
        'password': None,
        'a': '1',
        'b': '2',
    }
