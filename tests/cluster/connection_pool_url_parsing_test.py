from yaaredis import StrictRedis
from yaaredis.connection import Connection
from yaaredis.connection import UnixDomainSocketConnection
from yaaredis.pool import ConnectionPool


def test_defaults():
    pool = ConnectionPool.from_url('redis://localhost')
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': None,
    }


def test_hostname():
    pool = ConnectionPool.from_url('redis://myhost')
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'myhost',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': None,
    }


def test_quoted_hostname():
    pool = ConnectionPool.from_url('redis://my %2F host %2B%3D+',
                                   decode_components=True)
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'my / host +=+',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': None,
    }


def test_port():
    pool = ConnectionPool.from_url('redis://localhost:6380')
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6380,
        'db': 0,
        'username': None,
        'password': None,
    }


def test_username():
    pool = ConnectionPool.from_url('redis://myusername:@localhost')
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': 'myusername',
        'password': '',
    }


def test_password():
    pool = ConnectionPool.from_url('redis://:mypassword@localhost')
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': '',
        'password': 'mypassword',
    }


def test_username_and_password():
    pool = ConnectionPool.from_url(
        'redis://myusername:mypassword@localhost')
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': 'myusername',
        'password': 'mypassword',
    }


def test_quoted_password():
    pool = ConnectionPool.from_url(
        'redis://:%2Fmypass%2F%2B word%3D%24+@localhost',
        decode_components=True)
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': '/mypass/+ word=$+',
    }


def test_quoted_path():
    pool = ConnectionPool.from_url(
        'unix://:mypassword@/my%2Fpath%2Fto%2F..%2F+_%2B%3D%24ocket',
        decode_components=True)
    assert pool.connection_class == UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/my/path/to/../+_+=$ocket',
        'db': 0,
        'username': None,
        'password': 'mypassword',
    }


def test_db_as_argument():
    pool = ConnectionPool.from_url('redis://localhost', db='1')
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 1,
        'username': None,
        'password': None,
    }


def test_db_in_path():
    pool = ConnectionPool.from_url('redis://localhost/2', db='1')
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 2,
        'username': None,
        'password': None,
    }


def test_db_in_querystring():
    pool = ConnectionPool.from_url('redis://localhost/2?db=3', db='1')
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 3,
        'username': None,
        'password': None,
    }


def test_extra_querystring_options():
    pool = ConnectionPool.from_url('redis://localhost?a=1&b=2')
    assert pool.connection_class == Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': None,
        'a': '1',
        'b': '2',
    }


def test_client_creates_connection_pool():
    r = StrictRedis.from_url('redis://myhost')
    assert r.connection_pool.connection_class == Connection
    assert r.connection_pool.connection_kwargs == {
        'host': 'myhost',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': None,
    }
