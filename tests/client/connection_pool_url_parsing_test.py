import pytest

import yaaredis
from yaaredis.exceptions import ConnectionError  # pylint: disable=redefined-builtin
from yaaredis.pool import to_bool


def test_defaults():
    pool = yaaredis.ConnectionPool.from_url('redis://localhost')
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': None,
    }


def test_hostname():
    pool = yaaredis.ConnectionPool.from_url('redis://myhost')
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'myhost',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': None,
    }


def test_quoted_hostname():
    pool = yaaredis.ConnectionPool.from_url('redis://my %2F host %2B%3D+',
                                            decode_components=True)
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'my / host +=+',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': None,
    }


def test_port():
    pool = yaaredis.ConnectionPool.from_url('redis://localhost:6380')
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6380,
        'db': 0,
        'username': None,
        'password': None,
    }


def test_username():
    pool = yaaredis.ConnectionPool.from_url('redis://myusername:@localhost')
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': 'myusername',
        'password': '',
    }


def test_password():
    pool = yaaredis.ConnectionPool.from_url('redis://:mypassword@localhost')
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': '',
        'password': 'mypassword',
    }


def test_quoted_password():
    pool = yaaredis.ConnectionPool.from_url(
        'redis://:%2Fmypass%2F%2B word%3D%24+@localhost',
        decode_components=True)
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': '/mypass/+ word=$+',
    }


def test_username_and_password():
    pool = yaaredis.ConnectionPool.from_url(
        'redis://myusername:mypassword@localhost')
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': 'myusername',
        'password': 'mypassword',
    }


def test_db_as_argument():
    pool = yaaredis.ConnectionPool.from_url('redis://localhost', db='1')
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 1,
        'username': None,
        'password': None,
    }


def test_db_in_path():
    pool = yaaredis.ConnectionPool.from_url('redis://localhost/2', db='1')
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 2,
        'username': None,
        'password': None,
    }


def test_db_in_querystring():
    pool = yaaredis.ConnectionPool.from_url('redis://localhost/2?db=3', db='1')
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 3,
        'username': None,
        'password': None,
    }


def test_extra_typed_querystring_options():
    pool = yaaredis.ConnectionPool.from_url(
        'redis://localhost/2?stream_timeout=20&connect_timeout=10')

    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 2,
        'stream_timeout': 20.0,
        'connect_timeout': 10.0,
        'username': None,
        'password': None,
    }


@pytest.mark.parametrize(
    'expected,value',
    [
        (None, None),
        (None, ''),
        (False, 0),
        (False, '0'),
        (False, 'f'),
        (False, 'F'),
        (False, 'False'),
        (False, 'n'),
        (False, 'N'),
        (False, 'No'),
        (True, 1),
        (True, '1'),
        (True, 'y'),
        (True, 'Y'),
        (True, 'Yes'),
    ])
def test_boolean_parsing(expected, value):
    assert to_bool(value) is expected


def test_invalid_extra_typed_querystring_options():
    with pytest.raises(ConnectionError) as e:
        yaaredis.ConnectionPool.from_url(
            'redis://localhost/2?stream_timeout=_&connect_timeout=abc')
        expected = 'Invalid value for `stream_timeout` in connection URL.'
        assert str(e) == expected


def test_extra_querystring_options():
    pool = yaaredis.ConnectionPool.from_url('redis://localhost?a=1&b=2')
    assert pool.connection_class == yaaredis.Connection
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
    r = yaaredis.StrictRedis.from_url('redis://myhost')
    assert r.connection_pool.connection_class == yaaredis.Connection
    assert r.connection_pool.connection_kwargs == {
        'host': 'myhost',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': None,
    }
