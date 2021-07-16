# pylint: disable=protected-access
import yaaredis


def test_defaults():
    pool = yaaredis.ConnectionPool.from_url('unix:///socket')
    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': None,
        'password': None,
    }


def test_username():
    pool = yaaredis.ConnectionPool.from_url('unix://myusername:@/socket')
    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': 'myusername',
        'password': '',
    }


def test_password():
    pool = yaaredis.ConnectionPool.from_url('unix://:mypassword@/socket')
    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': '',
        'password': 'mypassword',
    }


def test_quoted_password():
    pool = yaaredis.ConnectionPool.from_url(
        'unix://:%2Fmypass%2F%2B word%3D%24+@/socket',
        decode_components=True)
    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': None,
        'password': '/mypass/+ word=$+',
    }


def test_username_and_password():
    pool = yaaredis.ConnectionPool.from_url(
        'unix://myusername:mypassword@/socket')
    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': 'myusername',
        'password': 'mypassword',
    }


def test_quoted_path():
    pool = yaaredis.ConnectionPool.from_url(
        'unix://:mypassword@/my%2Fpath%2Fto%2F..%2F+_%2B%3D%24ocket',
        decode_components=True)
    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/my/path/to/../+_+=$ocket',
        'db': 0,
        'username': None,
        'password': 'mypassword',
    }


def test_db_as_argument():
    pool = yaaredis.ConnectionPool.from_url('unix:///socket', db=1)
    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 1,
        'username': None,
        'password': None,
    }


def test_db_in_querystring():
    pool = yaaredis.ConnectionPool.from_url('unix:///socket?db=2', db=1)
    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 2,
        'username': None,
        'password': None,
    }


def test_extra_querystring_options():
    pool = yaaredis.ConnectionPool.from_url('unix:///socket?a=1&b=2')
    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': None,
        'password': None,
        'a': '1',
        'b': '2',
    }


def test_max_connections_querystring_option():
    pool = yaaredis.ConnectionPool.from_url(
        'unix:///socket?max_connections=32')

    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.max_connections == 32
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': None,
        'password': None,
    }


def test_max_idle_times_querystring_option():
    pool = yaaredis.ConnectionPool.from_url('unix:///socket?max_idle_time=5')

    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.max_idle_time == 5
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': None,
        'password': None,
    }


def test_idle_check_interval_querystring_option():
    pool = yaaredis.ConnectionPool.from_url(
        'unix:///socket?idle_check_interval=1')

    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.idle_check_interval == 1
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'username': None,
        'password': None,
    }


def test_reader_read_size_querystring_option():
    pool = yaaredis.ConnectionPool.from_url(
        'unix:///socket?reader_read_size=65535')

    assert pool.connection_class == yaaredis.UnixDomainSocketConnection
    assert pool.connection_kwargs == {
        'path': '/socket',
        'db': 0,
        'reader_read_size': 65535,
        'username': None,
        'password': None,
    }
