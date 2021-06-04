import ssl

import pytest

import yaaredis


def test_defaults():
    pool = yaaredis.ConnectionPool.from_url('rediss://localhost')
    assert pool.connection_class == yaaredis.Connection
    assert pool.connection_kwargs.pop('ssl_context') is not None
    assert pool.connection_kwargs == {
        'host': 'localhost',
        'port': 6379,
        'db': 0,
        'username': None,
        'password': None,
    }


@pytest.mark.asyncio()
async def test_cert_reqs_none():
    with pytest.raises(TypeError) as e:
        pool = yaaredis.ConnectionPool.from_url(
            'rediss://?ssl_cert_reqs=none&ssl_keyfile=test')
        assert e.message == 'certfile should be a valid filesystem path'
        mode = (await pool.get_connection()).ssl_context.verify_mode
        assert mode == ssl.CERT_NONE


@pytest.mark.asyncio()
async def test_cert_reqs_optional():
    with pytest.raises(TypeError) as e:
        pool = yaaredis.ConnectionPool.from_url(
            'rediss://?ssl_cert_reqs=optional&ssl_keyfile=test')
        assert e.message == 'certfile should be a valid filesystem path'
        mode = (await pool.get_connection()).ssl_context.verify_mode
        assert mode == ssl.CERT_OPTIONAL


@pytest.mark.asyncio()
async def test_cert_reqs_required():
    with pytest.raises(TypeError) as e:
        pool = yaaredis.ConnectionPool.from_url(
            'rediss://?ssl_cert_reqs=required&ssl_keyfile=test')
        assert e.message == 'certfile should be a valid filesystem path'
        mode = (await pool.get_connection()).ssl_context.verify_mode
        assert mode == ssl.CERT_REQUIRED
