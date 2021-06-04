# pylint: disable=protected-access
import asyncio
import os

import pytest

import yaaredis
from yaaredis.exceptions import ConnectionError  # pylint: disable=redefined-builtin


class SampleConnection:
    description = 'SampleConnection<>'

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.pid = os.getpid()
        self.awaiting_response = False


def get_pool(connection_kwargs=None, max_connections=None,
             connection_class=SampleConnection):
    connection_kwargs = connection_kwargs or {}
    return yaaredis.ConnectionPool(
        connection_class=connection_class,
        max_connections=max_connections,
        **connection_kwargs)


@pytest.mark.asyncio()
async def test_connection_creation():
    connection_kwargs = {'foo': 'bar', 'biz': 'baz'}
    pool = get_pool(connection_kwargs=connection_kwargs)
    connection = await pool.get_connection()
    assert isinstance(connection, SampleConnection)
    assert connection.kwargs == connection_kwargs


@pytest.mark.asyncio()
async def test_multiple_connections():
    pool = get_pool()
    c1 = await pool.get_connection()
    c2 = await pool.get_connection()
    assert c1 != c2


@pytest.mark.asyncio()
async def test_max_connections():
    pool = get_pool(max_connections=2)
    await pool.get_connection()
    await pool.get_connection()
    with pytest.raises(ConnectionError):
        await pool.get_connection()


@pytest.mark.asyncio()
async def test_reuse_previously_released_connection():
    pool = get_pool()
    c1 = await pool.get_connection()
    pool.release(c1)
    c2 = await pool.get_connection()
    assert c1 == c2


def test_repr_contains_db_info_tcp():
    connection_kwargs = {'host': 'localhost', 'port': 6379, 'db': 1}
    pool = get_pool(connection_kwargs=connection_kwargs,
                    connection_class=yaaredis.Connection)
    expected = 'ConnectionPool<Connection<host=localhost,port=6379,db=1>>'
    assert repr(pool) == expected


def test_repr_contains_db_info_unix():
    connection_kwargs = {'path': '/abc', 'db': 1}
    pool = get_pool(connection_kwargs=connection_kwargs,
                    connection_class=yaaredis.UnixDomainSocketConnection)
    expected = 'ConnectionPool<UnixDomainSocketConnection<path=/abc,db=1>>'
    assert repr(pool) == expected


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_connection_idle_check():
    rs = yaaredis.StrictRedis(host='127.0.0.1', port=6379, db=0,
                              max_idle_time=0.2, idle_check_interval=0.1)
    await rs.info()
    assert len(rs.connection_pool._available_connections) == 1
    assert len(rs.connection_pool._in_use_connections) == 0

    conn = rs.connection_pool._available_connections[0]
    last_active_at = conn.last_active_at
    await asyncio.sleep(0.3)

    assert len(rs.connection_pool._available_connections) == 0
    assert len(rs.connection_pool._in_use_connections) == 0
    assert last_active_at == conn.last_active_at
    assert conn._writer is None and conn._reader is None
