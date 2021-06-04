# pylint: disable=protected-access
import re

import pytest

import yaaredis
from .conftest import skip_if_server_version_lt
from yaaredis.exceptions import BusyLoadingError
from yaaredis.exceptions import ReadOnlyError
from yaaredis.exceptions import RedisError


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_on_connect_error(event_loop):
    """
    An error in Connection.on_connect should disconnect from the server
    see for details: https://github.com/andymccurdy/redis-py/issues/368
    """
    # this assumes the Redis server being tested against doesn't have
    # 9999 databases ;)
    bad_connection = yaaredis.StrictRedis(db=9999, loop=event_loop)
    # an error should be raised on connect
    with pytest.raises(RedisError):
        await bad_connection.info()

    pool = bad_connection.connection_pool
    assert len(pool._available_connections) == 0


@skip_if_server_version_lt('2.8.8')
@pytest.mark.asyncio(forbid_global_loop=True)
async def test_busy_loading_disconnects_socket(event_loop):
    """
    If Redis raises a LOADING error, the connection should be
    disconnected and a BusyLoadingError raised
    """
    client = yaaredis.StrictRedis(loop=event_loop)
    with pytest.raises(BusyLoadingError):
        await client.execute_command('DEBUG', 'ERROR', 'LOADING fake message')

    pool = client.connection_pool
    assert len(pool._available_connections) == 0


@skip_if_server_version_lt('2.8.8')
@pytest.mark.asyncio(forbid_global_loop=True)
async def test_busy_loading_from_pipeline_immediate_command(event_loop):
    """
    BusyLoadingErrors should raise from Pipelines that execute a
    command immediately, like WATCH does.
    """
    client = yaaredis.StrictRedis(loop=event_loop)
    pipe = await client.pipeline()
    with pytest.raises(BusyLoadingError):
        await pipe.immediate_execute_command('DEBUG', 'ERROR',
                                             'LOADING fake message')

    pool = client.connection_pool
    assert not pipe.connection
    assert len(pool._available_connections) == 0


@skip_if_server_version_lt('2.8.8')
@pytest.mark.asyncio(forbid_global_loop=True)
async def test_busy_loading_from_pipeline(event_loop):
    """
    BusyLoadingErrors should be raised from a pipeline execution
    regardless of the raise_on_error flag.
    """
    client = yaaredis.StrictRedis(loop=event_loop)
    pipe = await client.pipeline()
    await pipe.execute_command('DEBUG', 'ERROR', 'LOADING fake message')
    with pytest.raises(RedisError):
        await pipe.execute()

    pool = client.connection_pool
    assert not pipe.connection
    assert len(pool._available_connections) == 1
    assert pool._available_connections[0]._writer
    assert pool._available_connections[0]._reader


@skip_if_server_version_lt('2.8.8')
@pytest.mark.asyncio(forbid_global_loop=True)
async def test_read_only_error(event_loop):
    """
    READONLY errors get turned in ReadOnlyError exceptions
    """
    client = yaaredis.StrictRedis(loop=event_loop)
    with pytest.raises(ReadOnlyError):
        await client.execute_command('DEBUG', 'ERROR', 'READONLY blah blah')


def test_connect_from_url_tcp():
    connection = yaaredis.StrictRedis.from_url('redis://localhost')
    pool = connection.connection_pool

    assert re.match('(.*)<(.*)<(.*)>>', repr(pool)).groups() == (
        'ConnectionPool',
        'Connection',
        'host=localhost,port=6379,db=0',
    )


def test_connect_from_url_unix():
    connection = yaaredis.StrictRedis.from_url('unix:///path/to/socket')
    pool = connection.connection_pool

    assert re.match('(.*)<(.*)<(.*)>>', repr(pool)).groups() == (
        'ConnectionPool',
        'UnixDomainSocketConnection',
        'path=/path/to/socket,db=0',
    )
