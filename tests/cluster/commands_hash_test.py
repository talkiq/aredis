import pytest

from yaaredis.utils import b


@pytest.mark.asyncio
async def test_hget_and_hset(r):
    await r.flushdb()
    await r.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert await r.hget('a', '1') == b('1')
    assert await r.hget('a', '2') == b('2')
    assert await r.hget('a', '3') == b('3')

    # field was updated, redis returns 0
    assert await r.hset('a', '2', 5) == 0
    assert await r.hget('a', '2') == b('5')

    # field is new, redis returns 1
    assert await r.hset('a', '4', 4) == 1
    assert await r.hget('a', '4') == b('4')

    # key inside of hash that doesn't exist returns null value
    assert await r.hget('a', 'b') is None


@pytest.mark.asyncio
async def test_hdel(r):
    await r.flushdb()
    await r.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert await r.hdel('a', '2') == 1
    assert await r.hget('a', '2') is None
    assert await r.hdel('a', '1', '3') == 2
    assert await r.hlen('a') == 0


@pytest.mark.asyncio
async def test_hexists(r):
    await r.flushdb()
    await r.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert await r.hexists('a', '1')
    assert not await r.hexists('a', '4')


@pytest.mark.asyncio
async def test_hgetall(r):
    await r.flushdb()
    h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
    await r.hmset('a', h)
    assert await r.hgetall('a') == h


@pytest.mark.asyncio
async def test_hincrby(r):
    await r.flushdb()
    assert await r.hincrby('a', '1') == 1
    assert await r.hincrby('a', '1', amount=2) == 3
    assert await r.hincrby('a', '1', amount=-2) == 1


@pytest.mark.asyncio
async def test_hincrbyfloat(r):
    await r.flushdb()
    assert await r.hincrbyfloat('a', '1') == 1.0
    assert await r.hincrbyfloat('a', '1') == 2.0
    assert await r.hincrbyfloat('a', '1', 1.2) == 3.2


@pytest.mark.asyncio
async def test_hkeys(r):
    await r.flushdb()
    h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
    await r.hmset('a', h)
    local_keys = list(iter(h.keys()))
    remote_keys = await r.hkeys('a')
    assert sorted(local_keys) == sorted(remote_keys)


@pytest.mark.asyncio
async def test_hlen(r):
    await r.flushdb()
    await r.hmset('a', {'1': 1, '2': 2, '3': 3})
    assert await r.hlen('a') == 3


@pytest.mark.asyncio
async def test_hmget(r):
    await r.flushdb()
    assert await r.hmset('a', {'a': 1, 'b': 2, 'c': 3})
    assert await r.hmget('a', 'a', 'b', 'c') == [b('1'), b('2'), b('3')]


@pytest.mark.asyncio
async def test_hmset(r):
    await r.flushdb()
    h = {b('a'): b('1'), b('b'): b('2'), b('c'): b('3')}
    assert await r.hmset('a', h)
    assert await r.hgetall('a') == h


@pytest.mark.asyncio
async def test_hsetnx(r):
    await r.flushdb()
    # Initially set the hash field
    assert await r.hsetnx('a', '1', 1)
    assert await r.hget('a', '1') == b('1')
    assert not await r.hsetnx('a', '1', 2)
    assert await r.hget('a', '1') == b('1')


@pytest.mark.asyncio
async def test_hvals(r):
    await r.flushdb()
    h = {b('a1'): b('1'), b('a2'): b('2'), b('a3'): b('3')}
    await r.hmset('a', h)
    local_vals = list(iter(h.values()))
    remote_vals = await r.hvals('a')
    assert sorted(local_vals) == sorted(remote_vals)
