import pytest

from yaaredis.utils import b


@pytest.mark.asyncio
async def test_strict_zadd(sr):
    await sr.flushdb()
    await sr.zadd('a', 1.0, 'a1', 2.0, 'a2', a3=3.0)
    expected = [(b('a1'), 1.0), (b('a2'), 2.0), (b('a3'), 3.0)]
    assert await sr.zrange('a', 0, -1, withscores=True) == expected


@pytest.mark.asyncio
async def test_strict_lrem(sr):
    await sr.flushdb()
    await sr.rpush('a', 'a1', 'a2', 'a3', 'a1')
    await sr.lrem('a', 0, 'a1')
    assert await sr.lrange('a', 0, -1) == [b('a2'), b('a3')]


@pytest.mark.asyncio
async def test_strict_setex(sr):
    await sr.flushdb()
    assert await sr.setex('a', 60, '1')
    assert await sr.get('a') == b('1')
    assert 0 < await sr.ttl('a') <= 60


@pytest.mark.asyncio
async def test_strict_ttl(sr):
    await sr.flushdb()
    assert not await sr.expire('a', 10)
    await sr.set('a', '1')
    assert await sr.expire('a', 10)
    assert 0 < await sr.ttl('a') <= 10
    assert await sr.persist('a')
    assert await sr.ttl('a') == -1


@pytest.mark.asyncio
async def test_strict_pttl(sr):
    await sr.flushdb()
    assert not await sr.pexpire('a', 10000)
    await sr.set('a', '1')
    assert await sr.pexpire('a', 10000)
    assert 0 < await sr.pttl('a') <= 10000
    assert await sr.persist('a')
    assert await sr.pttl('a') == -1


@pytest.mark.asyncio
async def test_eval(sr):
    await sr.flushdb()
    res = await sr.eval('return {KEYS[1],KEYS[2],ARGV[1],ARGV[2]}', 2,
                        'A{foo}', 'B{foo}', 'first', 'second')
    assert res[0] == b('A{foo}')
    assert res[1] == b('B{foo}')
    assert res[2] == b('first')
    assert res[3] == b('second')
