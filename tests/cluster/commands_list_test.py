import pytest

from yaaredis.utils import b


@pytest.mark.asyncio
async def test_blpop(r):
    await r.flushdb()
    await r.rpush('a{foo}', '1', '2')
    await r.rpush('b{foo}', '3', '4')
    assert await r.blpop(['b{foo}', 'a{foo}'], timeout=1) == (b('b{foo}'), b('3'))
    assert await r.blpop(['b{foo}', 'a{foo}'], timeout=1) == (b('b{foo}'), b('4'))
    assert await r.blpop(['b{foo}', 'a{foo}'], timeout=1) == (b('a{foo}'), b('1'))
    assert await r.blpop(['b{foo}', 'a{foo}'], timeout=1) == (b('a{foo}'), b('2'))
    assert await r.blpop(['b{foo}', 'a{foo}'], timeout=1) is None
    await r.rpush('c{foo}', '1')
    assert await r.blpop('c{foo}', timeout=1) == (b('c{foo}'), b('1'))


@pytest.mark.asyncio
async def test_brpop(r):
    await r.flushdb()
    await r.rpush('a{foo}', '1', '2')
    await r.rpush('b{foo}', '3', '4')
    assert await r.brpop(['b{foo}', 'a{foo}'], timeout=1) == (b('b{foo}'), b('4'))
    assert await r.brpop(['b{foo}', 'a{foo}'], timeout=1) == (b('b{foo}'), b('3'))
    assert await r.brpop(['b{foo}', 'a{foo}'], timeout=1) == (b('a{foo}'), b('2'))
    assert await r.brpop(['b{foo}', 'a{foo}'], timeout=1) == (b('a{foo}'), b('1'))
    assert await r.brpop(['b{foo}', 'a{foo}'], timeout=1) is None
    await r.rpush('c{foo}', '1')
    assert await r.brpop('c{foo}', timeout=1) == (b('c{foo}'), b('1'))


@pytest.mark.asyncio
async def test_brpoplpush(r):
    await r.flushdb()
    await r.rpush('a{foo}', '1', '2')
    await r.rpush('b{foo}', '3', '4')
    assert await r.brpoplpush('a{foo}', 'b{foo}') == b('2')
    assert await r.brpoplpush('a{foo}', 'b{foo}') == b('1')
    assert await r.brpoplpush('a{foo}', 'b{foo}', timeout=1) is None
    assert await r.lrange('a{foo}', 0, -1) == []
    assert await r.lrange('b{foo}', 0, -1) == [b('1'), b('2'), b('3'), b('4')]


@pytest.mark.asyncio
async def test_brpoplpush_empty_string(r):
    await r.flushdb()
    await r.rpush('a', '')
    assert await r.brpoplpush('a', 'b') == b('')


@pytest.mark.asyncio
async def test_lindex(r):
    await r.flushdb()
    await r.rpush('a', '1', '2', '3')
    assert await r.lindex('a', '0') == b('1')
    assert await r.lindex('a', '1') == b('2')
    assert await r.lindex('a', '2') == b('3')


@pytest.mark.asyncio
async def test_linsert(r):
    await r.flushdb()
    await r.rpush('a', '1', '2', '3')
    assert await r.linsert('a', 'after', '2', '2.5') == 4
    assert await r.lrange('a', 0, -1) == [b('1'), b('2'), b('2.5'), b('3')]
    assert await r.linsert('a', 'before', '2', '1.5') == 5
    assert (await r.lrange('a', 0, -1) == [b('1'), b('1.5'), b('2'), b('2.5'), b('3')])


@pytest.mark.asyncio
async def test_llen(r):
    await r.flushdb()
    await r.rpush('a', '1', '2', '3')
    assert await r.llen('a') == 3


@pytest.mark.asyncio
async def test_lpop(r):
    await r.flushdb()
    await r.rpush('a', '1', '2', '3')
    assert await r.lpop('a') == b('1')
    assert await r.lpop('a') == b('2')
    assert await r.lpop('a') == b('3')
    assert await r.lpop('a') is None


@pytest.mark.asyncio
async def test_lpush(r):
    await r.flushdb()
    assert await r.lpush('a', '1') == 1
    assert await r.lpush('a', '2') == 2
    assert await r.lpush('a', '3', '4') == 4
    assert await r.lrange('a', 0, -1) == [b('4'), b('3'), b('2'), b('1')]


@pytest.mark.asyncio
async def test_lpushx(r):
    await r.flushdb()
    assert await r.lpushx('a', '1') == 0
    assert await r.lrange('a', 0, -1) == []
    await r.rpush('a', '1', '2', '3')
    assert await r.lpushx('a', '4') == 4
    assert await r.lrange('a', 0, -1) == [b('4'), b('1'), b('2'), b('3')]


@pytest.mark.asyncio
async def test_lrange(r):
    await r.flushdb()
    await r.rpush('a', '1', '2', '3', '4', '5')
    assert await r.lrange('a', 0, 2) == [b('1'), b('2'), b('3')]
    assert await r.lrange('a', 2, 10) == [b('3'), b('4'), b('5')]
    assert await r.lrange('a', 0, -1) == [b('1'), b('2'), b('3'), b('4'), b('5')]


@pytest.mark.asyncio
async def test_lrem(r):
    await r.flushdb()
    await r.rpush('a', '1', '1', '1', '1')
    assert await r.lrem('a', '1', 1) == 1
    assert await r.lrange('a', 0, -1) == [b('1'), b('1'), b('1')]
    assert await r.lrem('a', 0, '1') == 3
    assert await r.lrange('a', 0, -1) == []


@pytest.mark.asyncio
async def test_lset(r):
    await r.flushdb()
    await r.rpush('a', '1', '2', '3')
    assert await r.lrange('a', 0, -1) == [b('1'), b('2'), b('3')]
    assert await r.lset('a', 1, '4')
    assert await r.lrange('a', 0, 2) == [b('1'), b('4'), b('3')]


@pytest.mark.asyncio
async def test_ltrim(r):
    await r.flushdb()
    await r.rpush('a', '1', '2', '3')
    assert await r.ltrim('a', 0, 1)
    assert await r.lrange('a', 0, -1) == [b('1'), b('2')]


@pytest.mark.asyncio
async def test_rpop(r):
    await r.flushdb()
    await r.rpush('a', '1', '2', '3')
    assert await r.rpop('a') == b('3')
    assert await r.rpop('a') == b('2')
    assert await r.rpop('a') == b('1')
    assert await r.rpop('a') is None


@pytest.mark.asyncio
async def test_rpoplpush(r):
    await r.flushdb()
    await r.rpush('a', 'a1', 'a2', 'a3')
    await r.rpush('b', 'b1', 'b2', 'b3')
    assert await r.rpoplpush('a', 'b') == b('a3')
    assert await r.lrange('a', 0, -1) == [b('a1'), b('a2')]
    assert await r.lrange('b', 0, -1) == [b('a3'), b('b1'), b('b2'), b('b3')]


@pytest.mark.asyncio
async def test_rpush(r):
    await r.flushdb()
    assert await r.rpush('a', '1') == 1
    assert await r.rpush('a', '2') == 2
    assert await r.rpush('a', '3', '4') == 4
    assert await r.lrange('a', 0, -1) == [b('1'), b('2'), b('3'), b('4')]


@pytest.mark.asyncio
async def test_rpushx(r):
    await r.flushdb()
    assert await r.rpushx('a', 'b') == 0
    assert await r.lrange('a', 0, -1) == []
    await r.rpush('a', '1', '2', '3')
    assert await r.rpushx('a', '4') == 4
    assert await r.lrange('a', 0, -1) == [b('1'), b('2'), b('3'), b('4')]
