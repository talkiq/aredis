import pytest

from yaaredis.utils import b


@pytest.mark.asyncio
async def test_sadd(r):
    await r.flushdb()
    members = {b('1'), b('2'), b('3')}
    await r.sadd('a', *members)
    assert await r.smembers('a') == members


@pytest.mark.asyncio
async def test_scard(r):
    await r.flushdb()
    await r.sadd('a', '1', '2', '3')
    assert await r.scard('a') == 3


@pytest.mark.asyncio
async def test_sdiff(r):
    await r.flushdb()
    await r.sadd('a{foo}', '1', '2', '3')
    assert await r.sdiff('a{foo}', 'b{foo}') == {b('1'), b('2'), b('3')}
    await r.sadd('b{foo}', '2', '3')
    assert await r.sdiff('a{foo}', 'b{foo}') == {b('1')}


@pytest.mark.asyncio
async def test_sdiffstore(r):
    await r.flushdb()
    await r.sadd('a{foo}', '1', '2', '3')
    assert await r.sdiffstore('c{foo}', 'a{foo}', 'b{foo}') == 3
    assert await r.smembers('c{foo}') == {b('1'), b('2'), b('3')}
    await r.sadd('b{foo}', '2', '3')
    assert await r.sdiffstore('c{foo}', 'a{foo}', 'b{foo}') == 1
    assert await r.smembers('c{foo}') == {b('1')}

    # Diff:s that return empty set should not fail
    assert await r.sdiffstore('d{foo}', 'e{foo}') == 0


@pytest.mark.asyncio
async def test_sinter(r):
    await r.flushdb()
    await r.sadd('a{foo}', '1', '2', '3')
    assert await r.sinter('a{foo}', 'b{foo}') == set()
    await r.sadd('b{foo}', '2', '3')
    assert await r.sinter('a{foo}', 'b{foo}') == {b('2'), b('3')}


@pytest.mark.asyncio
async def test_sinterstore(r):
    await r.flushdb()
    await r.sadd('a{foo}', '1', '2', '3')
    assert await r.sinterstore('c{foo}', 'a{foo}', 'b{foo}') == 0
    assert await r.smembers('c{foo}') == set()
    await r.sadd('b{foo}', '2', '3')
    assert await r.sinterstore('c{foo}', 'a{foo}', 'b{foo}') == 2
    assert await r.smembers('c{foo}') == {b('2'), b('3')}


@pytest.mark.asyncio
async def test_sismember(r):
    await r.flushdb()
    await r.sadd('a', '1', '2', '3')
    assert await r.sismember('a', '1')
    assert await r.sismember('a', '2')
    assert await r.sismember('a', '3')
    assert not await r.sismember('a', '4')


@pytest.mark.asyncio
async def test_smembers(r):
    await r.flushdb()
    await r.sadd('a', '1', '2', '3')
    assert await r.smembers('a') == {b('1'), b('2'), b('3')}


@pytest.mark.asyncio
async def test_smove(r):
    await r.flushdb()
    await r.sadd('a{foo}', 'a1', 'a2')
    await r.sadd('b{foo}', 'b1', 'b2')
    assert await r.smove('a{foo}', 'b{foo}', 'a1')
    assert await r.smembers('a{foo}') == {b('a2')}
    assert await r.smembers('b{foo}') == {b('b1'), b('b2'), b('a1')}


@pytest.mark.asyncio
async def test_spop(r):
    await r.flushdb()
    s = [b('1'), b('2'), b('3')]
    await r.sadd('a', *s)
    value = await r.spop('a')
    assert value in s
    assert await r.smembers('a') == set(s) - {value}


@pytest.mark.asyncio
async def test_srandmember(r):
    await r.flushdb()
    s = [b('1'), b('2'), b('3')]
    await r.sadd('a', *s)
    assert await r.srandmember('a') in s


@pytest.mark.asyncio
async def test_srandmember_multi_value(r):
    await r.flushdb()
    s = [b('1'), b('2'), b('3')]
    await r.sadd('a', *s)
    randoms = await r.srandmember('a', number=2)
    assert len(randoms) == 2
    assert set(randoms).intersection(s) == set(randoms)


@pytest.mark.asyncio
async def test_srem(r):
    await r.flushdb()
    await r.sadd('a', '1', '2', '3', '4')
    assert await r.srem('a', '5') == 0
    assert await r.srem('a', '2', '4') == 2
    assert await r.smembers('a') == {b('1'), b('3')}


@pytest.mark.asyncio
async def test_sunion(r):
    await r.flushdb()
    await r.sadd('a{foo}', '1', '2')
    await r.sadd('b{foo}', '2', '3')
    assert await r.sunion('a{foo}', 'b{foo}') == {b('1'), b('2'), b('3')}


@pytest.mark.asyncio
async def test_sunionstore(r):
    await r.flushdb()
    await r.sadd('a{foo}', '1', '2')
    await r.sadd('b{foo}', '2', '3')
    assert await r.sunionstore('c{foo}', 'a{foo}', 'b{foo}') == 3
    assert await r.smembers('c{foo}') == {b('1'), b('2'), b('3')}
