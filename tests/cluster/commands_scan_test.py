import pytest

from yaaredis.utils import b


@pytest.mark.asyncio
async def test_scan(r):
    await r.flushdb()
    await r.set('a', 1)
    await r.set('b', 2)
    await r.set('c', 3)
    keys = []
    for result in (await r.scan()).values():
        cursor, partial_keys = result
        assert cursor == 0
        keys += partial_keys

    assert set(keys) == {b('a'), b('b'), b('c')}

    keys = []
    for result in (await r.scan(match='a')).values():
        cursor, partial_keys = result
        assert cursor == 0
        keys += partial_keys
    assert set(keys) == {b('a')}


@pytest.mark.asyncio
async def test_sscan(r):
    await r.flushdb()
    await r.sadd('a', 1, 2, 3)
    cursor, members = await r.sscan('a')
    assert cursor == 0
    assert set(members) == {b('1'), b('2'), b('3')}
    _, members = await r.sscan('a', match=b('1'))
    assert set(members) == {b('1')}


@pytest.mark.asyncio
async def test_hscan(r):
    await r.flushdb()
    await r.hmset('a', {'a': 1, 'b': 2, 'c': 3})
    cursor, dic = await r.hscan('a')
    assert cursor == 0
    assert dic == {b('a'): b('1'), b('b'): b('2'), b('c'): b('3')}
    _, dic = await r.hscan('a', match='a')
    assert dic == {b('a'): b('1')}


@pytest.mark.asyncio
async def test_zscan(r):
    await r.flushdb()
    await r.zadd('a', 1, 'a', 2, 'b', 3, 'c')
    cursor, pairs = await r.zscan('a')
    assert cursor == 0
    assert set(pairs) == {(b('a'), 1), (b('b'), 2), (b('c'), 3)}
    _, pairs = await r.zscan('a', match='a')
    assert set(pairs) == {(b('a'), 1)}


# SCAN
@pytest.mark.asyncio
async def test_scan_iter(r):
    await r.flushdb()
    await r.set('a', 1)
    await r.set('b', 2)
    await r.set('c', 3)
    keys = set()
    async for key in r.scan_iter():
        keys.add(key)
    assert keys == {b('a'), b('b'), b('c')}
    async for key in r.scan_iter(match='a'):
        assert key == b('a')


@pytest.mark.asyncio
async def test_scan_iter_multi_page(r):
    await r.flushdb()
    await r.set('a', 1)
    await r.set('b', 2)
    await r.set('c', 3)
    await r.set('d', 4)
    await r.set('e', 5)
    keys = set()
    async for key in r.scan_iter(count=1):
        # three nodes, five items, we've gotta be using a cursor
        keys.add(key)
    assert keys == {b('a'), b('b'), b('c'), b('d'), b('e')}
