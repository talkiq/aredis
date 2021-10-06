import pytest

from yaaredis.exceptions import DataError
from yaaredis.utils import b


@pytest.mark.asyncio
async def test_sort_basic(r):
    await r.flushdb()
    await r.rpush('a', '3', '2', '1', '4')
    assert await r.sort('a') == [b('1'), b('2'), b('3'), b('4')]


@pytest.mark.asyncio
async def test_sort_limited(r):
    await r.flushdb()
    await r.rpush('a', '3', '2', '1', '4')
    assert await r.sort('a', start=1, num=2) == [b('2'), b('3')]


@pytest.mark.asyncio
async def test_sort_by(r):
    await r.flushdb()
    await r.set('score:1', 8)
    await r.set('score:2', 3)
    await r.set('score:3', 5)
    await r.rpush('a', '3', '2', '1')
    assert await r.sort('a', by='score:*') == [b('2'), b('3'), b('1')]


@pytest.mark.asyncio
async def test_sort_get(r):
    await r.flushdb()
    await r.set('user:1', 'u1')
    await r.set('user:2', 'u2')
    await r.set('user:3', 'u3')
    await r.rpush('a', '2', '3', '1')
    assert await r.sort('a', get='user:*') == [b('u1'), b('u2'), b('u3')]


@pytest.mark.asyncio
async def test_sort_get_multi(r):
    await r.flushdb()
    await r.set('user:1', 'u1')
    await r.set('user:2', 'u2')
    await r.set('user:3', 'u3')
    await r.rpush('a', '2', '3', '1')
    expected = [b('u1'), b('1'), b('u2'), b('2'), b('u3'), b('3')]
    assert await r.sort('a', get=('user:*', '#')) == expected


@pytest.mark.asyncio
async def test_sort_get_groups_two(r):
    await r.flushdb()
    await r.set('user:1', 'u1')
    await r.set('user:2', 'u2')
    await r.set('user:3', 'u3')
    await r.rpush('a', '2', '3', '1')
    expected = [(b('u1'), b('1')), (b('u2'), b('2')), (b('u3'), b('3'))]
    assert await r.sort('a', get=('user:*', '#'), groups=True) == expected


@pytest.mark.asyncio
async def test_sort_groups_string_get(r):
    await r.flushdb()
    await r.set('user:1', 'u1')
    await r.set('user:2', 'u2')
    await r.set('user:3', 'u3')
    await r.rpush('a', '2', '3', '1')
    with pytest.raises(DataError):
        await r.sort('a', get='user:*', groups=True)


@pytest.mark.asyncio
async def test_sort_groups_just_one_get(r):
    await r.flushdb()
    await r.set('user:1', 'u1')
    await r.set('user:2', 'u2')
    await r.set('user:3', 'u3')
    await r.rpush('a', '2', '3', '1')
    with pytest.raises(DataError):
        await r.sort('a', get=['user:*'], groups=True)


@pytest.mark.asyncio
async def test_sort_groups_no_get(r):
    await r.flushdb()
    await r.set('user:1', 'u1')
    await r.set('user:2', 'u2')
    await r.set('user:3', 'u3')
    await r.rpush('a', '2', '3', '1')
    with pytest.raises(DataError):
        await r.sort('a', groups=True)


@pytest.mark.asyncio
async def test_sort_groups_three_gets(r):
    await r.flushdb()
    await r.set('user:1', 'u1')
    await r.set('user:2', 'u2')
    await r.set('user:3', 'u3')
    await r.set('door:1', 'd1')
    await r.set('door:2', 'd2')
    await r.set('door:3', 'd3')
    await r.rpush('a', '2', '3', '1')
    assert await r.sort('a', get=('user:*', 'door:*', '#'), groups=True) == [
        (b('u1'), b('d1'), b('1')),
        (b('u2'), b('d2'), b('2')),
        (b('u3'), b('d3'), b('3')),
    ]


@pytest.mark.asyncio
async def test_sort_desc(r):
    await r.flushdb()
    await r.rpush('a', '2', '3', '1')
    assert await r.sort('a', desc=True) == [b('3'), b('2'), b('1')]


@pytest.mark.asyncio
async def test_sort_alpha(r):
    await r.flushdb()
    await r.rpush('a', 'e', 'c', 'b', 'd', 'a')
    assert (await r.sort('a', alpha=True) == [b('a'), b('b'), b('c'), b('d'), b('e')])


@pytest.mark.asyncio
async def test_sort_store(r):
    await r.flushdb()
    await r.rpush('a', '2', '3', '1')
    assert await r.sort('a', store='sorted_values') == 3
    assert await r.lrange('sorted_values', 0, -1) == [b('1'), b('2'), b('3')]


@pytest.mark.asyncio
async def test_sort_all_options(r):
    await r.flushdb()
    await r.set('user:1:username', 'zeus')
    await r.set('user:2:username', 'titan')
    await r.set('user:3:username', 'hermes')
    await r.set('user:4:username', 'hercules')
    await r.set('user:5:username', 'apollo')
    await r.set('user:6:username', 'athena')
    await r.set('user:7:username', 'hades')
    await r.set('user:8:username', 'dionysus')

    await r.set('user:1:favorite_drink', 'yuengling')
    await r.set('user:2:favorite_drink', 'rum')
    await r.set('user:3:favorite_drink', 'vodka')
    await r.set('user:4:favorite_drink', 'milk')
    await r.set('user:5:favorite_drink', 'pinot noir')
    await r.set('user:6:favorite_drink', 'water')
    await r.set('user:7:favorite_drink', 'gin')
    await r.set('user:8:favorite_drink', 'apple juice')

    await r.rpush('gods', '5', '8', '3', '1', '2', '7', '6', '4')
    num = await r.sort('gods', start=2, num=4, by='user:*:username',
                       get='user:*:favorite_drink', desc=True, alpha=True,
                       store='sorted')
    assert num == 4
    expected = [b('vodka'), b('milk'), b('gin'), b('apple juice')]
    assert await r.lrange('sorted', 0, 10) == expected
