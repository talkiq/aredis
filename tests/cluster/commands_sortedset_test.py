import re

import pytest

from yaaredis.exceptions import ResponseError
from yaaredis.utils import b


@pytest.mark.asyncio
async def test_zadd(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3)
    assert await r.zrange('a', 0, -1) == [b('a1'), b('a2'), b('a3')]


@pytest.mark.asyncio
async def test_zaddoption(r):
    await r.flushdb()
    await r.zadd('a', a1=1)
    assert int(await r.zscore('a', 'a1')) == 1
    assert int(await r.zaddoption('a', 'NX', a1=2)) == 0
    assert int(await r.zaddoption('a', 'NX CH', a1=2)) == 0
    assert int(await r.zscore('a', 'a1')) == 1
    assert await r.zcard('a') == 1
    assert int(await r.zaddoption('a', 'XX', a2=1)) == 0
    assert await r.zcard('a') == 1
    assert int(await r.zaddoption('a', 'XX', a1=2)) == 0
    assert int(await r.zaddoption('a', 'XX CH', a1=3)) == 1
    assert int(await r.zscore('a', 'a1')) == 3
    assert int(await r.zaddoption('a', 'NX', a2=1)) == 1
    assert int(await r.zaddoption('a', 'NX CH', a3=1)) == 1
    assert await r.zcard('a') == 3
    await r.zaddoption('a', 'INCR', a3=1)
    assert int(await r.zscore('a', 'a3')) == 2


@pytest.mark.asyncio
async def test_zcard(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3)
    assert await r.zcard('a') == 3


@pytest.mark.asyncio
async def test_zcount(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3)
    assert await r.zcount('a', '-inf', '+inf') == 3
    assert await r.zcount('a', 1, 2) == 2
    assert await r.zcount('a', 10, 20) == 0


@pytest.mark.asyncio
async def test_zincrby(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3)
    assert await r.zincrby('a', 'a2') == 3.0
    assert await r.zincrby('a', 'a3', amount=5) == 8.0
    assert await r.zscore('a', 'a2') == 3.0
    assert await r.zscore('a', 'a3') == 8.0


@pytest.mark.asyncio
async def test_zlexcount(r):
    await r.flushdb()
    await r.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
    assert await r.zlexcount('a', '-', '+') == 7
    assert await r.zlexcount('a', '[b', '[f') == 5


@pytest.mark.asyncio
async def test_zinterstore_fail_cross_slot(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=1, a3=1)
    await r.zadd('b', a1=2, a2=2, a3=2)
    await r.zadd('c', a1=6, a3=5, a4=4)
    with pytest.raises(ResponseError) as excinfo:
        await r.zinterstore('d', ['a', 'b', 'c'])
    assert re.search('ClusterCrossSlotError', str(excinfo))


@pytest.mark.asyncio
async def test_zinterstore_sum(r):
    await r.flushdb()
    await r.zadd('a{foo}', a1=1, a2=1, a3=1)
    await r.zadd('b{foo}', a1=2, a2=2, a3=2)
    await r.zadd('c{foo}', a1=6, a3=5, a4=4)
    assert await r.zinterstore('d{foo}', ['a{foo}', 'b{foo}', 'c{foo}']) == 2
    assert (await r.zrange('d{foo}', 0, -1, withscores=True) == [(b('a3'), 8), (b('a1'), 9)])


@pytest.mark.asyncio
async def test_zinterstore_max(r):
    await r.flushdb()
    await r.zadd('a{foo}', a1=1, a2=1, a3=1)
    await r.zadd('b{foo}', a1=2, a2=2, a3=2)
    await r.zadd('c{foo}', a1=6, a3=5, a4=4)
    assert await r.zinterstore('d{foo}', ['a{foo}', 'b{foo}', 'c{foo}'], aggregate='MAX') == 2
    assert (await r.zrange('d{foo}', 0, -1, withscores=True) == [(b('a3'), 5), (b('a1'), 6)])


@pytest.mark.asyncio
async def test_zinterstore_min(r):
    await r.flushdb()
    await r.zadd('a{foo}', a1=1, a2=2, a3=3)
    await r.zadd('b{foo}', a1=2, a2=3, a3=5)
    await r.zadd('c{foo}', a1=6, a3=5, a4=4)
    assert await r.zinterstore('d{foo}', ['a{foo}', 'b{foo}', 'c{foo}'], aggregate='MIN') == 2
    assert (await r.zrange('d{foo}', 0, -1, withscores=True) == [(b('a1'), 1), (b('a3'), 3)])


@pytest.mark.asyncio
async def test_zinterstore_with_weight(r):
    await r.flushdb()
    await r.zadd('a{foo}', a1=1, a2=1, a3=1)
    await r.zadd('b{foo}', a1=2, a2=2, a3=2)
    await r.zadd('c{foo}', a1=6, a3=5, a4=4)
    assert await r.zinterstore('d{foo}', {'a{foo}': 1, 'b{foo}': 2, 'c{foo}': 3}) == 2
    assert (await r.zrange('d{foo}', 0, -1, withscores=True) == [(b('a3'), 20), (b('a1'), 23)])


@pytest.mark.asyncio
async def test_zrange(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3)
    assert await r.zrange('a', 0, 1) == [b('a1'), b('a2')]
    assert await r.zrange('a', 1, 2) == [b('a2'), b('a3')]

    # withscores
    assert (await r.zrange('a', 0, 1, withscores=True) == [(b('a1'), 1.0), (b('a2'), 2.0)])
    assert (await r.zrange('a', 1, 2, withscores=True) == [(b('a2'), 2.0), (b('a3'), 3.0)])

    # custom score function
    assert (await r.zrange('a', 0, 1, withscores=True, score_cast_func=int) == [(b('a1'), 1), (b('a2'), 2)])


@pytest.mark.asyncio
async def test_zrangebylex(r):
    await r.flushdb()
    await r.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
    assert await r.zrangebylex('a', '-', '[c') == [b('a'), b('b'), b('c')]
    assert await r.zrangebylex('a', '-', '(c') == [b('a'), b('b')]
    assert (await r.zrangebylex('a', '[aaa', '(g') == [b('b'), b('c'), b('d'), b('e'), b('f')])
    assert await r.zrangebylex('a', '[f', '+') == [b('f'), b('g')]
    assert await r.zrangebylex('a', '-', '+', start=3, num=2) == [b('d'), b('e')]


@pytest.mark.asyncio
async def test_zrangebyscore(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
    assert await r.zrangebyscore('a', 2, 4) == [b('a2'), b('a3'), b('a4')]

    # slicing with start/num
    expected = [b('a3'), b('a4')]
    assert await r.zrangebyscore('a', 2, 4, start=1, num=2) == expected

    # withscores
    expected = [(b('a2'), 2.0), (b('a3'), 3.0), (b('a4'), 4.0)]
    assert await r.zrangebyscore('a', 2, 4, withscores=True) == expected

    # custom score function
    expected = [(b('a2'), 2), (b('a3'), 3), (b('a4'), 4)]
    assert await r.zrangebyscore('a', 2, 4, withscores=True,
                                 score_cast_func=int) == expected


@pytest.mark.asyncio
async def test_zrank(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
    assert await r.zrank('a', 'a1') == 0
    assert await r.zrank('a', 'a2') == 1
    assert await r.zrank('a', 'a6') is None


@pytest.mark.asyncio
async def test_zrem(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3)
    assert await r.zrem('a', 'a2') == 1
    assert await r.zrange('a', 0, -1) == [b('a1'), b('a3')]
    assert await r.zrem('a', 'b') == 0
    assert await r.zrange('a', 0, -1) == [b('a1'), b('a3')]


@pytest.mark.asyncio
async def test_zrem_multiple_keys(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3)
    assert await r.zrem('a', 'a1', 'a2') == 2
    assert await r.zrange('a', 0, 5) == [b('a3')]


@pytest.mark.asyncio
async def test_zremrangebylex(r):
    await r.flushdb()
    await r.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
    assert await r.zremrangebylex('a', '-', '[c') == 3
    assert await r.zrange('a', 0, -1) == [b('d'), b('e'), b('f'), b('g')]
    assert await r.zremrangebylex('a', '[f', '+') == 2
    assert await r.zrange('a', 0, -1) == [b('d'), b('e')]
    assert await r.zremrangebylex('a', '[h', '+') == 0
    assert await r.zrange('a', 0, -1) == [b('d'), b('e')]


@pytest.mark.asyncio
async def test_zremrangebyrank(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
    assert await r.zremrangebyrank('a', 1, 3) == 3
    assert await r.zrange('a', 0, 5) == [b('a1'), b('a5')]


@pytest.mark.asyncio
async def test_zremrangebyscore(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
    assert await r.zremrangebyscore('a', 2, 4) == 3
    assert await r.zrange('a', 0, -1) == [b('a1'), b('a5')]
    assert await r.zremrangebyscore('a', 2, 4) == 0
    assert await r.zrange('a', 0, -1) == [b('a1'), b('a5')]


@pytest.mark.asyncio
async def test_zrevrange(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3)
    assert await r.zrevrange('a', 0, 1) == [b('a3'), b('a2')]
    assert await r.zrevrange('a', 1, 2) == [b('a2'), b('a1')]

    # withscores
    expected = [(b('a3'), 3.0), (b('a2'), 2.0)]
    assert await r.zrevrange('a', 0, 1, withscores=True) == expected
    expected = [(b('a2'), 2.0), (b('a1'), 1.0)]
    assert await r.zrevrange('a', 1, 2, withscores=True) == expected

    # custom score function
    expected = [(b('a3'), 3.0), (b('a2'), 2.0)]
    assert await r.zrevrange('a', 0, 1, withscores=True,
                             score_cast_func=int) == expected


@pytest.mark.asyncio
async def test_zrevrangebyscore(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
    assert await r.zrevrangebyscore('a', 4, 2) == [b('a4'), b('a3'), b('a2')]

    # slicing with start/num
    expected = [b('a3'), b('a2')]
    assert await r.zrevrangebyscore('a', 4, 2, start=1, num=2) == expected

    # withscores
    expected = [(b('a4'), 4.0), (b('a3'), 3.0), (b('a2'), 2.0)]
    assert await r.zrevrangebyscore('a', 4, 2, withscores=True) == expected

    # custom score function
    expected = [(b('a4'), 4), (b('a3'), 3), (b('a2'), 2)]
    assert await r.zrevrangebyscore('a', 4, 2, withscores=True,
                                    score_cast_func=int) == expected


@pytest.mark.asyncio
async def test_zrevrank(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3, a4=4, a5=5)
    assert await r.zrevrank('a', 'a1') == 4
    assert await r.zrevrank('a', 'a2') == 3
    assert await r.zrevrank('a', 'a6') is None


@pytest.mark.asyncio
async def test_zscore(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=2, a3=3)
    assert await r.zscore('a', 'a1') == 1.0
    assert await r.zscore('a', 'a2') == 2.0
    assert await r.zscore('a', 'a4') is None


@pytest.mark.asyncio
async def test_zunionstore_fail_crossslot(r):
    await r.flushdb()
    await r.zadd('a', a1=1, a2=1, a3=1)
    await r.zadd('b', a1=2, a2=2, a3=2)
    await r.zadd('c', a1=6, a3=5, a4=4)
    with pytest.raises(ResponseError) as excinfo:
        await r.zunionstore('d', ['a', 'b', 'c'])
    assert re.search('ClusterCrossSlotError', str(excinfo))


@pytest.mark.asyncio
async def test_zunionstore_sum(r):
    await r.flushdb()
    await r.zadd('a{foo}', a1=1, a2=1, a3=1)
    await r.zadd('b{foo}', a1=2, a2=2, a3=2)
    await r.zadd('c{foo}', a1=6, a3=5, a4=4)
    assert await r.zunionstore('d{foo}', ['a{foo}', 'b{foo}', 'c{foo}']) == 4
    expected = [(b('a2'), 3), (b('a4'), 4), (b('a3'), 8), (b('a1'), 9)]
    assert await r.zrange('d{foo}', 0, -1, withscores=True) == expected


@pytest.mark.asyncio
async def test_zunionstore_max(r):
    await r.flushdb()
    await r.zadd('a{foo}', a1=1, a2=1, a3=1)
    await r.zadd('b{foo}', a1=2, a2=2, a3=2)
    await r.zadd('c{foo}', a1=6, a3=5, a4=4)
    assert await r.zunionstore('d{foo}', ['a{foo}', 'b{foo}', 'c{foo}'], aggregate='MAX') == 4
    expected = [(b('a2'), 2), (b('a4'), 4), (b('a3'), 5), (b('a1'), 6)]
    assert await r.zrange('d{foo}', 0, -1, withscores=True) == expected


@pytest.mark.asyncio
async def test_zunionstore_min(r):
    await r.flushdb()
    await r.zadd('a{foo}', a1=1, a2=2, a3=3)
    await r.zadd('b{foo}', a1=2, a2=2, a3=4)
    await r.zadd('c{foo}', a1=6, a3=5, a4=4)
    assert await r.zunionstore('d{foo}', ['a{foo}', 'b{foo}', 'c{foo}'], aggregate='MIN') == 4
    expected = [(b('a1'), 1), (b('a2'), 2), (b('a3'), 3), (b('a4'), 4)]
    assert await r.zrange('d{foo}', 0, -1, withscores=True) == expected


@pytest.mark.asyncio
async def test_zunionstore_with_weight(r):
    await r.flushdb()
    await r.zadd('a{foo}', a1=1, a2=1, a3=1)
    await r.zadd('b{foo}', a1=2, a2=2, a3=2)
    await r.zadd('c{foo}', a1=6, a3=5, a4=4)
    assert await r.zunionstore('d{foo}', {'a{foo}': 1, 'b{foo}': 2, 'c{foo}': 3}) == 4
    expected = [(b('a2'), 5), (b('a4'), 12), (b('a3'), 20), (b('a1'), 23)]
    assert await r.zrange('d{foo}', 0, -1, withscores=True) == expected
