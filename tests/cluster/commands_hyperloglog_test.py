import pytest

from tests.cluster.conftest import skip_if_server_version_lt
from yaaredis.utils import b


@pytest.mark.asyncio
async def test_pfadd(r):
    await r.flushdb()
    members = {b('1'), b('2'), b('3')}
    assert await r.pfadd('a', *members) == 1
    assert await r.pfadd('a', *members) == 0
    assert await r.pfcount('a') == len(members)


@pytest.mark.asyncio
@pytest.mark.xfail(reason='New pfcount in 2.10.5 currently breaks in cluster')
@skip_if_server_version_lt('2.8.9')
async def test_pfcount(r):
    await r.flushdb()
    members = {b('1'), b('2'), b('3')}
    await r.pfadd('a', *members)
    assert await r.pfcount('a') == len(members)
    members_b = {b('2'), b('3'), b('4')}
    await r.pfadd('b', *members_b)
    assert await r.pfcount('b') == len(members_b)
    assert await r.pfcount('a', 'b') == len(members_b.union(members))


@pytest.mark.asyncio
async def test_pfmerge(r):
    await r.flushdb()
    mema = {b('1'), b('2'), b('3')}
    memb = {b('2'), b('3'), b('4')}
    memc = {b('5'), b('6'), b('7')}
    await r.pfadd('a', *mema)
    await r.pfadd('b', *memb)
    await r.pfadd('c', *memc)
    await r.pfmerge('d', 'c', 'a')
    assert await r.pfcount('d') == 6
    await r.pfmerge('d', 'b')
    assert await r.pfcount('d') == 7
