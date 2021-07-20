from string import ascii_letters

import pytest

from yaaredis.commands.server import parse_info
from yaaredis.utils import b


@pytest.mark.asyncio
async def test_binary_get_set(r):
    await r.flushdb()
    assert await r.set(' foo bar ', '123')
    assert await r.get(' foo bar ') == b('123')

    assert await r.set(' foo\r\nbar\r\n ', '456')
    assert await r.get(' foo\r\nbar\r\n ') == b('456')

    assert await r.set(' \r\n\t\x07\x13 ', '789')
    assert await r.get(' \r\n\t\x07\x13 ') == b('789')

    expected = [b(' \r\n\t\x07\x13 '), b(' foo\r\nbar\r\n '), b(' foo bar ')]
    assert sorted(await r.keys('*')) == expected

    assert await r.delete(' foo bar ')
    assert await r.delete(' foo\r\nbar\r\n ')
    assert await r.delete(' \r\n\t\x07\x13 ')


@pytest.mark.asyncio
async def test_binary_lists(r):
    await r.flushdb()
    mapping = {
        b('foo bar'): [b('1'), b('2'), b('3')],
        b('foo\r\nbar\r\n'): [b('4'), b('5'), b('6')],
        b('foo\tbar\x07'): [b('7'), b('8'), b('9')],
    }
    # fill in lists
    for key, value in iter(mapping.items()):
        await r.rpush(key, *value)

    # check that KEYS returns all the keys as they are
    assert sorted(await r.keys('*')) == sorted(iter(mapping.keys()))

    # check that it is possible to get list content by key name
    for key, value in iter(mapping.items()):
        assert await r.lrange(key, 0, -1) == value


@pytest.mark.asyncio
async def test_22_info():
    """
    Older Redis versions contained 'allocation_stats' in INFO that
    was the cause of a number of bugs when parsing.
    """
    info = (b'allocation_stats:6=1,7=1,8=7141,9=180,10=92,11=116,12=5330,'
            b'13=123,14=3091,15=11048,16=225842,17=1784,18=814,19=12020,'
            b'20=2530,21=645,22=15113,23=8695,24=142860,25=318,26=3303,'
            b'27=20561,28=54042,29=37390,30=1884,31=18071,32=31367,33=160,'
            b'34=169,35=201,36=10155,37=1045,38=15078,39=22985,40=12523,'
            b'41=15588,42=265,43=1287,44=142,45=382,46=945,47=426,48=171,'
            b'49=56,50=516,51=43,52=41,53=46,54=54,55=75,56=647,57=332,'
            b'58=32,59=39,60=48,61=35,62=62,63=32,64=221,65=26,66=30,'
            b'67=36,68=41,69=44,70=26,71=144,72=169,73=24,74=37,75=25,'
            b'76=42,77=21,78=126,79=374,80=27,81=40,82=43,83=47,84=46,'
            b'85=114,86=34,87=37,88=7240,89=34,90=38,91=18,92=99,93=20,'
            b'94=18,95=17,96=15,97=22,98=18,99=69,100=17,101=22,102=15,'
            b'103=29,104=39,105=30,106=70,107=22,108=21,109=26,110=52,'
            b'111=45,112=33,113=67,114=41,115=44,116=48,117=53,118=54,'
            b'119=51,120=75,121=44,122=57,123=44,124=66,125=56,126=52,'
            b'127=81,128=108,129=70,130=50,131=51,132=53,133=45,134=62,'
            b'135=12,136=13,137=7,138=15,139=21,140=11,141=20,142=6,143=7,'
            b'144=11,145=6,146=16,147=19,148=1112,149=1,151=83,154=1,'
            b'155=1,156=1,157=1,160=1,161=1,162=2,166=1,169=1,170=1,171=2,'
            b'172=1,174=1,176=2,177=9,178=34,179=73,180=30,181=1,185=3,'
            b'187=1,188=1,189=1,192=1,196=1,198=1,200=1,201=1,204=1,205=1,'
            b'207=1,208=1,209=1,214=2,215=31,216=78,217=28,218=5,219=2,'
            b'220=1,222=1,225=1,227=1,234=1,242=1,250=1,252=1,253=1,'
            b'>=256=203')
    parsed = parse_info(info)
    assert 'allocation_stats' in parsed
    assert '6' in parsed['allocation_stats']
    assert '>=256' in parsed['allocation_stats']


@pytest.mark.asyncio
async def test_large_responses(r):
    """
    The PythonParser has some special cases for return values > 1MB
    """
    await r.flushdb()
    # load up 100K of data into a key
    data = ''.join([ascii_letters] * (100000 // len(ascii_letters)))
    await r.set('a', data)
    assert await r.get('a') == b(data)


@pytest.mark.asyncio
async def test_floating_point_encoding(r):
    """
    High precision floating point values sent to the server should keep
    precision.
    """
    await r.flushdb()
    timestamp = 1349673917.939762
    await r.zadd('a', timestamp, 'a1')
    assert await r.zscore('a', 'a1') == timestamp
