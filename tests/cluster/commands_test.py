import datetime
import re
import time

import pytest

from tests.cluster.conftest import skip_if_redis_py_version_lt
from tests.cluster.conftest import skip_if_server_version_lt
from yaaredis.exceptions import DataError
from yaaredis.exceptions import RedisClusterException
from yaaredis.exceptions import RedisError
from yaaredis.exceptions import ResponseError
from yaaredis.utils import b


pytestmark = skip_if_server_version_lt('2.9.0')


async def redis_server_time(client):
    t = await client.time()
    seconds, milliseconds = list(t.values())[0]
    timestamp = float('{}.{}'.format(seconds, milliseconds))
    return datetime.datetime.fromtimestamp(timestamp)


@pytest.mark.asyncio
@skip_if_server_version_lt('2.9.9')
async def test_zrevrangebylex(r):
    await r.flushdb()
    await r.zadd('a', a=0, b=0, c=0, d=0, e=0, f=0, g=0)
    assert await r.zrevrangebylex('a', '[c', '-') == [b('c'), b('b'), b('a')]
    assert await r.zrevrangebylex('a', '(c', '-') == [b('b'), b('a')]
    expected = [b('f'), b('e'), b('d'), b('c'), b('b')]
    assert await r.zrevrangebylex('a', '(g', '[aaa') == expected
    assert await r.zrevrangebylex('a', '+', '[f') == [b('g'), b('f')]
    expected = [b('d'), b('c')]
    assert await r.zrevrangebylex('a', '+', '-', start=3, num=2) == expected


@pytest.mark.asyncio
async def test_command_on_invalid_key_type(r):
    await r.flushdb()
    await r.lpush('a', '1')
    with pytest.raises(ResponseError):
        await r.get('a')


# SERVER INFORMATION
@pytest.mark.asyncio
async def test_client_list(r):
    client_lists = await r.client_list()
    for clients in client_lists.values():
        assert isinstance(clients[0], dict)
        assert 'addr' in clients[0]


@pytest.mark.asyncio
async def test_client_getname(r):
    client_names = await r.client_getname()
    for name in client_names.values():
        assert name is None


@pytest.mark.asyncio
async def test_client_setname(r):
    with pytest.raises(RedisClusterException):
        assert await r.client_setname('redis_py_test')


@pytest.mark.asyncio
async def test_config_get(r):
    config = await r.config_get()
    for data in config.values():
        assert 'maxmemory' in data
        assert data['maxmemory'].isdigit()


@pytest.mark.asyncio
async def test_config_resetstat(r):
    await r.ping()
    info = await r.info()
    for info in info.values():
        prior_commands_processed = int(info['total_commands_processed'])
        assert prior_commands_processed >= 1

    await r.config_resetstat()
    info = await r.info()
    for info in info.values():
        reset_commands_processed = int(info['total_commands_processed'])
        assert reset_commands_processed < prior_commands_processed


@pytest.mark.asyncio
async def test_config_set(r):
    assert await r.config_set('dbfilename', 'redis_py_test.rdb')
    config = await r.config_get()
    for config in config.values():
        assert config['dbfilename'] == 'redis_py_test.rdb'


@pytest.mark.asyncio
async def test_echo(r):
    echo = await r.echo('foo bar')
    for res in echo.values():
        assert res == b('foo bar')


@pytest.mark.asyncio
async def test_object(r):
    await r.flushdb()
    await r.set('a', 'foo')
    assert await r.object('refcount', 'a') == 1
    assert isinstance(await r.object('refcount', 'a'), int)
    # assert isinstance(await r.object('idletime', 'a'), int)
    # assert await r.object('encoding', 'a') in (b('raw'), b('embstr'))
    assert await r.object('idletime', 'invalid-key') is None


@pytest.mark.asyncio
async def test_ping(r):
    assert await r.ping()


@pytest.mark.asyncio
async def test_time(r):
    await r.flushdb()
    time_ = await r.time()
    for t in time_.values():
        assert len(t) == 2
        assert isinstance(t[0], int)
        assert isinstance(t[1], int)


# BASIC KEY COMMANDS
@pytest.mark.asyncio
async def test_append(r):
    await r.flushdb()
    assert await r.append('a', 'a1') == 2
    assert await r.get('a') == b('a1')
    assert await r.append('a', 'a2') == 4
    assert await r.get('a') == b('a1a2')


@pytest.mark.asyncio
async def test_bitcount(r):
    await r.flushdb()
    await r.setbit('a', 5, True)
    assert await r.bitcount('a') == 1
    await r.setbit('a', 6, True)
    assert await r.bitcount('a') == 2
    await r.setbit('a', 5, False)
    assert await r.bitcount('a') == 1
    await r.setbit('a', 9, True)
    await r.setbit('a', 17, True)
    await r.setbit('a', 25, True)
    await r.setbit('a', 33, True)
    assert await r.bitcount('a') == 5
    assert await r.bitcount('a', 0, -1) == 5
    assert await r.bitcount('a', 2, 3) == 2
    assert await r.bitcount('a', 2, -1) == 3
    assert await r.bitcount('a', -2, -1) == 2
    assert await r.bitcount('a', 1, 1) == 1


@pytest.mark.asyncio
async def test_bitop_not_supported(r):
    await r.flushdb()
    await r.set('a', '')
    with pytest.raises(RedisClusterException):
        await r.bitop('not', 'r', 'a')


@pytest.mark.asyncio
@skip_if_server_version_lt('2.8.7')
@skip_if_redis_py_version_lt('2.10.2')
async def test_bitpos(r):
    """
    Bitpos was added in redis-py in version 2.10.2

    # TODO: Added b() around keys but i think they should not have to be
            there for this command to work properly.
    """
    await r.flushdb()
    key = 'key:bitpos'
    await r.set(key, b('\xff\xf0\x00'))
    assert await r.bitpos(key, 0) == 12
    assert await r.bitpos(key, 0, 2, -1) == 16
    assert await r.bitpos(key, 0, -2, -1) == 12
    await r.set(key, b('\x00\xff\xf0'))
    assert await r.bitpos(key, 1, 0) == 8
    assert await r.bitpos(key, 1, 1) == 8
    await r.set(key, '\x00\x00\x00')
    assert await r.bitpos(key, 1) == -1


@pytest.mark.asyncio
@skip_if_server_version_lt('2.8.7')
@skip_if_redis_py_version_lt('2.10.2')
async def test_bitpos_wrong_arguments(r):
    """
    Bitpos was added in redis-py in version 2.10.2
    """
    key = 'key:bitpos:wrong:args'
    await r.flushdb()
    await r.set(key, b('\xff\xf0\x00'))
    with pytest.raises(RedisError):
        await r.bitpos(key, 0, end=1)
    with pytest.raises(RedisError):
        await r.bitpos(key, 7)


@pytest.mark.asyncio
async def test_decr(r):
    await r.flushdb()
    assert await r.decr('a') == -1
    assert await r.get('a') == b('-1')
    assert await r.decr('a') == -2
    assert await r.get('a') == b('-2')
    assert await r.decr('a', amount=5) == -7
    assert await r.get('a') == b('-7')


@pytest.mark.asyncio
async def test_delete(r):
    await r.flushdb()
    assert await r.delete('a') == 0
    await r.set('a', 'foo')
    assert await r.delete('a') == 1


@pytest.mark.asyncio
async def test_delete_with_multiple_keys(r):
    await r.flushdb()
    await r.set('a', 'foo')
    await r.set('b', 'bar')
    assert await r.delete('a', 'b') == 2
    assert await r.get('a') is None
    assert await r.get('b') is None


@pytest.mark.asyncio
async def test_delitem(r):
    await r.flushdb()
    await r.set('a', 'foo')
    await r.delete('a')
    assert await r.get('a') is None


@pytest.mark.asyncio
async def test_dump_and_restore(r):
    await r.flushdb()
    await r.set('a', 'foo')
    dumped = await r.dump('a')
    await r.delete('a')
    await r.restore('a', 0, dumped)
    assert await r.get('a') == b('foo')


@pytest.mark.asyncio
async def test_exists(r):
    await r.flushdb()
    assert not await r.exists('a')
    await r.set('a', 'foo')
    assert await r.exists('a')


@pytest.mark.asyncio
async def test_exists_contains(r):
    await r.flushdb()
    assert not await r.exists('a')
    await r.set('a', 'foo')
    assert await r.exists('a')


@pytest.mark.asyncio
async def test_expire(r):
    await r.flushdb()
    assert not await r.expire('a', 10)
    await r.set('a', 'foo')
    assert await r.expire('a', 10)
    assert 0 < await r.ttl('a') <= 10
    assert await r.persist('a')
    # the ttl command changes behavior in redis-2.8+ http://redis.io/commands/ttl
    assert await r.ttl('a') == -1


@pytest.mark.asyncio
async def test_expireat_datetime(r):
    await r.flushdb()
    expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
    await r.set('a', 'foo')
    assert await r.expireat('a', expire_at)
    assert 0 < await r.ttl('a') <= 61


@pytest.mark.asyncio
async def test_expireat_no_key(r):
    await r.flushdb()
    expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
    assert not await r.expireat('a', expire_at)


@pytest.mark.asyncio
async def test_expireat_unixtime(r):
    expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
    await r.set('a', 'foo')
    expire_at_seconds = int(time.mktime(expire_at.timetuple()))
    assert await r.expireat('a', expire_at_seconds)
    assert 0 < await r.ttl('a') <= 61


@pytest.mark.asyncio
async def test_get_and_set(r):
    # get and set can't be tested independently of each other
    await r.flushdb()
    assert await r.get('a') is None
    byte_string = b('value')
    integer = 5
    unicode_string = str(3456) + 'abcd' + str(3421)
    assert await r.set('byte_string', byte_string)
    assert await r.set('integer', 5)
    assert await r.set('unicode_string', unicode_string)
    assert await r.get('byte_string') == byte_string
    assert await r.get('integer') == b(str(integer))
    assert (await r.get('unicode_string')).decode('utf-8') == unicode_string


@pytest.mark.asyncio
async def test_getitem_and_setitem(r):
    await r.flushdb()
    await r.set('a', 'bar')
    assert await r.get('a') == b('bar')


@pytest.mark.asyncio
async def test_get_set_bit(r):
    await r.flushdb()
    # no value
    assert not await r.getbit('a', 5)
    # set bit 5
    assert not await r.setbit('a', 5, True)
    assert await r.getbit('a', 5)
    # unset bit 4
    assert not await r.setbit('a', 4, False)
    assert not await r.getbit('a', 4)
    # set bit 4
    assert not await r.setbit('a', 4, True)
    assert await r.getbit('a', 4)
    # set bit 5 again
    assert await r.setbit('a', 5, True)
    assert await r.getbit('a', 5)


@pytest.mark.asyncio
async def test_getrange(r):
    await r.flushdb()
    await r.set('a', 'foo')
    assert await r.getrange('a', 0, 0) == b('f')
    assert await r.getrange('a', 0, 2) == b('foo')
    assert await r.getrange('a', 3, 4) == b('')


@pytest.mark.asyncio
async def test_getset(r):
    await r.flushdb()
    assert await r.getset('a', 'foo') is None
    assert await r.getset('a', 'bar') == b('foo')
    assert await r.get('a') == b('bar')


@pytest.mark.asyncio
async def test_incr(r):
    await r.flushdb()
    assert await r.incr('a') == 1
    assert await r.get('a') == b('1')
    assert await r.incr('a') == 2
    assert await r.get('a') == b('2')
    assert await r.incr('a', amount=5) == 7
    assert await r.get('a') == b('7')


@pytest.mark.asyncio
async def test_incrby(r):
    await r.flushdb()
    assert await r.incrby('a') == 1
    assert await r.incrby('a', 4) == 5
    assert await r.get('a') == b('5')


@pytest.mark.asyncio
async def test_incrbyfloat(r):
    await r.flushdb()
    assert await r.incrbyfloat('a') == 1.0
    assert await r.get('a') == b('1')
    assert await r.incrbyfloat('a', 1.1) == 2.1
    assert float(await r.get('a')) == float(2.1)


@pytest.mark.asyncio
async def test_keys(r):
    await r.flushdb()
    keys = await r.keys()
    assert keys == []
    keys_with_underscores = {'test_a', 'test_b'}
    keys = keys_with_underscores.union({'testc'})
    for key in keys:
        await r.set(key, 1)
    assert set(await r.keys(pattern='test_*')) == {b(k) for k in keys_with_underscores}
    assert set(await r.keys(pattern='test*')) == {b(k) for k in keys}


@pytest.mark.asyncio
async def test_mget(r):
    await r.flushdb()
    assert await r.mget(['a', 'b']) == [None, None]
    await r.set('a', 1)
    await r.set('b', 2)
    await r.set('c', 3)
    assert await r.mget('a', 'other', 'b', 'c') == [b('1'), None, b('2'), b('3')]


@pytest.mark.asyncio
async def test_mget_hash_tags(r):
    await r.flushdb()
    assert await r.mget(['a{foo}', 'b{foo}']) == [None, None]
    await r.set('a{foo}', 1)
    await r.set('b{foo}', 2)
    await r.set('c{bar}', 3)
    assert await r.mget('a{foo}', 'other', 'b{foo}', 'c{bar}') == [b('1'), None, b('2'), b('3')]


@pytest.mark.asyncio
async def test_mset(r):
    await r.flushdb()
    d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
    assert await r.mset(d)
    for k, v in iter(d.items()):
        assert await r.mget(k) == [v]


@pytest.mark.asyncio
async def test_mset_hash_tags(r):
    await r.flushdb()
    d = {'a{foo}': b('1'), 'b{foo}': b('2'), 'c{bar}': b('3')}
    assert await r.mset(d)
    for k, v in iter(d.items()):
        assert await r.mget(k) == [v]


@pytest.mark.asyncio
async def test_mset_kwargs(r):
    await r.flushdb()
    d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
    assert await r.mset(**d)
    for k, v in iter(d.items()):
        assert await r.get(k) == v


@pytest.mark.asyncio
async def test_msetnx(r):
    await r.flushdb()
    d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
    assert await r.msetnx(d)
    d2 = {'a': b('x'), 'd': b('4')}
    assert not await r.msetnx(d2)
    for k, v in iter(d.items()):
        assert await r.get(k) == v
    assert await r.get('d') is None


@pytest.mark.asyncio
async def test_msetnx_kwargs(r):
    await r.flushdb()
    d = {'a': b('1'), 'b': b('2'), 'c': b('3')}
    assert await r.msetnx(**d)
    d2 = {'a': b('x'), 'd': b('4')}
    assert not await r.msetnx(**d2)
    for k, v in iter(d.items()):
        assert await r.get(k) == v
    assert await r.get('d') is None


@pytest.mark.asyncio
async def test_pexpire(r):
    await r.flushdb()
    assert not await r.pexpire('a', 60000)
    await r.set('a', 'foo')
    assert await r.pexpire('a', 60000)
    assert 0 < await r.pttl('a') <= 60000
    assert await r.persist('a')
    # redis-py tests seemed to be for older version of redis?
    # redis-2.8+ returns -1 if key exists but is non-expiring: http://redis.io/commands/pttl
    assert await r.pttl('a') == -1


@pytest.mark.asyncio
async def test_pexpireat_datetime(r):
    await r.flushdb()
    expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
    await r.set('a', 'foo')
    assert await r.pexpireat('a', expire_at)
    assert 0 < await r.pttl('a') <= 61000


@pytest.mark.asyncio
async def test_pexpireat_no_key(r):
    await r.flushdb()
    expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
    assert not await r.pexpireat('a', expire_at)


@pytest.mark.asyncio
async def test_pexpireat_unixtime(r):
    await r.flushdb()
    expire_at = await redis_server_time(r) + datetime.timedelta(minutes=1)
    await r.set('a', 'foo')
    expire_at_seconds = int(time.mktime(expire_at.timetuple())) * 1000
    assert await r.pexpireat('a', expire_at_seconds)
    assert 0 < await r.pttl('a') <= 61000


@pytest.mark.asyncio
async def test_psetex(r):
    await r.flushdb()
    assert await r.psetex('a', 1000, 'value')
    assert await r.get('a') == b('value')
    assert 0 < await r.pttl('a') <= 1000


@pytest.mark.asyncio
async def test_psetex_timedelta(r):
    await r.flushdb()
    expire_at = datetime.timedelta(milliseconds=1000)
    assert await r.psetex('a', expire_at, 'value')
    assert await r.get('a') == b('value')
    assert 0 < await r.pttl('a') <= 1000


@pytest.mark.asyncio
async def test_randomkey(r):
    await r.flushdb()
    assert await r.randomkey() is None
    for key in ('a', 'b', 'c'):
        await r.set(key, 1)
    assert await r.randomkey() in (b('a'), b('b'), b('c'))


@pytest.mark.asyncio
async def test_rename(r):
    await r.flushdb()
    await r.set('a', '1')
    assert await r.rename('a', 'b')
    assert await r.get('a') is None
    assert await r.get('b') == b('1')

    with pytest.raises(ResponseError) as ex:
        await r.rename('foo', 'foo')
    assert str(ex.value).startswith(
        'source and destination objects are the same')

    assert await r.get('foo') is None
    with pytest.raises(ResponseError) as ex:
        await r.rename('foo', 'bar')
    assert str(ex.value).startswith('no such key')


@pytest.mark.asyncio
async def test_renamenx(r):
    await r.flushdb()
    await r.set('a', '1')
    await r.set('b', '2')
    assert not await r.renamenx('a', 'b')
    assert await r.get('a') == b('1')
    assert await r.get('b') == b('2')

    assert await r.renamenx('a', 'c')
    assert await r.get('c') == b('1')


@pytest.mark.asyncio
async def test_set_nx(r):
    await r.flushdb()
    assert await r.set('a', '1', nx=True)
    assert not await r.set('a', '2', nx=True)
    assert await r.get('a') == b('1')


@pytest.mark.asyncio
async def test_set_xx(r):
    await r.flushdb()
    assert not await r.set('a', '1', xx=True)
    assert await r.get('a') is None
    await r.set('a', 'bar')
    assert await r.set('a', '2', xx=True)
    assert await r.get('a') == b('2')


@pytest.mark.asyncio
async def test_set_px(r):
    await r.flushdb()
    assert await r.set('a', '1', px=10000)
    assert await r.get('a') == b('1')
    assert 0 < await r.pttl('a') <= 10000
    assert 0 < await r.ttl('a') <= 10


@pytest.mark.asyncio
async def test_set_px_timedelta(r):
    await r.flushdb()
    expire_at = datetime.timedelta(milliseconds=1000)
    assert await r.set('a', '1', px=expire_at)
    assert 0 < await r.pttl('a') <= 1000
    assert 0 < await r.ttl('a') <= 1


@pytest.mark.asyncio
async def test_set_ex(r):
    await r.flushdb()
    assert await r.set('a', '1', ex=10)
    assert 0 < await r.ttl('a') <= 10


@pytest.mark.asyncio
async def test_set_ex_timedelta(r):
    await r.flushdb()
    expire_at = datetime.timedelta(seconds=60)
    assert await r.set('a', '1', ex=expire_at)
    assert 0 < await r.ttl('a') <= 60


@pytest.mark.asyncio
async def test_set_multipleoptions(r):
    await r.flushdb()
    await r.set('a', 'val')
    assert await r.set('a', '1', xx=True, px=10000)
    assert 0 < await r.ttl('a') <= 10


@pytest.mark.asyncio
@skip_if_server_version_lt('6.0.0')
async def test_set_keepttl(r):
    await r.flushdb()
    await r.set('a', 'val')
    assert await r.set('a', '1', xx=True, px=10000)
    assert 0 < await r.ttl('a') <= 10
    await r.set('a', '2', keepttl=True)
    assert await r.get('a') == b('2')
    assert 0 < await r.ttl('a') <= 10


@pytest.mark.asyncio
async def test_setex(r):
    await r.flushdb()
    assert await r.setex('a', 60, '1')
    assert await r.get('a') == b('1')
    assert 0 < await r.ttl('a') <= 60


@pytest.mark.asyncio
async def test_setnx(r):
    await r.flushdb()
    assert await r.setnx('a', '1')
    assert await r.get('a') == b('1')
    assert not await r.setnx('a', '2')
    assert await r.get('a') == b('1')


@pytest.mark.asyncio
async def test_setrange(r):
    await r.flushdb()
    assert await r.setrange('a', 5, 'foo') == 8
    assert await r.get('a') == b('\0\0\0\0\0foo')
    await r.set('a', 'abcasync defghijh')
    assert await r.setrange('a', 6, '12345') == 17
    assert await r.get('a') == b('abcasy12345fghijh')


@pytest.mark.asyncio
async def test_strlen(r):
    await r.flushdb()
    await r.set('a', 'foo')
    assert await r.strlen('a') == 3


@pytest.mark.asyncio
async def test_substr(r):
    await r.flushdb()
    await r.set('a', '0123456789')
    assert await r.substr('a', 0) == b('0123456789')
    assert await r.substr('a', 2) == b('23456789')
    assert await r.substr('a', 3, 5) == b('345')
    assert await r.substr('a', 3, -2) == b('345678')


@pytest.mark.asyncio
async def test_type(r):
    await r.flushdb()
    assert await r.type('a') == b('none')
    await r.set('a', '1')
    assert await r.type('a') == b('string')
    await r.delete('a')
    await r.lpush('a', '1')
    assert await r.type('a') == b('list')
    await r.delete('a')
    await r.sadd('a', '1')
    assert await r.type('a') == b('set')
    await r.delete('a')
    await r.zadd('a', **{'1': 1})
    assert await r.type('a') == b('zset')


# LIST COMMANDS
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


# SCAN COMMANDS
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


# SET COMMANDS
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


# SORTED SET COMMANDS
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


# HYPERLOGLOG TESTS
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


# HASH COMMANDS
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


# SORT
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
