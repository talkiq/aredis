import datetime
import time

import pytest

from tests.cluster.conftest import skip_if_redis_py_version_lt
from tests.cluster.conftest import skip_if_server_version_lt
from yaaredis.exceptions import RedisClusterException
from yaaredis.exceptions import RedisError
from yaaredis.exceptions import ResponseError
from yaaredis.utils import b


pytestmark = skip_if_server_version_lt('2.9.0')


async def redis_server_time(client):
    t = await client.time()
    seconds, milliseconds = list(t.values())[0]
    timestamp = float(f'{seconds}.{milliseconds}')
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
    # the ttl command changes behavior in redis-2.8+
    # http://redis.io/commands/ttl
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
    # redis-2.8+ returns -1 if key exists but is non-expiring:
    # http://redis.io/commands/pttl
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
