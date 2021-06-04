# pylint: disable=redefined-outer-name
import pickle

import pytest

import yaaredis


@pytest.fixture(scope='function')
def r():
    return yaaredis.StrictRedis(decode_responses=True)


@pytest.mark.asyncio()
async def test_simple_encoding(r):
    await r.flushdb()
    unicode_string = chr(124) + 'abcd' + chr(125)
    await r.set('unicode-string', unicode_string)
    cached_val = await r.get('unicode-string')
    assert isinstance(cached_val, str)
    assert unicode_string == cached_val


@pytest.mark.asyncio()
async def test_list_encoding(r):
    unicode_string = chr(124) + 'abcd' + chr(125)
    result = [unicode_string, unicode_string, unicode_string]
    await r.rpush('a', *result)
    assert await r.lrange('a', 0, -1) == result


@pytest.mark.asyncio()
async def test_object_value(r):
    unicode_string = chr(124) + 'abcd' + chr(125)
    await r.set('unicode-string', Exception(unicode_string))
    cached_val = await r.get('unicode-string')
    assert isinstance(cached_val, str)
    assert unicode_string == cached_val


@pytest.mark.asyncio()
async def test_pickled_object():
    r = yaaredis.StrictRedis()
    obj = Exception('args')
    pickled_obj = pickle.dumps(obj)
    await r.set('pickled-obj', pickled_obj)
    cached_obj = await r.get('pickled-obj')
    assert isinstance(cached_obj, bytes)
    assert obj.args == pickle.loads(cached_obj).args
