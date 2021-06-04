import pytest

from yaaredis.exceptions import NoScriptError
from yaaredis.exceptions import RedisClusterException
from yaaredis.exceptions import ResponseError
from yaaredis.utils import b


MULTIPLY_SCRIPT = """
local value = redis.call('GET', KEYS[1])
value = tonumber(value)
return value * ARGV[1]"""

MSGPACK_HELLO_SCRIPT = """
local message = cmsgpack.unpack(ARGV[1])
local name = message['name']
return "hello " .. name
"""
MSGPACK_HELLO_SCRIPT_BROKEN = """
local message = cmsgpack.unpack(ARGV[1])
local names = message['name']
return "hello " .. name
"""


@pytest.mark.asyncio()
async def test_eval(r):
    await r.set('a', 2)
    # 2 * 3 == 6
    assert await r.eval(MULTIPLY_SCRIPT, 1, 'a', 3) == 6


@pytest.mark.asyncio()
async def test_eval_same_slot(r):
    await r.set('A{foo}', 2)
    await r.set('B{foo}', 4)
    # 2 * 4 == 8

    script = """
    local value = redis.call('GET', KEYS[1])
    local value2 = redis.call('GET', KEYS[2])
    return value * value2
    """
    result = await r.eval(script, 2, 'A{foo}', 'B{foo}')
    assert result == 8


@pytest.mark.asyncio()
async def test_eval_crossslot(r):
    """
    This test assumes that {foo} and {bar} will not go to the same
    server when used. In 3 masters + 3 slaves config this should pass.
    """
    await r.set('A{foo}', 2)
    await r.set('B{bar}', 4)
    # 2 * 4 == 8

    script = """
    local value = redis.call('GET', KEYS[1])
    local value2 = redis.call('GET', KEYS[2])
    return value * value2
    """
    with pytest.raises(RedisClusterException):
        await r.eval(script, 2, 'A{foo}', 'B{bar}')


@pytest.mark.asyncio()
async def test_evalsha(r):
    await r.set('a', 2)
    sha = await r.script_load(MULTIPLY_SCRIPT)
    # 2 * 3 == 6
    assert await r.evalsha(sha, 1, 'a', 3) == 6


@pytest.mark.asyncio()
async def test_evalsha_script_not_loaded(r):
    await r.set('a', 2)
    sha = await r.script_load(MULTIPLY_SCRIPT)
    # remove the script from Redis's cache
    await r.script_flush()
    with pytest.raises(NoScriptError):
        await r.evalsha(sha, 1, 'a', 3)


@pytest.mark.asyncio()
async def test_script_loading(r):
    # get the sha, then clear the cache
    sha = await r.script_load(MULTIPLY_SCRIPT)
    await r.script_flush()
    assert await r.script_exists(sha) == [False]
    await r.script_load(MULTIPLY_SCRIPT)
    assert await r.script_exists(sha) == [True]


@pytest.mark.asyncio()
async def test_script_object(r):
    await r.set('a', 2)
    multiply = r.register_script(MULTIPLY_SCRIPT)
    assert multiply.sha == '29cdf3e36c89fa05d7e6d6b9734b342ab15c9ea7'
    # test evalsha fail -> script load + retry
    assert await multiply.execute(keys=['a'], args=[3]) == 6
    assert multiply.sha
    assert await r.script_exists(multiply.sha) == [True]
    # test first evalsha
    assert await multiply.execute(keys=['a'], args=[3]) == 6


@pytest.mark.asyncio(forbid_global_loop=True)
@pytest.mark.xfail(reason='Not Yet Implemented')
async def test_script_object_in_pipeline(r):
    multiply = await r.register_script(MULTIPLY_SCRIPT)
    assert not multiply.sha
    pipe = r.pipeline()
    await pipe.set('a', 2)
    await pipe.get('a')
    multiply(keys=['a'], args=[3], client=pipe)
    # even though the pipeline wasn't executed yet, we made sure the
    # script was loaded and got a valid sha
    assert multiply.sha
    assert await r.script_exists(multiply.sha) == [True]
    # [SET worked, GET 'a', result of multiple script]
    assert await pipe.execute() == [True, b('2'), 6]

    # purge the script from redis's cache and re-run the pipeline
    # the multiply script object knows it's sha, so it shouldn't get
    # reloaded until pipe.execute()
    await r.script_flush()
    pipe = await r.pipeline()
    await pipe.set('a', 2)
    await pipe.get('a')
    assert multiply.sha
    multiply(keys=['a'], args=[3], client=pipe)
    assert await r.script_exists(multiply.sha) == [False]
    # [SET worked, GET 'a', result of multiple script]
    assert await pipe.execute() == [True, b('2'), 6]


@pytest.mark.asyncio(forbid_global_loop=True)
@pytest.mark.xfail(reason='Not Yet Implemented')
async def test_eval_msgpack_pipeline_error_in_lua(r):
    msgpack_hello = await r.register_script(MSGPACK_HELLO_SCRIPT)
    assert not msgpack_hello.sha

    pipe = r.pipeline()

    # avoiding a dependency to msgpack, this is the output of
    # msgpack.dumps({"name": "joe"})
    msgpack_message_1 = b'\x81\xa4name\xa3Joe'

    msgpack_hello(args=[msgpack_message_1], client=pipe)

    assert await r.script_exists(msgpack_hello.sha) == [True]
    assert await pipe.execute()[0] == b'hello Joe'

    msgpack_hello_broken = await r.register_script(MSGPACK_HELLO_SCRIPT_BROKEN)

    msgpack_hello_broken(args=[msgpack_message_1], client=pipe)
    with pytest.raises(ResponseError) as excinfo:
        await pipe.execute()
    assert excinfo.type == ResponseError
