# pylint: disable=protected-access
import asyncio

import pytest

from yaaredis.cache import Cache


APP = 'test_cache'
KEY = 'test_key'
DATA = {str(i): i for i in range(3)}


def expensive_work(data):
    return data


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_set(r):
    await r.flushdb()

    cache = Cache(r, APP)
    res = await cache.set(KEY, expensive_work(DATA), DATA)
    assert res

    identity = cache._gen_identity(KEY, DATA)
    content = await r.get(identity)
    content = cache._unpack(content)
    assert content == DATA


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_set_timeout(r, event_loop):
    await r.flushdb()

    cache = Cache(r, APP)
    res = await cache.set(KEY, expensive_work(DATA), DATA, expire_time=1)
    assert res

    identity = cache._gen_identity(KEY, DATA)
    content = await r.get(identity)
    content = cache._unpack(content)
    assert content == DATA

    await asyncio.sleep(1)
    content = await r.get(identity)
    assert content is None


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_set_with_plain_key(r):
    await r.flushdb()

    cache = Cache(r, APP, identity_generator_class=None)
    res = await cache.set(KEY, expensive_work(DATA), DATA, expire_time=1)
    assert res

    identity = cache._gen_identity(KEY, DATA)
    assert identity == KEY

    content = await r.get(identity)
    content = cache._unpack(content)
    assert content == DATA


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_get(r):
    await r.flushdb()

    cache = Cache(r, APP)
    res = await cache.set(KEY, expensive_work(DATA), DATA, expire_time=1)
    assert res

    content = await cache.get(KEY, DATA)
    assert content == DATA


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_set_many(r):
    await r.flushdb()

    cache = Cache(r, APP)
    res = await cache.set_many(expensive_work(DATA), DATA)
    assert res

    for key, value in DATA.items():
        assert await cache.get(key, DATA) == value


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_delete(r):
    await r.flushdb()

    cache = Cache(r, APP)
    res = await cache.set(KEY, expensive_work(DATA), DATA, expire_time=1)
    assert res

    content = await cache.get(KEY, DATA)
    assert content == DATA

    res = await cache.delete(KEY, DATA)
    assert res

    content = await cache.get(KEY, DATA)
    assert content is None


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_delete_pattern(r):
    await r.flushdb()

    cache = Cache(r, APP)
    await cache.set_many(expensive_work(DATA), DATA)
    res = await cache.delete_pattern('test_*', 10)
    assert res == 3

    content = await cache.get(KEY, DATA)
    assert content is None


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_ttl(r, event_loop):
    await r.flushdb()

    cache = Cache(r, APP)
    await cache.set(KEY, expensive_work(DATA), DATA, expire_time=1)
    ttl = await cache.ttl(KEY, DATA)
    assert ttl > 0

    await asyncio.sleep(1.1)
    ttl = await cache.ttl(KEY, DATA)
    assert ttl < 0


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_exists(r, event_loop):
    await r.flushdb()

    cache = Cache(r, APP)
    await cache.set(KEY, expensive_work(DATA), DATA, expire_time=1)
    exists = await cache.exist(KEY, DATA)
    assert exists is True

    await asyncio.sleep(1.1)
    exists = await cache.exist(KEY, DATA)
    assert exists is False
