# pylint: disable=protected-access
import asyncio
import time

import pytest

from yaaredis.cache import HerdCache


APP = 'test_cache'
KEY = 'test_key'
DATA = {str(i): i for i in range(3)}


def expensive_work(data):
    return data


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_set(r):
    await r.flushdb()

    cache = HerdCache(r, APP, default_herd_timeout=1, extend_herd_timeout=1)
    now = int(time.time())
    res = await cache.set(KEY, expensive_work(DATA), DATA)
    assert res

    identity = cache._gen_identity(KEY, DATA)
    content = await r.get(identity)
    content, expect_expire_time = cache._unpack(content)
    # supposed equal to 1, but may there be latency
    assert expect_expire_time - now <= 1
    assert content == DATA


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_get(r):
    await r.flushdb()

    cache = HerdCache(r, APP, default_herd_timeout=1, extend_herd_timeout=1)
    res = await cache.set(KEY, expensive_work(DATA), DATA)
    assert res

    content = await cache.get(KEY, DATA)
    assert content == DATA


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_set_many(r):
    await r.flushdb()

    cache = HerdCache(r, APP, default_herd_timeout=1, extend_herd_timeout=1)
    res = await cache.set_many(expensive_work(DATA), DATA)
    assert res

    for key, value in DATA.items():
        assert await cache.get(key, DATA) == value


@pytest.mark.asyncio(forbid_global_loop=True)
async def test_herd(r):
    await r.flushdb()
    now = int(time.time())
    cache = HerdCache(r, APP, default_herd_timeout=1, extend_herd_timeout=1)
    await cache.set(KEY, expensive_work(DATA), DATA)
    await asyncio.sleep(1)

    # first get
    identity = cache._gen_identity(KEY, DATA)
    content = await r.get(identity)
    content, expect_expire_time = cache._unpack(content)
    assert now + 1 == expect_expire_time

    # HerdCach.get
    await asyncio.sleep(0.1)
    res = await cache.get(KEY, DATA)
    # first herd get will reset expire time and return None
    assert res is None

    # second get
    identity = cache._gen_identity(KEY, DATA)
    content = await r.get(identity)
    content, new_expire_time = cache._unpack(content)
    assert new_expire_time >= expect_expire_time + 1
