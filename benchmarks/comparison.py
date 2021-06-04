#!/usr/bin/python
import asyncio
import time

import aioredis
import asyncio_redis
import redis

import yaaredis


HOST = '127.0.0.1'
NUM = 10000


async def test_yaaredis(n):
    start = time.time()
    client = yaaredis.StrictRedis(host=HOST)
    res = None
    for _ in range(n):
        res = await client.keys('*')
    print(time.time() - start)
    return res


async def test_asyncio_redis(n):
    connection = await asyncio_redis.Connection.create(host=HOST, port=6379)
    start = time.time()
    res = None
    for _ in range(n):
        res = await connection.keys('*')
    print(time.time() - start)
    connection.close()
    return res


def test_conn(n):
    start = time.time()
    client = redis.StrictRedis(host=HOST)
    res = None
    for _ in range(n):
        res = client.keys('*')
    print(time.time() - start)
    return res


async def test_aioredis(n, loop_):
    start = time.time()
    rc = await aioredis.create_redis((HOST, 6379), loop=loop_)
    val = None
    for _ in range(n):
        val = await rc.keys('*')
    print(time.time() - start)
    rc.close()
    await rc.wait_closed()
    return val


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    print('yaaredis')
    print(loop.run_until_complete(test_yaaredis(NUM)))
    print('asyncio_redis')
    print(loop.run_until_complete(test_asyncio_redis(NUM)))
    print('redis-py')
    print(test_conn(NUM))
    print('aioredis')
    print(loop.run_until_complete(test_aioredis(NUM, loop)))
