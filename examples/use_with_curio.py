import curio

import yaaredis


async def aio_child():
    redis = yaaredis.StrictRedis(host='127.0.0.1', port=6379, db=0)
    await redis.flushdb()
    await redis.set('bar', 'foo')

    resp = await redis.get('bar')
    return resp


async def wrapper():
    async with curio.bridge.AsyncioLoop() as loop:
        return await loop.run_asyncio(aio_child)


if __name__ == '__main__':
    print(curio.run(wrapper))
