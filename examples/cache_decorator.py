#!/usr/bin/python
import asyncio
import functools

import yaaredis


def cached(cache):
    def decorator(func):
        @functools.wraps(func)
        async def _inner(*args, **kwargs):
            key = func.__name__
            res = await cache.get(key, (args, kwargs))
            if res:
                print(f'using cache: {res}')
            else:
                print('cache miss')
                res = func(*args, **kwargs)
                await cache.set(key, res, (args, kwargs))
            return res
        return _inner
    return decorator


CACHE = yaaredis.StrictRedis().cache('example_cache')


@cached(cache=CACHE)
def job(*args, **kwargs):
    return f'example_results for job({args}, {kwargs})'


if __name__ == '__main__':
    loop = asyncio.get_event_loop()
    loop.run_until_complete(job(111))
