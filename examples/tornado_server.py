import asyncio

from tornado.httpserver import HTTPServer
from tornado.platform.asyncio import AsyncIOMainLoop
from tornado.web import Application
from tornado.web import RequestHandler

from yaaredis import StrictRedis


class GetRedisKeyHandler(RequestHandler):

    def __init__(self, application, request, **kwargs):
        super().__init__(
            application, request, **kwargs)
        self.redis_client = StrictRedis()

    async def get(self):
        key = self.get_argument('key')
        res = await self.redis_client.get(key)
        print(f'key: {key} val: {res} in redis')
        self.write(res)


if __name__ == '__main__':
    AsyncIOMainLoop().install()
    app = Application([('/', GetRedisKeyHandler)])
    server = HTTPServer(app)
    server.bind(8000)
    server.start()
    asyncio.get_event_loop().run_forever()
