import asyncio


class IterCommandMixin:
    """
    convenient function of scan iter, make it a class separately
    because yield can not be used in async function in Python3.6
    """
    RESPONSE_CALLBACKS = {}

    async def scan_iter(self, match=None, count=None,
                        type=None):  # pylint: disable=redefined-builtin
        """
        Make an iterator using the SCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``type`` filters results by a redis type
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = await self.scan(cursor=cursor, match=match,
                                           count=count, type=type)
            for item in data:
                yield item

    async def sscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the SSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = await self.sscan(name, cursor=cursor,
                                            match=match, count=count)
            for item in data:
                yield item

    async def hscan_iter(self, name, match=None, count=None):
        """
        Make an iterator using the HSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = await self.hscan(name, cursor=cursor,
                                            match=match, count=count)
            for item in data.items():
                yield item

    async def zscan_iter(self, name, match=None, count=None,
                         score_cast_func=float):
        """
        Make an iterator using the ZSCAN command so that the client doesn't
        need to remember the cursor position.

        ``match`` allows for filtering the keys by pattern

        ``count`` allows for hint the minimum number of returns

        ``score_cast_func`` a callable used to cast the score return value
        """
        cursor = '0'
        while cursor != 0:
            cursor, data = await self.zscan(name, cursor=cursor, match=match,
                                            count=count,
                                            score_cast_func=score_cast_func)
            for item in data:
                yield item


class ClusterIterCommandMixin(IterCommandMixin):
    async def scan_iter(self, match=None, count=None,
                        type=None):  # pylint: disable=redefined-builtin
        nodes = await self.cluster_nodes()

        async def iterate_node(node, queue):
            cursor = '0'
            while cursor != 0:
                pieces = [cursor]
                if match is not None:
                    pieces.extend(['MATCH', match])
                if count is not None:
                    pieces.extend(['COUNT', count])
                if type is not None:
                    pieces.extend(['TYPE', type])
                response = await self.execute_command_on_nodes(
                    [node], 'SCAN', *pieces)
                cursor, data = list(response.values())[0]
                for item in data:
                    await queue.put(item)  # blocks if queue is full

        # maxsize ensures we don't pull too much data into
        # memory if we are not processing it yet
        maxsize = 10 if count is None else count
        # reducing maxsize by one: the idea here is that the SCAN for an individual
        # node can never fill the queue in a single iteration, so we'll get at most
        # one SCAN iteration for each node if the queue is never consumed
        maxsize -= 1
        queue = asyncio.Queue(maxsize=maxsize)
        tasks = []
        for node in nodes:
            if 'master' in node['flags']:
                t = asyncio.ensure_future(iterate_node(node, queue))
                tasks.append(t)

        while not all(t.done() for t in tasks) or not queue.empty():
            try:
                yield queue.get_nowait()
            except asyncio.QueueEmpty:
                # N.B. if we `yield await queue.get()` above, we introduce a
                # race condition: the last iteration may have occurred in the
                # last task since our `while` liveness check *and that
                # iteration may not have appended any data to the queue*.
                # Instead, we must be careful to avoid `await`ing between
                # checking for alive tasks and fetching from the queue.
                await asyncio.sleep(0)
