import asyncio
import time

import pytest

from yaaredis import StrictRedis
from yaaredis import StrictRedisCluster
from yaaredis.exceptions import ConnectionError  # pylint: disable=redefined-builtin


async def wait_for_message(pubsub, timeout=0.5,
                           ignore_subscribe_messages=False):
    now = time.time()
    timeout = now + timeout
    while now < timeout:
        message = await pubsub.get_message(
            ignore_subscribe_messages=ignore_subscribe_messages,
            timeout=0.01,
        )
        if message is not None:
            return message
        await asyncio.sleep(0.01)
        now = time.time()
    return None


def make_message(kind, channel, data, pattern=None):
    return {
        'type': kind,
        'pattern': pattern and pattern.encode('utf-8') or None,
        'channel': channel.encode('utf-8'),
        'data': data.encode('utf-8') if isinstance(data, str) else data,
    }


def make_subscribe_test_data(pubsub, kind):
    if kind == 'channel':
        return {
            'p': pubsub,
            'sub_type': 'subscribe',
            'unsub_type': 'unsubscribe',
            'sub_func': pubsub.subscribe,
            'unsub_func': pubsub.unsubscribe,
            'keys': ['foo', 'bar', 'uni' + chr(4456) + 'code'],
        }
    if kind == 'pattern':
        return {
            'p': pubsub,
            'sub_type': 'psubscribe',
            'unsub_type': 'punsubscribe',
            'sub_func': pubsub.psubscribe,
            'unsub_func': pubsub.punsubscribe,
            'keys': ['f*', 'b*', 'uni' + chr(4456) + '*'],
        }
    assert False, f'invalid subscribe type: {type}'


class TestPubSubSubscribeUnsubscribe:

    async def _test_subscribe_unsubscribe(self, p, sub_type, unsub_type,
                                          sub_func, unsub_func, keys):
        for key in keys:
            assert await sub_func(key) is None

        # should be a message for each channel/pattern we just subscribed to
        for i, key in enumerate(keys):
            assert await wait_for_message(p) == make_message(sub_type, key,
                                                             i + 1)

        for key in keys:
            assert await unsub_func(key) is None

        # should be a message for each channel/pattern we just unsubscribed
        # from
        for i, key in enumerate(keys):
            i = len(keys) - 1 - i
            assert await wait_for_message(p) == make_message(unsub_type, key,
                                                             i)

    @pytest.mark.asyncio
    async def test_channel_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'channel')
        await self._test_subscribe_unsubscribe(**kwargs)

    @pytest.mark.asyncio
    async def test_pattern_subscribe_unsubscribe(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'pattern')
        await self._test_subscribe_unsubscribe(**kwargs)

    async def _test_resubscribe_on_reconnection(self, p, sub_type, unsub_type,
                                                sub_func, unsub_func, keys):
        # pylint: disable=unused-argument
        for key in keys:
            assert await sub_func(key) is None

        # should be a message for each channel/pattern we just subscribed to
        for i, key in enumerate(keys):
            assert await wait_for_message(p) == make_message(sub_type, key, i + 1)

        # manually disconnect
        p.connection.disconnect()

        # calling get_message again reconnects and resubscribes
        # note, we may not re-subscribe to channels in exactly the same order
        # so we have to do some extra checks to make sure we got them all
        messages = []
        for i, _ in enumerate(keys):
            messages.append(await wait_for_message(p))

        unique_channels = set()
        assert len(messages) == len(keys)
        for i, message in enumerate(messages):
            assert message['type'] == sub_type
            assert message['data'] == i + 1
            assert isinstance(message['channel'], bytes)
            channel = message['channel'].decode('utf-8')
            unique_channels.add(channel)

        assert len(unique_channels) == len(keys)
        for channel in unique_channels:
            assert channel in keys

    @pytest.mark.asyncio
    async def test_resubscribe_to_channels_on_reconnection(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'channel')
        await self._test_resubscribe_on_reconnection(**kwargs)

    @pytest.mark.asyncio
    async def test_resubscribe_to_patterns_on_reconnection(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'pattern')
        await self._test_resubscribe_on_reconnection(**kwargs)

    async def _test_subscribed_property(self, p, sub_type, unsub_type, sub_func, unsub_func, keys):
        assert p.subscribed is False
        await sub_func(keys[0])
        # we're now subscribed even though we haven't processed the
        # reply from the server just yet
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(sub_type, keys[0], 1)
        # we're still subscribed
        assert p.subscribed is True

        # unsubscribe from all channels
        await unsub_func()
        # we're still technically subscribed until we process the
        # response messages from the server
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(unsub_type, keys[0], 0)
        # now we're no longer subscribed as no more messages can be delivered
        # to any channels we were listening to
        assert p.subscribed is False

        # subscribing again flips the flag back
        await sub_func(keys[0])
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(sub_type, keys[0], 1)

        # unsubscribe again
        await unsub_func()
        assert p.subscribed is True
        # subscribe to another channel before reading the unsubscribe response
        await sub_func(keys[1])
        assert p.subscribed is True
        # read the unsubscribe for key1
        assert await wait_for_message(p) == make_message(unsub_type, keys[0], 0)
        # we're still subscribed to key2, so subscribed should still be True
        assert p.subscribed is True
        # read the key2 subscribe message
        assert await wait_for_message(p) == make_message(sub_type, keys[1], 1)
        await unsub_func()
        # haven't read the message yet, so we're still subscribed
        assert p.subscribed is True
        assert await wait_for_message(p) == make_message(unsub_type, keys[1], 0)
        # now we're finally unsubscribed
        assert p.subscribed is False

    @pytest.mark.asyncio
    async def test_subscribe_property_with_channels(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'channel')
        await self._test_subscribed_property(**kwargs)

    @pytest.mark.asyncio
    async def test_subscribe_property_with_patterns(self, r):
        kwargs = make_subscribe_test_data(r.pubsub(), 'pattern')
        await self._test_subscribed_property(**kwargs)

    @pytest.mark.asyncio
    async def test_ignore_all_subscribe_messages(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)

        checks = (
            (p.subscribe, 'foo'),
            (p.unsubscribe, 'foo'),
            # (p.psubscribe, 'f*'),
            # (p.punsubscribe, 'f*'),
        )

        assert p.subscribed is False
        for func, channel in checks:
            assert await func(channel) is None
            assert p.subscribed is True
            assert await wait_for_message(p) is None
        assert p.subscribed is False

    @pytest.mark.asyncio
    async def test_ignore_individual_subscribe_messages(self, r):
        p = r.pubsub()

        checks = (
            (p.subscribe, 'foo'),
            (p.unsubscribe, 'foo'),
            # (p.psubscribe, 'f*'),
            # (p.punsubscribe, 'f*'),
        )

        assert p.subscribed is False
        for func, channel in checks:
            assert await func(channel) is None
            assert p.subscribed is True
            message = await wait_for_message(p, ignore_subscribe_messages=True)
            assert message is None
        assert p.subscribed is False


class MockWorker:
    def __init__(self):
        self.message = None

    def handle(self, message):
        self.message = message


class TestPubSubMessages:
    """
    Bug: Currently in cluster mode publish command will behave different then in
         standard/non cluster mode. See (docs/Pubsub.md) for details.

         Currently StrictRedis instances will be used to test pubsub because they
         are easier to work with.
    """
    @staticmethod
    def get_strict_redis_node(port, host='127.0.0.1'):
        return StrictRedis(port=port, host=host)

    @pytest.mark.asyncio
    async def test_published_message_to_channel(self):
        node = self.get_strict_redis_node(7000)
        p = node.pubsub(ignore_subscribe_messages=True)
        await p.subscribe('foo')

        assert await node.publish('foo', 'test message') == 1

        message = await wait_for_message(p)
        assert isinstance(message, dict)
        assert message == make_message('message', 'foo', 'test message')

        # Cleanup pubsub connections
        p.close()

    @pytest.mark.asyncio
    @pytest.mark.xfail(reason='This test is buggy and fails randomly')
    async def test_publish_message_to_channel_other_server(self):
        """
        Test that pubsub still works across the cluster on different nodes
        """
        node_subscriber = self.get_strict_redis_node(7000)
        p = node_subscriber.pubsub(ignore_subscribe_messages=True)
        await p.subscribe('foo')

        node_sender = self.get_strict_redis_node(7001)
        # This should return 0 because of no connected clients to this server.
        assert await node_sender.publish('foo', 'test message') == 0

        message = await wait_for_message(p)
        assert isinstance(message, dict)
        assert message == make_message('message', 'foo', 'test message')

        # Cleanup pubsub connections
        p.close()

    @pytest.mark.asyncio
    @pytest.mark.xfail(reason='Pattern pubsub do not work currently')
    async def test_published_message_to_pattern(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        try:
            await p.subscribe('foo')
            await p.psubscribe('f*')
            # 1 to pattern, 1 to channel
            assert await r.publish('foo', 'test message') == 2

            message1 = await wait_for_message(p)
            message2 = await wait_for_message(p)
            assert isinstance(message1, dict)
            assert isinstance(message2, dict)

            expected = [
                make_message('message', 'foo', 'test message'),
                make_message('pmessage', 'foo', 'test message', pattern='f*'),
            ]

            assert message1 in expected
            assert message2 in expected
            assert message1 != message2
        finally:
            await p.unsubscribe('foo')
            await p.punsubscribe('f*')

    @pytest.mark.asyncio
    async def test_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        try:
            worker = MockWorker()
            await p.subscribe(foo=worker.handle)
            assert await r.publish('foo', 'test message') == 1
            assert await wait_for_message(p) is None
            assert worker.message == make_message(
                'message', 'foo', 'test message')
        finally:
            await p.unsubscribe('foo')

    @pytest.mark.asyncio
    @pytest.mark.xfail(reason='Pattern pubsub do not work currently')
    async def test_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        worker = MockWorker()
        await p.psubscribe(**{'f*': worker.handle})
        assert await r.publish('foo', 'test message') == 1
        assert await wait_for_message(p) is None
        assert worker.message == make_message('pmessage', 'foo', 'test message',
                                              pattern='f*')

    @pytest.mark.asyncio
    @pytest.mark.xfail(reason='Pattern pubsub do not work currently')
    async def test_unicode_channel_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        channel = 'uni' + chr(4456) + 'code'
        worker = MockWorker()
        channels = {channel: worker.handle}
        print(channels)
        await p.subscribe(**channels)
        assert await r.publish(channel, 'test message') == 1
        assert await wait_for_message(p) is None
        assert worker.message == make_message(
            'message', channel, 'test message')

    @pytest.mark.asyncio
    @pytest.mark.xfail(reason='Pattern pubsub do not work currently')
    async def test_unicode_pattern_message_handler(self, r):
        p = r.pubsub(ignore_subscribe_messages=True)
        pattern = 'uni' + chr(4456) + '*'
        channel = 'uni' + chr(4456) + 'code'
        worker = MockWorker()
        await p.psubscribe(**{pattern: worker.handle})
        assert await r.publish(channel, 'test message') == 1
        assert await wait_for_message(p) is None
        assert worker.message == make_message('pmessage', channel,
                                              'test message', pattern=pattern)


class TestPubSubAutoDecoding:
    """
    These tests only validate that we get unicode values back
    """

    channel = 'uni' + chr(4456) + 'code'
    pattern = 'uni' + chr(4456) + '*'
    data = 'abc' + chr(4458) + '123'

    @staticmethod
    def make_message(kind, channel, data, pattern=None):
        return {
            'type': kind,
            'channel': channel,
            'pattern': pattern,
            'data': data,
        }

    @pytest.mark.asyncio
    async def test_channel_subscribe_unsubscribe(self, o):
        p = o.pubsub()
        await p.subscribe(self.channel)
        assert await wait_for_message(p) == self.make_message('subscribe',
                                                              self.channel, 1)

        await p.unsubscribe(self.channel)
        assert await wait_for_message(p) == self.make_message('unsubscribe',
                                                              self.channel, 0)

    @pytest.mark.asyncio
    @pytest.mark.xfail(reason='Pattern pubsub do not work currently')
    async def test_pattern_subscribe_unsubscribe(self, o):
        p = o.pubsub()
        await p.psubscribe(self.pattern)
        assert await wait_for_message(p) == self.make_message('psubscribe',
                                                              self.pattern, 1)

        await p.punsubscribe(self.pattern)
        assert await wait_for_message(p) == self.make_message('punsubscribe',
                                                              self.pattern, 0)

    @pytest.mark.asyncio
    async def test_channel_publish(self, o):
        p = o.pubsub(ignore_subscribe_messages=True)
        await p.subscribe(self.channel)
        await o.publish(self.channel, self.data)
        assert await wait_for_message(p) == self.make_message('message',
                                                              self.channel,
                                                              self.data)

    @pytest.mark.asyncio
    @pytest.mark.xfail(reason='Pattern pubsub do not work currently')
    async def test_pattern_publish(self, o):
        p = o.pubsub(ignore_subscribe_messages=True)
        await p.psubscribe(self.pattern)
        await o.publish(self.channel, self.data)
        assert await wait_for_message(p) == self.make_message('pmessage',
                                                              self.channel,
                                                              self.data,
                                                              pattern=self.pattern)

    @pytest.mark.asyncio
    async def test_channel_message_handler(self, o):
        p = o.pubsub(ignore_subscribe_messages=True)
        worker = MockWorker()
        await p.subscribe(**{self.channel: worker.handle})
        await o.publish(self.channel, self.data)
        assert await wait_for_message(p) is None
        assert worker.message == self.make_message('message', self.channel,
                                                   self.data)

        # test that we reconnected to the correct channel
        p.connection.disconnect()
        assert await wait_for_message(p) is None  # should reconnect
        new_data = self.data + 'new data'
        await o.publish(self.channel, new_data)
        assert await wait_for_message(p) is None
        assert worker.message == self.make_message('message', self.channel,
                                                   new_data)

    @pytest.mark.asyncio
    @pytest.mark.xfail(reason='Pattern pubsub do not work currently')
    async def test_pattern_message_handler(self, o):
        p = o.pubsub(ignore_subscribe_messages=True)
        worker = MockWorker()
        await p.psubscribe(**{self.pattern: worker.handle})
        await o.publish(self.channel, self.data)
        assert await wait_for_message(p) is None
        assert worker.message == self.make_message('pmessage', self.channel,
                                                   self.data,
                                                   pattern=self.pattern)

        # test that we reconnected to the correct pattern
        p.connection.disconnect()
        assert await wait_for_message(p) is None  # should reconnect
        new_data = self.data + 'new data'
        await o.publish(self.channel, new_data)
        assert await wait_for_message(p) is None
        assert worker.message == self.make_message('pmessage', self.channel,
                                                   new_data,
                                                   pattern=self.pattern)


class TestPubSubRedisDown:

    @pytest.mark.asyncio
    async def test_channel_subscribe(self, r):
        r = StrictRedis(host='localhost', port=6390)
        p = r.pubsub()
        with pytest.raises(ConnectionError):
            await p.subscribe('foo')


def test_pubsub_thread_publish():
    """
    This test will never fail but it will still show and be viable to use
    and to test the threading capability of the connectionpool and the publish
    mechanism.
    """
    startup_nodes = [{'host': '127.0.0.1', 'port': '7000'}]

    r = StrictRedisCluster(
        startup_nodes=startup_nodes,
        max_connections=16,
        max_connections_per_node=16,
    )

    async def t_run(rc):
        for _ in range(0, 50):
            await rc.publish('foo', 'bar')
            await rc.publish('bar', 'foo')
            await rc.publish('asd', 'dsa')
            await rc.publish('dsa', 'asd')
            await rc.publish('qwe', 'bar')
            await rc.publish('ewq', 'foo')
            await rc.publish('wer', 'dsa')
            await rc.publish('rew', 'asd')

        # Use this for debugging
        # print(rc.connection_pool._available_connections)
        # print(rc.connection_pool._in_use_connections)
        # print(rc.connection_pool._created_connections)

    try:
        loop = asyncio.get_event_loop()
        loop.run_until_complete(asyncio.gather(*(t_run(r) for _ in range(10))))
    except Exception as e:
        print(e)
        print('Error: unable to start thread')
#
#
# class TestPubSubPubSubSubcommands:
#     """
#     Test Pub/Sub subcommands of PUBSUB
#     @see https://redis.io/commands/pubsub
#     """
#
#     @skip_if_redis_py_version_lt('2.10.6')
#     async def test_pubsub_channels(self, r):
#         r.pubsub(ignore_subscribe_messages=True).subscribe('foo', 'bar', 'baz', 'quux')
#         channels = sorted(r.pubsub_channels())
#         assert channels == [b('bar'), b('baz'), b('foo'), b('quux')]
#
#     @skip_if_redis_py_version_lt('2.10.6')
#     def test_pubsub_numsub(self, r):
#         r.pubsub(ignore_subscribe_messages=True).subscribe('foo', 'bar', 'baz')
#         r.pubsub(ignore_subscribe_messages=True).subscribe('bar', 'baz')
#         r.pubsub(ignore_subscribe_messages=True).subscribe('baz')
#
#         channels = [(b('bar'), 2), (b('baz'), 3), (b('foo'), 1)]
#         assert channels == sorted(r.pubsub_numsub('foo', 'bar', 'baz'))
#
#     @skip_if_redis_py_version_lt('2.10.6')
#     def test_pubsub_numpat(self, r):
#         r.pubsub(ignore_subscribe_messages=True).psubscribe('*oo', '*ar', 'b*z')
#         assert r.pubsub_numpat() == 3
