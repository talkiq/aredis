talkiq/yaaredis
===============

|circleci| |pypi-version| |python-versions|

.. |circleci| image:: https://img.shields.io/circleci/project/github/talkiq/yaaredis/master.svg?style=flat-square
    :alt: CircleCI Test Status
    :target: https://circleci.com/gh/talkiq/yaaredis/tree/master

.. |pypi-version| image:: https://img.shields.io/pypi/v/yaaredis.svg?style=flat-square&label=PyPI
    :alt: Latest PyPI Release
    :target: https://pypi.org/project/yaaredis/

.. |python-versions| image:: https://img.shields.io/pypi/pyversions/yaaredis.svg?style=flat-square&label=Python%20Versions
    :alt: Compatible Python Versions
    :target: https://pypi.org/project/yaaredis/

``yaaredis`` (Yet Another Async Redis (client)) is a fork of
`aredis <https://github.com/NoneGG/aredis>`_, which itself was ported from
`redis-py <https://github.com/andymccurdy/redis-py>`_. ``yaaredis`` provides an
efficient and user-friendly async redis client with support for Redis Server,
Cluster, and Sentinels.

To get more information please read the `full documentation`_ managed by the
upstream ``aredis`` repo. We are working on hosting our own as the projects
diverge -- stay tuned!

Installation
------------

``yaaredis`` requires a running Redis server. To install yaaredis, simply:

.. code-block:: console

    python3 -m pip install yaaredis

or from source:

.. code-block:: console

    python3 -m pip install .

Note that ``yaaredis`` also supports using ``hiredis`` as a drop-in performance
improvements. You can either install ``hiredis`` separately or make use of the
PyPI extra to make use of this functionality:

.. code-block:: console

    python3 -m pip install yaaredis[hiredis]

Getting started
---------------

We have `various examples`_ in this repo which you may find useful. A few more
specific cases are listed below.

Single Node Client
^^^^^^^^^^^^^^^^^^

.. code-block:: python

    import asyncio
    from yaaredis import StrictRedis

    async def example():
        client = StrictRedis(host='127.0.0.1', port=6379, db=0)
        await client.flushdb()
        await client.set('foo', 1)
        assert await client.exists('foo') is True
        await client.incr('foo', 100)

        assert int(await client.get('foo')) == 101
        await client.expire('foo', 1)
        await asyncio.sleep(0.1)
        await client.ttl('foo')
        await asyncio.sleep(1)
        assert not await client.exists('foo')

    asyncio.run(example())

Cluster Client
^^^^^^^^^^^^^^

.. code-block:: python

    import asyncio
    from yaaredis import StrictRedisCluster

    async def example():
        client = StrictRedisCluster(host='172.17.0.2', port=7001)
        await client.flushdb()
        await client.set('foo', 1)
        await client.lpush('a', 1)
        print(await client.cluster_slots())

        await client.rpoplpush('a', 'b')
        assert await client.rpop('b') == b'1'

    asyncio.run(example())
    # {(10923, 16383): [{'host': b'172.17.0.2', 'node_id': b'332f41962b33fa44bbc5e88f205e71276a9d64f4', 'server_type': 'master', 'port': 7002},
    # {'host': b'172.17.0.2', 'node_id': b'c02deb8726cdd412d956f0b9464a88812ef34f03', 'server_type': 'slave', 'port': 7005}],
    # (5461, 10922): [{'host': b'172.17.0.2', 'node_id': b'3d1b020fc46bf7cb2ffc36e10e7d7befca7c5533', 'server_type': 'master', 'port': 7001},
    # {'host': b'172.17.0.2', 'node_id': b'aac4799b65ff35d8dd2ad152a5515d15c0dc8ab7', 'server_type': 'slave', 'port': 7004}],
    # (0, 5460): [{'host': b'172.17.0.2', 'node_id': b'0932215036dc0d908cf662fdfca4d3614f221b01', 'server_type': 'master', 'port': 7000},
    # {'host': b'172.17.0.2', 'node_id': b'f6603ab4cb77e672de23a6361ec165f3a1a2bb42', 'server_type': 'slave', 'port': 7003}]}

Benchmark
---------

Please run test scripts in the ``benchmarks`` directory to confirm the
benchmarks. For a benchmark in the original yaaredis author's environment
please see: `benchmark`_.

Contributing
------------

Developer? See our `guide`_ on how you can contribute.

.. _benchmark: http://aredis.readthedocs.io/en/latest/benchmark.html
.. _full documentation: http://aredis.readthedocs.io/en/latest/
.. _guide: https://github.com/talkiq/yaaredis/blob/master/.github/CONTRIBUTING.rst
.. _various examples: https://github.com/talkiq/yaaredis/tree/master/examples
