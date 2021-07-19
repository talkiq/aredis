<a name="2.0.4"></a>
## 2.0.4 (2021-07-19)

#### Bug Fixes

* **connection:**  avoid hiding timeout errors on connect (#11) ([7186a9b4](7186a9b4))

<a name="2.0.3"></a>
## 2.0.3 (2021-07-02)

#### Bug Fixes

* **cluster:**  fix broken retry logic for ClusterDownErrors (#8) ([a16c35d2](a16c35d2))

<a name="2.0.2"></a>
## 2.0.2 (2021-06-23)

#### Bug Fixes

* **cluster**:  fixup invalid `Cluster.scan_iter()` default value for `type` param (#10) ([2064e842](2064e842))
* **cluster**:  fixup support for Python 3.6 in `Cluster.scan_iter()` (#10) ([2064e842](2064e842))
* **cluster**:  avoid potential pagination race condition in `Cluster.scan_iter()` (#10) ([2064e842](2064e842))

<a name="2.0.1"></a>
## 2.0.1 (2021-06-18)

#### Performance

* **cluster:**  run scan operation in parallel across nodes (#7) ([4fbb46d0](4fbb46d0))

<a name="2.0.0"></a>
## 2.0.0 (2021-06-08)

This is a breaking change! As of v1.1.8+, `aredis` has been hard-forked and
renamed to `yaaredis`. The `aredis` changelog is maintained below (see
`1.1.8`_ for posterity).

#### Breaking Changes

* rename package to `yaaredis` ([1ea13dd](1ea13dd))
* remove all deprecated features ([f4699a45](f4699a45)):
  * deprecated methods have been removed
  * deprecated options now raise Exceptions
  * some ignorable deprecations have been replaced with error logs
* **pipeline:**  auto-execute all remaining commands at end of `with` block ([814ca2bb](814ca2bb))
* **pool**:  `ConnectionPool.get_connection()` is now a coroutine ([32969ed9](32969ed9))

#### Features

* **auth:**  add support for Redis 5+ user&pass auth ([ca96dc6d](ca96dc6d))
* **client:**  add option to init with client name ([f00c361a](f00c361a))
* **commands:**  add support for SET's KEEPTTL option ([22c62f04](22c62f04))
* **pool:**  implement blocking connection pool ([32969ed9](32969ed9)):
  * this is available as the `BlockingConnectionPool` class; note that it does
    not yet support cluster mode
* **scan:**  add TYPE option ([0625dd8c](0625dd8c))

#### Bug Fixes

* **client:**  prevent retry_on_timeout from affecting ConnectionError ([6626ebce](6626ebce)):
  * ie. `retry_on_timeout` now only comes into play for timeouts
* **cluster:**  avoid runtime error on disconnected client ([740ea19f](740ea19f)):
  * prevents a race condition within `yaaredis` from raising occasional errors
* **connection:**
  *  always reduce count on force disconnect ([b49b3c65](b49b3c65)):
    * fixes some instances of "connection leaking"
  *  expose initial `connect()` error message ([2073f576](2073f576)):
    * by forwarding this message, this should make some debugging a bit easier
* **pubsub:**  reraise CancelledError during _execute ([90dd2641](90dd2641)):
  * prevents an issue where a cancelled coroutine may continue indefinitely

#### Performance

* **cluster:**  support MGET and MSET for hashed keys ([08180835](08180835)):
  * see the following redis documentation for more info:
    https://redis.io/topics/cluster-tutorial#redis-cluster-data-sharding
    you'll need to make use of "hash slots" to get the full benefit of this
    feature

#### Internal

* convert build system to poetry ([b86d695](b86d695)):
  * note that this should not affect end-users, only yaaredis developers
* publish pypi wheels from CI ([05c2265](05c2265)):
  * we should now have wheels for various versions of Python for all releases
  * we currently support manylinux wheels for all compatible Python versions

<a name="1.1.8"></a>
## 1.1.8
* Fixbug: connection is disconnected before idel check, valueError will be raised if a connection(not exist) is removed from connection list
* Fixbug: abstract compat.py to handle import problem of asyncio.future
* Fixbug: When cancelling a task, CancelledError exception is not propagated to client
* Fixbug: XREAD command should accept 0 as a block argument
* Fixbug: In redis cluster mode, XREAD command does not function properly
* Fixbug: slave connection params when there are no slaves

<a name="1.1.7"></a>
## 1.1.7
* Fixbug: ModuleNotFoundError raised when install aredis 1.1.6 with Python3.6

<a name="1.1.6"></a>
## 1.1.6
* Fixbug: parsing stream messgae with empty payload will cause error(#116)
* Fixbug: Let ClusterConnectionPool handle skip_full_coverage_check (#118)
* New: threading local issue in coroutine, use contextvars instead of threading local in case of the safety of thread local mechanism being broken by coroutine (#120)
* New: support Python 3.8

<a name="1.1.5"></a>
## 1.1.5
* new: Dev conn pool max idle time (#111) release connection if max-idle-time exceeded
* update: discard travis-CI
* Fix bug: new stream id used for test_streams

<a name="1.1.4"></a>
## 1.1.4
* fix bug: fix cluster port parsing for redis 4+(node info)
* fix bug: wrong parse method of scan_iter in cluster mode
* fix bug: When using "zrange" with "desc=True" parameter, it returns a coroutine without "await"
* fix bug: do not use stream_timeout in the PubSubWorkerThread
* opt: add socket_keepalive options
* new: add ssl param in get_redis_link to support ssl mode
* new: add ssl_context to StrictRedis constructor and make it higher priority than ssl parameter

<a name="1.1.3"></a>
## 1.1.3
* allow use of zadd options for zadd in sorted sets
* fix bug: use inspect.isawaitable instead of typing.Awaitable to judge if an object is awaitable
* fix bug: implicitly disconnection on cancelled error (#84)
* new: add support for `streams`(including commands not officially released, see `streams <http://aredis.readthedocs.io/en/latest/streams.html>`_ )

<a name="1.1.2"></a>
## 1.1.2
* fix bug: redis command encoding bug
* optimization: sync change on acquring lock from redis-py
* fix bug: decrement connection count on connection disconnected
* fix bug: optimize code proceed single node slots
* fix bug: initiation error of aws cluster client caused by not appropiate function list used
* fix bug: use `ssl_context` instead of ssl_keyfile,ssl_certfile,ssl_cert_reqs,ssl_ca_certs in intialization of connection_pool

<a name="1.1.1"></a>
## 1.1.1
* fix bug: connection with unread response being released to connection pool will lead to parse error, now this kind of connection will be destructed directly. `related issue <https://github.com/NoneGG/aredis/issues/52>`_
* fix bug: remove Connection.can_read check which may lead to block in awaiting pubsub message. Connection.can_read api will be deprecated in next release. `related issue <https://github.com/NoneGG/aredis/issues/56>`_
* add c extension to speedup crc16, which will speedup cluster slot hashing
* add error handling for asyncio.futures.Cancelled error, which may cause error in response parsing.
* sync optimization of client list made by swilly22 from redis-py
* add support for distributed lock using redis cluster

<a name="1.1.0"></a>
## 1.1.0
* sync optimization of scripting from redis-py made by `bgreenberg <https://github.com/bgreenberg-eb>`_ `related pull request <https://github.com/andymccurdy/redis-py/pull/867>`_
* sync bug fixed of `geopos` from redis-py made by `categulario <https://github.com/categulario>`_ `related pull request <https://github.com/andymccurdy/redis-py/pull/888>`_
* fix bug which makes pipeline callback function not executed
* fix error caused by byte decode issues in sentinel
* add basic transaction support for single node in cluster
* fix bug of get_random_connection reported by myrfy001

<a name="1.0.9"></a>
## 1.0.9
* fix bug of pubsub, in some env AssertionError is raised because connection is used again after reader stream being fed eof
* add reponse decoding related options(`encoding` & `decode_responses`), make client easier to use
* add support for command `cluster forget`
* add support for command option `spop count`

<a name="1.0.8"></a>
## 1.0.8
* fix initialization bug of redis cluster client
* add example to explain how to use `client reply on | off | skip`

<a name="1.0.7"></a>
## 1.0.7
* introduce loop argument to aredis
* add support for command `cluster slots`
* add support for redis cluster

<a name="1.0.6"></a>
## 1.0.6
* bitfield set/get/incrby/overflow supported
* new command `hstrlen` supported
* new command `unlink` supported
* new command `touch` supported

<a name="1.0.5"></a>
## 1.0.5
* fix bug in setup.py when using pip to install aredis

<a name="1.0.4"></a>
## 1.0.4
* add support for command `pubsub channel`, `pubsub numpat` and `pubsub numsub`
* add support for command `client pause`
* reconsitution of commands to make develop easier(which is transparent to user)

<a name="1.0.2"></a>
## 1.0.2
* add support for cache (Cache and HerdCache class)
* fix bug of `PubSub.run_in_thread`

<a name="1.0.1"></a>
## 1.0.1
* add scan_iter, sscan_iter, hscan_iter, zscan_iter and corresponding unit tests
* fix bug of `PubSub.run_in_thread`
* add more examples
* change `Script.register` to `Script.execute`
