Contributing to ``aredis``
==========================

Thanks for contributing to ``aredis``! We appreciate contributions of any size
and hope to make it easy for you to dive in. Here's the thousand-foot overview
of how we've set this project up.

Local Development
~~~~~~~~~~~~~~~~~

We recommend working out of a virtual environment:

- create and activate a virtual environment: ``python3 -m venv venv && source venv/bin/activate``
- install test dependencies: ``pip install -r test_dependencies.txt``
- install library from local path: ``pip install -e .``

Then, you can run any tests manually as described in the next section.

Testing
-------

Tests are run with `pytest`_ and vary between cluster mode and single-instance
mode. You'll need a locally running redis instance/cluster started first -- we
recommend using docker containers to make this simple, but a locally installed
and running redis server would work just as well.

For example, you can run single-instance tests with:

.. code-block:: console

    docker run --rm -d -p6379:6379 --name=redis redis:5
    python3 -m pytest tests/client

or cluster tests with:

.. code-block:: console

    docker run --rm -d -p5000-5002:5000-5002 -p7000-7007:7000-7007 --name=redis-cluster grokzen/redis-cluster:5.0.5
    python3 -m pytest tests/cluster

Note that running the cluster tests on OSX is **currently not supported** since
accessing the docker bridge network is not possible from your host machine.
If you run ``pytest`` from within the same docker network, you may be able to
make this work -- otherwise, note that these tests are run automatically by our
CI provider.

Submitting Changes
------------------

Please send us a `Pull Request`_ with a clear list of what you've done. When
you submit a PR, we'd appreciate test coverage of your changes (and feel free
to test other things; we could always use more and better tests!).

Please make sure all tests pass and your commits are atomic (one feature per
commit).

Always write a clear message for your changes. We think the
`conventional changelog`_ message format is pretty cool and try to stick to it
as best we can (we even generate release notes from it automatically!).

Roughly speaking, we'd appreciate if your commits looked list this:

.. code-block:: console

    feat(cluster): enabled support for read replicas

    Modified read/write connection pooling to prefer sending read requests to
    read replicas. Readonly-style connection pools are unchanged.

The first line is the most specific in this format, it should have the format
``type(project): message``, where:

- ``type`` is one of ``feat``, ``fix``, ``docs``, ``refactor``, ``style``, ``perf``, ``test``, or ``chore``
- ``project`` is ``auth``, ``bigquery``, ``datastore``, etc.
- ``message`` is a concise description of the patch and brings the line to no more than 72 characters

Coding Conventions
------------------

We use `pre-commit`_ to manage our coding conventions and linting. You can
install it with ``pip install pre-commit`` and set it to run pre-commit hooks
for ``gcloud-aio`` by running ``pre-commit install``. The same linters get run
in CI against all changesets.

You can also run ``pre-commit`` in an ad-hoc fashion by calling
``pre-commit run --all-files``.

Other than the above enforced standards, we like code that is easy-to-read for
any new or returning contributors with relevant comments where appropriate.

Releases
--------

If you are a maintainer looking to release a new version, see our
`Release documentation`_.

.. _Pull Request: https://github.com/talkiq/gcloud-aio/pull/new/master
.. _Release documentation: https://github.com/talkiq/gcloud-aio/blob/master/.github/RELEASE.rst
.. _conventional changelog: https://github.com/conventional-changelog/conventional-changelog
.. _pre-commit: http://pre-commit.com/
.. _pytest: https://pytest.readthedocs.io/en/latest/

Thanks for your contribution!

With love,
Vi Engineering
