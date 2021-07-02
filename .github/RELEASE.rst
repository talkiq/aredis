Releasing New Versions
======================

TODO: fill this out more completely!

.. code-block:: console

    $ poetry version <kind>
    $ clog -C CHANGELOG.md --setversion <new> -f <old>
    # edit ./CHANGELOG.md manually to make it easier to read, if need be
    $ git commit -am 'chore(release): bump version' && git push
    $ git tag x.y.z && git push origin x.y.z

Then create a Github release. If you have access to a build system other than
our CI (say, if you're running on OSX), you can provide bonus wheels with:

.. code-block:: console

    $ rm -rf dist/
    $ poetry build -fwheel
    $ poetry upload
