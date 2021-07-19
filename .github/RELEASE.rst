Releasing New Versions
======================

TODO: fill this out more completely!

.. code-block:: console

    $ poetry version <kind>
    $ clog -C CHANGELOG.md --setversion <new> -f <old>
    # edit ./CHANGELOG.md manually to make it easier to read, if need be
    $ git commit -am 'chore(release): bump version' && git push
    $ git tag x.y.z && git push origin x.y.z

Then create a Github release and fill in the changelog -- copying the entry
created above works pretty nicely, but you should also expand upon anything
especially interesting to users: eg. how to handle breaking changes and such.

If you have access to a build system other than our CI (say, if you're running
on OSX), you can provide bonus wheels with:

.. code-block:: console

    $ rm -rf dist/
    $ poetry build -fwheel
    $ poetry publish

If you're feeling even more generous, you can build for multiple Python
versions with the following (TODO: do this via poetry):

.. code-block:: console

    $ rm -rf dist/
    $ for version in 3.6 3.7 3.8 3.9; do poetry env use $version; poetry run pip wheel yaaredis -w dist/; done
    $ poetry publish
