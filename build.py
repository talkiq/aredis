import traceback

from setuptools import Extension
from setuptools.command.build_ext import build_ext


ext_modules = [
    Extension('yaaredis.speedups', sources=['yaaredis/speedups.c']),
]


class SafeBuildExt(build_ext):
    def run(self):
        try:
            build_ext.run(self)
        except Exception:
            print('Could not compile C extension.')
            traceback.print_exc()

    def build_extension(self, ext):
        try:
            build_ext.build_extension(self, ext)
        except Exception:
            print('Could not compile C extension.')
            traceback.print_exc()


def build(setup_kwargs):
    """
    This function is mandatory in order to build the extensions.
    """
    setup_kwargs['ext_modules'] = ext_modules
    setup_kwargs['cmdclass'] = {'build_ext': SafeBuildExt}
