from distutils.core import setup, Extension

setup(name='pychro',
      version='0.8.2',
      packages=['pychro'],
      author='Jon Turner',
      description='Memory-mapped message journal',
      url='https://github.com/jontuk/pychro',
      license='Apache 2.0',
      ext_modules=[Extension('pychro.pychroc', sources=['_pychroc/pychroc.c'])],
      classifiers=['Development Status :: 4 - Beta',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: Apache Software License',
                   'Operating System :: POSIX :: Linux',
                   'Programming Language :: Python :: Implementation :: CPython',
                   'Programming Language :: Python :: 3.4',
                   'Programming Language :: Python :: 3.5']
)
