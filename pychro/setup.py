from setuptools import setup, Extension

setup(name='pychro',
      version='0.9.0',
      packages=['pychro'],
      author='Jon Turner',
      description='Memory-mapped message journal',
      url='https://github.com/jontuk/pychro',
      license='Apache 2.0',
      python_requires='>=3.8',
      ext_modules=[Extension('pychro.pychroc', sources=['_pychroc/pychroc.c'])],
      classifiers=['Development Status :: 4 - Beta',
                   'Intended Audience :: Developers',
                   'License :: OSI Approved :: Apache Software License',
                   'Operating System :: POSIX :: Linux',
                   'Programming Language :: Python :: Implementation :: CPython',
                   'Programming Language :: Python :: 3.8',
                   'Programming Language :: Python :: 3.9',
                   'Programming Language :: Python :: 3.10',
                   'Programming Language :: Python :: 3.11',
                   'Programming Language :: Python :: 3.12',
                   'Programming Language :: Python :: 3.13']
)
