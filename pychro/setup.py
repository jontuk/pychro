from distutils.core import setup

setup(name='pychro',
      version='0.7.7',
      packages=['pychro'],
      package_data={'pychro':['libpychroc_linux.so', 'libpychroc_darwin.so', 'PychroCLib.dll']},
      author='Jon Turner',
      description='Memory-mapped message journal',
      url='https://github.com/jontuk/pychro',
      license='Apache 2.0',
      classifiers=[ 'Development Status :: 4 - Beta',
                    'Intended Audience :: Developers',
                    'License :: OSI Approved :: Apache Software License',
                    'Programming Language :: Python :: 3.4' ]
)
