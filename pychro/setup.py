from distutils.core import setup

setup(name='pychro',
      version='0.6.3',
      packages=['pychro'],
      package_data={'pychro':['libpychroc.so', 'PychroCLib.dll']},
      author='Jon Turner',
      description='Memory-mapped message journal',
      url='https://github.com/jontuk/pychro',
      license='Apache 2.0',
      classifiers=[ 'Development Status :: 3 - Alpha',
                    'Intended Audience :: Developers',
                    'License :: OSI Approved :: Apache Software License',
                    'Programming Language :: Python :: 3.4' ]
)
