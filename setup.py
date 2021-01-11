from setuptools import setup

setup(
    name='hivejdbc',
    version='0.2.2',
    author='OpenBigDataPlatform',
    license='Apache-2.0',
    url='https://github.com/OpenBigDataPlatform/hivejdbc',
    description='Hive database driver via jdbc',
    long_description=open('README.md').read(),
    long_description_content_type='text/markdown',
    keywords='dbapi jdbc hive hadoop',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Programming Language :: Java',
        'Programming Language :: Python :: 3',
        'Topic :: Database',
        'Topic :: Software Development :: Libraries :: Java Libraries',
        'Topic :: Software Development :: Libraries :: Python Modules',
    ],

    packages=['hivejdbc'],
    install_requires=[
        'pyjdbc>=0.2.2'
    ],
)
