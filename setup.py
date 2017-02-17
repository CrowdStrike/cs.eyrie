#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys


try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

requirements = [
    # TODO: put package requirements here
    'cryptography',
    'pyramid',
    'pyzmq',
    'setproctitle',
    'setuptools',
    'tornado',
    'six',
    'zope.interface',
]
if sys.version < '3':
    requirements.append('unicodecsv')

test_requirements = [
    # TODO: put package test requirements here
    'python-dateutil',
    'dogpile.cache',
]

setup(
    name='cs.eyrie',
    version='0.5.6',
    description='Primitives for building ZMQ pipelines.',
    long_description=readme + '\n\n' + history,
    author='CrowdStrike, Inc.',
    author_email='csoc@crowdstrike.com',
    url='https://github.com/CrowdStrike/cs.eyrie',
    packages=find_packages(),
    include_package_data=True,
    namespace_packages=['cs'],
    install_requires=requirements,
    extras_require={
        'Dogpile':  ["dogpile.cache"],
        'Kafka':  ["kafka-python", "kazoo", "gevent"],
        'PostgreSQL':  ["python-dateutil", "sixfeetup.bowab", "zope.sqlalchemy"],
    },
    license="BSD",
    zip_safe=False,
    keywords='zmq async tornado postgresql',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Topic :: System :: Distributed Computing',
    ],
    test_suite='tests.test_collector',
    tests_require=test_requirements,
    entry_points={
        'paste.app_factory': 'main = cs.eyrie:main',
        'console_scripts': [
            'eyrie_logger = cs.eyrie.scripts.logger:main',
            'eyrie_injector = cs.eyrie.scripts.log_injector:main',
            'kafka_consumer = cs.eyrie.scripts.kafka_consumer:main [Kafka]',
            'kafka_router = cs.eyrie.scripts.kafka_router:main [Kafka]',
        ],
        'dogpile.cache': [
            'cs.eyrie.sharded_redis = cs.eyrie.config:ShardedRedisBackend',
            'cs.eyrie.pymemcache = cs.eyrie.config:PyMemcacheBackend',
        ]
    },
)
