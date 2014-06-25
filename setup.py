#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys


try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup, find_packages


readme = open('README.rst').read()
history = open('HISTORY.rst').read().replace('.. :changelog:', '')

requirements = [
    # TODO: put package requirements here
    'setuptools',
]

test_requirements = [
    # TODO: put package test requirements here
]

setup(
    name='cs.eyrie',
    version='0.1.0',
    description='Primitives for building ZMQ pipelines.',
    long_description=readme + '\n\n' + history,
    author='CrowdStrike, Inc.',
    author_email='csoc@crowdstrike.com',
    url='https://github.com/CrowdStrike/cs.eyrie',
    packages=find_packages(),
    #package_dir={'': 'cs'},
    include_package_data=True,
    namespace_packages=['cs'],
    install_requires=requirements,
    license="BSD",
    zip_safe=False,
    keywords='zmq async tornado',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: BSD License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
    ],
    test_suite='tests',
    tests_require=test_requirements
)
