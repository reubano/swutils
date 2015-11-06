#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
import swutils
import pkutils

try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

sys.dont_write_bytecode = True
requirements = list(pkutils.parse_requirements('requirements.txt'))
dev_requirements = list(pkutils.parse_requirements('dev-requirements.txt'))
dependencies = list(pkutils.parse_requirements('requirements.txt', dep=True))
readme = pkutils.read('README.md')
changes = pkutils.read('CHANGES.rst').replace('.. :changelog:', '')
license = swutils.__license__

setup(
    name=swutils.__title__,
    version=swutils.__version__,
    description=swutils.__description__,
    long_description=readme,
    author=swutils.__author__,
    author_email=swutils.__email__,
    url='https://github.com/reubano/swutils',
    py_modules=['swutils'],
    include_package_data=True,
    install_requires=requirements,
    dependency_links=dependencies,
    tests_require=dev_requirements,
    license=license,
    zip_safe=False,
    keywords=[swutils.__title__],
    classifiers=[
        pkutils.LICENSES[license],
        'Development Status :: 4 - Beta',
        'Natural Language :: English',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Environment :: Console',
        'Intended Audience :: Developers',
        'Intended Audience :: End Users/Desktop',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: POSIX',
        'Operating System :: POSIX :: Linux',
    ],
    platforms=['MacOS X', 'Windows', 'Linux'],
)
