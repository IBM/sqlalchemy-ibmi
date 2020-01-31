#!/usr/bin/env python

from setuptools import setup
import os
import re

v = open(
    os.path.join(
        os.path.dirname(__file__),
        'sqlalchemy_ibmi',
        '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.md')

setup(
    name='sqlalchemy-ibmi',
    version=VERSION,
    license='Apache License 2.0',
    description='SQLAlchemy support for Db2 on IBM i',
    author='IBM',
    keywords='sqlalchemy database ibm ibmi db2',
    long_description_content_type='text/markdown',
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: Apache License 2.0',
        'Operating System :: OS Independent',
        'Topic :: Databases :: Front-end, middle-tier'
    ],
    long_description=open(readme).read(),
    platforms='All',
    install_requires=[
        'sqlalchemy>=1.3',
        'pyodbc>=4.0'
    ],
    packages=[
        'sqlalchemy_ibmi'
    ],
    entry_points={
        'sqlalchemy.dialects': [
            'ibmi=sqlalchemy_ibmi.backend:AS400Dialect_pyodbc'
        ]
    },
    zip_safe=False,
)
