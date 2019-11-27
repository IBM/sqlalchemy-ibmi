#!/usr/bin/env python

from setuptools import setup
import os
import re


v = open(os.path.join(os.path.dirname(__file__), 'pyodbc_sa', '__init__.py'))
VERSION = re.compile(r".*__version__ = '(.*?)'", re.S).match(v.read()).group(1)
v.close()

readme = os.path.join(os.path.dirname(__file__), 'README.md')
    

setup(
         name='pyodbc_sa',
         version=VERSION,
         license='Apache License 2.0',
         description==,
         author='IBM Application Development Team',
         author_email='opendev@us.ibm.com',
         keywords='sqlalchemy database ibm ibmi db2',
         long_description_content_type='text/markdown',
         classifiers=[
            'Development Status :: 4 - Beta',
            'Intended Audience :: Developers',
            'License :: OSI Approved :: Apache License 2.0',
            'Operating System :: OS Independent',
            'Topic :: Databases :: Front-end, middle-tier'
        ],
         long_description=open(readme).read(),
         platforms='All',
         install_requires=['sqlalchemy>=1.3', 'pyodbc'],
         packages=['sqlalchemy-ibmi'],
        entry_points={
         'sqlalchemy.dialects': [
                     'ibmi=pyodbc_sa.backend:DB2Dialect_pyodbc'
                    ]
       },
       zip_safe=False,
       tests_require=['nose >= 0.11'],
     )
