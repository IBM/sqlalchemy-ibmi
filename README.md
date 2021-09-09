[![Latest version released on PyPi](https://img.shields.io/pypi/v/sqlalchemy-ibmi.svg)](https://pypi.org/project/sqlalchemy-ibmi)
![Supported Python Version Badge](https://img.shields.io/pypi/pyversions/sqlalchemy-ibmi.svg)
[![GitHub Actions status | sdras/awesome-actions](https://github.com/IBM/sqlalchemy-ibmi/workflows/Build%20PR/badge.svg)](https://github.com/IBM/sqlalchemy-ibmi/actions?workflow=Build+PR)
[![Documentation Status](https://readthedocs.org/projects/sqlalchemy-ibmi/badge/?version=latest)](https://sqlalchemy-ibmi.readthedocs.io/en/latest/?badge=latest)


SQLAlchemy adapter for IBM i
=========

The IBM i SQLAlchemy adapter provides a [SQLAlchemy](https://www.sqlalchemy.org/) interface to Db2 for [IBM i](https://en.wikipedia.org/wiki/IBM_i).

**Please note that this project is still under active development. Please
 report any bugs in the issue tracker** :rotating_light: 

```python
import sqlalchemy as sa

# see documentation for available connection options
# pass connection options in url query string, eg.
# engine = sa.create_engine("ibmi://user:pass@host?autocommit=true&timeout=10"
# find usage of create_engine database urls here
# https://docs.sqlalchemy.org/en/13/core/engines.html#database-urls
# this is the base connection which connects to *LOCAL on the host

engine = sa.create_engine("ibmi://user:pass@host")

cnxn = engine.connect()
metadata = sa.MetaData()
table = sa.Table('table_name', metadata, autoload=True, autoload_with=engine)

query = sa.select([table])

result = cnxn.execute(query)
result = result.fetchall()

# print first entry
print(result[0])

```

Installation
-------------
```sh
pip install sqlalchemy-ibmi
```
 
Feature Support
----------------
- SQLAlchemy ORM  - Python object based automatically constructed SQL
- SQLAlchemy Core - schema-centric SQL Expression Language

Documentation
-------------

The documentation for the SQLAlchemy adapter for IBM i can be found at:
https://sqlalchemy-ibmi.readthedocs.io/en/latest/


Known Limitations 
-------------------------------------------------------------
1) Non-standard SQL queries are not supported. e.g. "SELECT ? FROM TAB1"
2) For updations involving primary/foreign key references, the entries should be made in correct order. Integrity check is always on and thus the primary keys referenced by the foreign keys in the referencing tables should always exist in the parent table.
3) Unique key which contains nullable column not supported
4) UPDATE CASCADE for foreign keys not supported
5) DEFERRABLE INITIALLY deferred not supported
6) Subquery in ON clause of LEFT OUTER JOIN not supported

Contributing to the IBM i SQLAlchemy adapter
----------------------------------------
Please read the [contribution guidelines](contributing/CONTRIBUTING.md).

```
The developer sign-off should include the reference to the DCO in remarks(example below):
DCO 1.1 Signed-off-by: Random J Developer <random@developer.org>
```

License
-------

[Apache 2.0](LICENSE)

Credits
-------
- ibm_db_sa for SQLAlchemy was first produced by IBM Inc., targeting version 0.4.
- The library was ported for version 0.6 and 0.7 by Jaimy Azle.
- Port for version 0.8 and modernization of test suite by Mike Bayer.
- Port for sqlalchemy-ibmi by Naveen Ram/Kevin Adler.
