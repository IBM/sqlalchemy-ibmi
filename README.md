[![Build Status](https://travis-ci.com/IBM/sqlalchemy-ibmi.svg?branch=master)](https://travis-ci.com/IBM/sqlalchemy-ibmi)
[![Latest version released on PyPi](https://img.shields.io/pypi/v/sqlalchemy-ibmi.svg)]()
[![](https://img.shields.io/pypi/pyversions/sqlalchemy-ibmi.svg)]()
[![Documentation Status](https://readthedocs.org/projects/sqlalchemy-ibmi/badge/?version=latest)]()

SQLAlchemy-IBMi
=========

The SQLAlchemy-IBMi adapter provides the Python/[SQLAlchemy](https://www.sqlalchemy.org/) interface to Db2 for [IBM i](https://en.wikipedia.org/wiki/IBM_i).

```python
from sqlalchemy import create_engine, db


e = create_engine("db2+ibm_db://user:pass@host[:port]/database")
cnxn = e.connect()
metadata = db.Metadata()
table = db.Table('table_name', metadata, autoload=True, autoload_with=e)
query = db.select([table])
result = cnxn.execute(query)
result = result.fetchall()

print(result[0])

```

For more, check out the [samples](samples). (TODO)

Installation
-------------
TODO
 
Feature Support
----------------
TODO

Version
--------

TODO

Documentation
-------------

TODO


Known Limitations in ibm_db_sa adapter for DB2 databases
-------------------------------------------------------------
1) Non-standard SQL queries are not supported. e.g. "SELECT ? FROM TAB1"
2) For updations involving primary/foreign key references, the entries should be made in correct order. Integrity check is always on and thus the primary keys referenced by the foreign keys in the referencing tables should always exist in the parent table.
3) Unique key which contains nullable column not supported
4) UPDATE CASCADE for foreign keys not supported
5) DEFERRABLE INITIALLY deferred not supported
6) Subquery in ON clause of LEFT OUTER JOIN not supported


Credits
-------
- ibm_db_sa for SQLAlchemy was first produced by IBM Inc., targeting version 0.4.
- The library was ported for version 0.6 and 0.7 by Jaimy Azle.
- Port for version 0.8 and modernization of test suite by Mike Bayer.
- Port for sqlalchemy-ibmi by Naveen Ram/Kevin Adler.

Contributing to sqlalchemy-ibmi python project
----------------------------------------
Please read the [contribution guidelines](contributing/CONTRIBUTING.md).

```
The developer sign-off should include the reference to the DCO in remarks(example below):
DCO 1.1 Signed-off-by: Random J Developer <random@developer.org>
```

Releasing a New Version
-----------------------

Run the following commands

```bash
# checkout and pull the latest code from master
git checkout master
git pull

# bump to a release version (a tag and commit are made)
bumpversion release

# remove any old distributions
rm dist/*

# build the new distribution
python setup.py sdist

# bump to the new dev version (a commit is made)
bumpversion --no-tag patch

# push the new tag and commits
git push origin master --tags

# upload the distribution to PyPI
twine upload dist/*
```

License
-------

TODO
