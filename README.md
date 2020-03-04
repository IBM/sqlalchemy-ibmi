[![GitHub Actions status | sdras/awesome-actions](https://github.com/IBM/sqlalchemy-ibmi/workflows/Python%20package/badge.svg)](https://github.com/IBM/sqlalchemy-ibmi/actions?workflow=Python+package)

SQLAlchemy adapter for IBM i
=========

The IBM i SQLAlchemy adapter provides a [SQLAlchemy](https://www.sqlalchemy.org/) interface to Db2 for [IBM i](https://en.wikipedia.org/wiki/IBM_i).

**Please note that this project is still in active development and is not ready for use.** :rotating_light: 

```python
import sqlalchemy as sa

engine = sa.create_engine("ibmi://user:pass@host[:port]/database")
cnxn = engine.connect()
metadata = sa.MetaData()
table = sa.Table('table_name', metadata, autoload=True, autoload_with=engine)

query = sa.select([table])

result = cnxn.execute(query)
result = result.fetchall()

# print first entry
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