SQLAlchemy-IBMi
=========

The SQLAlchemy-IBMi adapter provides the Python/SQLAlchemy interface to IBM i db2.

Version
--------
0.1 (2019/11/26)

Prerequisites
--------------
1. Install Python 2.7 or newer versions except python 3.3
2. SQLAlchemy 1.3 or above.
3. PyODBC
```
   Install pyodbc driver with below commands:
	    Linux and Windows: 
	   	   pip install pyodbc
```

Install and Configuration
=========================

Connecting
----------
A TCP/IP connection can be specified as the following::
```
	from sqlalchemy import create_engine

	e = create_engine("ibmi://user:pass@dsn")
```

For a local socket connection, exclude the "host" and "port" portions::

```
	from sqlalchemy import create_engine

	e = create_engine("ibmi://user:pass@/database")
```

Supported Databases
-------------------
- IBM DB2 Universal Database for Linux/Unix/Windows versions 9.7 onwards 
- IBM Db2 on Cloud

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
ibm_db_sa for SQLAlchemy was first produced by IBM Inc., targeting version 0.4.
The library was ported for version 0.6 and 0.7 by Jaimy Azle.
Port for version 0.8 and modernization of test suite by Mike Bayer.
Port for sqlalchemy-ibmi by Naveen Ram/Kevin Adler

Contributing to IBM_DB_SA python project
----------------------------------------
See `CONTRIBUTING
<https://github.com/ibmdb/python-ibmdbsa/tree/master/ibm_db_sa/contributing/CONTRIBUTING.md>`_.

```
The developer sign-off should include the reference to the DCO in remarks(example below):
DCO 1.1 Signed-off-by: Random J Developer <random@developer.org>
```

