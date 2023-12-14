# SQLAlchemy adapter for IBM i

[![Latest version released on PyPi](https://img.shields.io/pypi/v/sqlalchemy-ibmi.svg)](https://pypi.org/project/sqlalchemy-ibmi)
![Supported Python Version Badge](https://img.shields.io/pypi/pyversions/sqlalchemy-ibmi.svg)
[![Documentation Status](https://readthedocs.org/projects/sqlalchemy-ibmi/badge/?version=latest)](https://sqlalchemy-ibmi.readthedocs.io/en/latest/?badge=latest)

The IBM i SQLAlchemy adapter provides an [SQLAlchemy](https://www.sqlalchemy.org/)
interface to Db2 for [IBM i](https://en.wikipedia.org/wiki/IBM_i).

**Please note that this project is still under active development. Please
 report any bugs in the issue tracker** :rotating_light:

## Requirements

### SQLAlchemy

| SQLAlchemy Version | Supported | Notes                                           |
|--------------------|-----------|-------------------------------------------------|
| SQLAlchemy 1.3     |    ✅     | Most tested.                                    |
| SQLAlchemy 1.4     |    ✅     | Preliminary support added in 0.9.3.             |
| SQLAlchemy 2.0+    |    ❌     | Currently not supported, but planned for 0.9.4. |

### Python

Python 3.6 - 3.11 are supported. Support for Python 3.12 and
up is [currently broken](https://github.com/IBM/sqlalchemy-ibmi/issues/149).

NOTE: sqlalchemy-ibmi 0.9.3 is the last version to support Python 3.6.

### IBM i Access ODBC Driver

It is best to use the latest version of the driver, which is currently available in the
IBM i Access Client Solutions Application Package 1.1.0.27.

Some options may require certain minimum driver versions to be enabled. Because the
driver ignores any unknown options, using them on older driver versions will not cause
an error but instead be silently ignored.

|  Connection Option | Required Version  |
|--------------------|-------------------|
| `trim_char_fields` |    1.1.0.25       |

### IBM i

This adapter is only tested against IBM i 7.3 and up. It may support older IBM i
releases, but no support is guaranteed.

## Installation

```sh
pip install sqlalchemy-ibmi
```

## Getting Started

You will need to have the [IBM i Access ODBC Driver](https://www.ibm.com/support/pages/ibm-i-access-client-solutions)
installed in order to use this adapter. Please read
[these docs](https://ibmi-oss-docs.readthedocs.io/en/latest/odbc/installation.html)
for the simplest way to install for your platform.

```python
import sqlalchemy as sa
engine = sa.create_engine("ibmi://user:password@host.example.com")

cnxn = engine.connect()
metadata = sa.MetaData()
table = sa.Table('table_name', metadata, autoload=True, autoload_with=engine)

query = sa.select([table])

result = cnxn.execute(query)
result = result.fetchall()

# print first entry
print(result[0])
```

For more details on connection options, check
[our docs](https://sqlalchemy-ibmi.readthedocs.io/en/latest#connection-arguments)

If you're new to SQLAlchemy, please refer to the
[SQLAlchemy Unified Tutorial](https://docs.sqlalchemy.org/en/14/tutorial/index.html).

## Documentation

The documentation for the SQLAlchemy adapter for IBM i can be found at:
<https://sqlalchemy-ibmi.readthedocs.io/en/latest/>

## Known Limitations

1) Non-standard SQL queries are not supported. e.g. "SELECT ? FROM TAB1"
2) For updations involving primary/foreign key references, the entries should be made in correct order. Integrity check is always on and thus the primary keys referenced by the foreign keys in the referencing tables should always exist in the parent table.
3) Unique key which contains nullable column not supported
4) UPDATE CASCADE for foreign keys not supported
5) DEFERRABLE INITIALLY deferred not supported
6) Subquery in ON clause of LEFT OUTER JOIN not supported

## Contributing to the IBM i SQLAlchemy adapter

Please read the [contribution guidelines](contributing/CONTRIBUTING.md).

```text
The developer sign-off should include the reference to the DCO in remarks(example below):
DCO 1.1 Signed-off-by: Random J Developer <random@developer.org>
```

## Releasing a New Version

```sh
# checkout and pull the latest code from main
git checkout main
git pull

# bump to a release version (a tag and commit are made)
bumpversion release

# To skip a release candidate
bumpversion --no-tag --no-commit release
bumpversion --allow-dirty release

# bump to the new dev version (a commit is made)
bumpversion --no-tag patch

# push the new tag and commits
git push origin main --tags
```

## License

[Apache 2.0](LICENSE)

## Credits

- ibm_db_sa for SQLAlchemy was first produced by IBM Inc., targeting version 0.4.
- The library was ported for version 0.6 and 0.7 by Jaimy Azle.
- Port for version 0.8 and modernization of test suite by Mike Bayer.
- Port for sqlalchemy-ibmi by Naveen Ram/Kevin Adler.
