from sqlalchemy.dialects import registry

registry.register("ibmi", "pyodbc_sa.backend", "DB2Dialect_pyodbc")

from sqlalchemy.testing import runner

runner.main()

