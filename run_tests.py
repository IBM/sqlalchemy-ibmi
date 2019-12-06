from sqlalchemy.dialects import registry

registry.register("ibmi", "sqlalchemy_ibmi.backend", "DB2Dialect_pyodbc")

from sqlalchemy.testing import runner

runner.main()

