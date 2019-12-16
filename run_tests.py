from sqlalchemy.dialects import registry

registry.register("ibmi", "sqlalchemy_ibmi.backend", "AS400Dialect_pyodbc")