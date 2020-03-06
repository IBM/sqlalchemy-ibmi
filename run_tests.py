from sqlalchemy.dialects import registry

registry.register("ibmi", "sqlalchemy_ibmi.base", "IBMiDb2Dialect")
