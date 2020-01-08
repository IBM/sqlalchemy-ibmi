from sqlalchemy.dialects import registry
import pytest

registry.register("ibmi", "sqlalchemy_ibmi.backend", "AS400Dialect_pyodbc")

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *