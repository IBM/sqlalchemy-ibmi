from sqlalchemy.dialects import registry
import pytest

registry.register("ibmi", "sqlalchemy_ibmi.base", "IBMiDb2Dialect")

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *
