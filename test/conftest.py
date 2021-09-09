# Follow standard SQLAlchemy testing setup
# See https://github.com/sqlalchemy/sqlalchemy/blob/b5927dd9229d9ce85fc2ba25dad10ecbb749195c/README.dialects.rst

from sqlalchemy.dialects import registry
import pytest

registry.register("ibmi", "sqlalchemy_ibmi.base", "IBMiDb2Dialect")

pytest.register_assert_rewrite("sqlalchemy.testing.assertions")

from sqlalchemy.testing.plugin.pytestplugin import *  # noqa F401,F403
