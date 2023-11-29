# +--------------------------------------------------------------------------+
# |  Licensed Materials - Property of IBM                                    |
# |                                                                          |
# | (C) Copyright IBM Corporation 2008, 2016.                                |
# +--------------------------------------------------------------------------+
# | This module complies with SQLAlchemy 0.8 and is                          |
# | Licensed under the Apache License, Version 2.0 (the "License");          |
# | you may not use this file except in compliance with the License.         |
# | You may obtain a copy of the License at                                  |
# | http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable |
# | law or agreed to in writing, software distributed under the License is   |
# | distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY |
# | KIND, either express or implied. See the License for the specific        |
# | language governing permissions and limitations under the License.        |
# +--------------------------------------------------------------------------+
# | Authors: Alex Pitigoi, Abhigyan Agrawal, Rahul Priyadarshi               |
# | Contributors: Jaimy Azle, Mike Bayer                                     |
# +--------------------------------------------------------------------------+
"""
DBAPI Connection
----------------
This dialect uses the `pyodbc <https://github.com/mkleehammer/pyodbc>`_ DBAPI
and the `IBM i Access ODBC Driver
<https://www.ibm.com/support/pages/ibm-i-access-client-solutions>`_.

Connection string::

    engine = create_engine("ibmi://user:password@host/rdbname[?key=value&key=value...]")

Connection Arguments
--------------------

The sqlalchemy-ibmi dialect supports multiple connection arguments that are
passed in the URL to the `create_engine
<https://docs.sqlalchemy.org/en/20/core/engines.html>`_ function.

Connection string keywords:

* ``current_schema`` - Define the default schema to use for unqualified names.
* ``library_list`` - Specify which IBM i libraries to add to the server job's
  library list. Can be specified in the URL as a comma separated list, or as a
  keyword argument to the create_engine function as a list of strings
* ``autocommit`` - If ``False``, Connection.commit must be called;
  otherwise each statement is automatically committed.
  Defaults to ``False``.
* ``readonly`` - If ``True``, the connection is set to read-only. Defaults to ``False``.
* ``timeout`` - The login timeout for the connection, in seconds.
* ``use_system_naming`` - If ``True``, the connection is set to use the System
  naming convention, otherwise it will use the SQL naming convention.
  Defaults to ``False``.
* ``trim_char_fields`` - If ``True``, all character fields will be returned
  with trailing spaces truncated. Defaults to ``False``.

create-engine arguments:

* ``fast_executemany`` - Enables PyODBC's `fast_executemany
  <https://github.com/mkleehammer/pyodbc/wiki/Cursor#executemanysql-params-with-fast_executemanytrue>`_
  option. Conversion between input and target types is mostly unsupported when this
  feature is enabled. eg. Inserting a Decimal object into a Float column will
  produce the error "Converting decimal loses precision". Defaults to ``False``.

Transaction Isolation Level / Autocommit
----------------------------------------
Db2 for i supports 5 isolation levels:

* ``SERIALIZABLE``: ``*RR``
* ``READ COMMITTED``: ``*CS``
* ``READ UNCOMMITTED``: ``*CHG``
* ``REPEATABLE READ``: ``*ALL``
* ``NO COMMIT``: ``*NC``

**At this time, sqlalchemy-ibmi supports all of these isolation levels
except NO COMMIT.**

Autocommit is supported on all available isolation levels.

To set isolation level globally::

    engine = create_engine("ibmi://user:pass@host/", isolation_level='REPEATABLE_READ')

To set using per-connection execution options::

    connection = engine.connect()
    connection = connection.execution_options(
        isolation_level="SERIALIZABLE"
    )

Table Creation String Size
--------------------------
When creating a table with SQLAlchemy, Db2 for i requires that the size of
a String column be provided.

Provide the length for a String column as follows:

.. code-block:: python
   :emphasize-lines: 4, 8

    class User(Base):
        __tablename__ = 'users'
        id = Column(Integer, Sequence('user_id_seq'), primary_key=True)
        name = Column(String(50))

    users = Table('users', metadata,
        Column('id', Integer, Sequence('user_id_seq'), primary_key=True),
        Column('name', String(50)),
    )


Literal Values and Untyped Parameters
-------------------------------------
SQLAlchemy will try to use parameter markers as much as possible, even for values
specified with the `literal
<https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.literal>`_,
`null <https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.null>`_,
`func <https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.func>`_
sql expression functions. Because Db2 for i doesn't support untyped parameter markers,
in places where the type is unknown, a CAST expression must be placed around it to
give it a type. sqlalchemy-ibmi will automatically do this based on the type object
provided to SQLAlchemy.

In some cases, SQLAlchemy allows specifying a Python object directly without a type
object. In this case, SQLAlchemy will deduce the type object based on the Python type:

+-------------------+-----------------+
|    Python type    | SQLAlchemy type |
+===================+=================+
|        bool       |    Boolean      |
+-------------------+-----------------+
|        int        |    Integer      |
+-------------------+-----------------+
|       float       |    Float        |
+-------------------+-----------------+
|        str        |    Unicode      |
+-------------------+-----------------+
|       bytes       |    LargeBinary  |
+-------------------+-----------------+
|  decimal.Decimal  |    Numeric      |
+-------------------+-----------------+
| datetime.datetime |    DateTime     |
+-------------------+-----------------+
|   datetime.date   |    DateTime     |
+-------------------+-----------------+
|   datetime.time   |    DateTime     |
+-------------------+-----------------+

The deduced SQLAlchemy type will be generic however, having no length, precision, or
scale defined. This causes problems when generating these CAST expressions. To support
handling the majority of cases, some types will be adjusted:

+-------------------+-----------------+
|    Python type    | SQLAlchemy type |
+===================+=================+
|        int        |    BigInteger   |
+-------------------+-----------------+
|        str        |  Unicode(32739) |
+-------------------+-----------------+

In addition, Numeric types will be rendered as inline literals. On SQLAlchemy 1.4 and
up, this will be done using `render_literal_execute
<https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.BindParameter.render_literal_execute>`_
to support statement caching.

If the type used is not appropriate (eg. when specifying a >32k string), you must
specify the type (or use a `cast
<https://docs.sqlalchemy.org/en/20/core/sqlelement.html#sqlalchemy.sql.expression.cast>`_)::

    too_big_for_varchar = 'a' * 32768
    connection.execute(
        select(literal(too_big_for_varchar, UnicodeText()))
    ).scalar()


Text search support
-------------------
The ColumnOperators.match function is implemented using a basic LIKE operation by
default. However, when `OmniFind Text Search Server for Db2 for i
<https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_75/rzash/rzashkickoff.htm>`_ is
installed, match will take advantage of the CONTAINS function that it provides.

"""  # noqa E501
import datetime
import re

from collections import defaultdict
from distutils.util import strtobool

from sqlalchemy import (
    select,
    schema as sa_schema,
    exc,
    util,
    Table,
    MetaData,
    Column,
    __version__ as _SA_Version,
)
from sqlalchemy.sql import compiler, operators
from sqlalchemy.sql.expression import and_, cast
from sqlalchemy.engine import default, reflection
from sqlalchemy.types import (
    BLOB,
    CHAR,
    CLOB,
    DATE,
    DATETIME,
    INTEGER,
    SMALLINT,
    BIGINT,
    DECIMAL,
    NUMERIC,
    REAL,
    TIME,
    TIMESTAMP,
    VARCHAR,
    FLOAT,
)
from sqlalchemy import types as sa_types

from .constants import RESERVED_WORDS


def get_sa_version():
    """Returns the SQLAlchemy version as a list of integers."""
    version = [int(ver_token) for ver_token in _SA_Version.split(".")[0:2]]
    return version


SA_Version = get_sa_version()


class IBMBoolean(sa_types.Boolean):
    """Represents a Db2 Boolean Column"""

    def result_processor(self, _, coltype):
        def process(value):
            if value is None:
                return None
            return bool(value)

        return process

    def bind_processor(self, _):
        def process(value):
            if value is None:
                return None
            return "1" if value else "0"

        return process


class IBMDate(sa_types.Date):
    """Represents a Db2 Date Column"""

    def result_processor(self, _, coltype):
        def process(value):
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                value = datetime.date(value.year, value.month, value.day)
            return value

        return process

    def bind_processor(self, _):
        def process(value):
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                value = datetime.date(value.year, value.month, value.day)
            return str(value)

        return process


class DOUBLE(sa_types.Numeric):
    """Represents a Db2 Double Column"""

    __visit_name__ = "DOUBLE"


class GRAPHIC(sa_types.CHAR):
    """Represents a Db2 Graphic Column"""

    __visit_name__ = "GRAPHIC"


class VARGRAPHIC(sa_types.Unicode):
    """Represents a Db2 Vargraphic Column"""

    __visit_name__ = "VARGRAPHIC"


class DBCLOB(sa_types.CLOB):
    """Represents a Db2 Dbclob Column"""

    __visit_name__ = "DBCLOB"


class XML(sa_types.Text):
    """Represents a Db2 XML Column"""

    __visit_name__ = "XML"


COLSPECS = {
    sa_types.Boolean: IBMBoolean,
    sa_types.Date: IBMDate,
}

ISCHEMA_NAMES = {
    "BLOB": BLOB,
    "CHAR": CHAR,
    "CHARACTER": CHAR,
    "CLOB": CLOB,
    "DATE": DATE,
    "DATETIME": DATETIME,
    "INTEGER": INTEGER,
    "SMALLINT": SMALLINT,
    "BIGINT": BIGINT,
    "DECIMAL": DECIMAL,
    "NUMERIC": NUMERIC,
    "REAL": REAL,
    "DOUBLE": DOUBLE,
    "FLOAT": FLOAT,
    "TIME": TIME,
    "TIMESTAMP": TIMESTAMP,
    "VARCHAR": VARCHAR,
    "XML": XML,
    "GRAPHIC": GRAPHIC,
    "VARGRAPHIC": VARGRAPHIC,
    "DBCLOB": DBCLOB,
}


class DB2TypeCompiler(compiler.GenericTypeCompiler):
    """IBM i Db2 Type Compiler"""

    def visit_TIMESTAMP(self, type_, **kw):
        precision = getattr(type_, "precision", None)
        if precision is not None:
            return f"TIMESTAMP({precision})"
        else:
            return "TIMESTAMP"

    def visit_DATETIME(self, type_, **kw):
        return self.visit_TIMESTAMP(type_, **kw)

    def visit_FLOAT(self, type_, **kw):
        return (
            "FLOAT"
            if type_.precision is None
            else "FLOAT(%(precision)s)" % {"precision": type_.precision}
        )

    def visit_BOOLEAN(self, type_, **kw):
        return self.visit_SMALLINT(type_, **kw)

    def _extend(self, type_, name, ccsid=None, length=None):
        text = name

        if not length:
            length = type_.length

        if length:
            text += f"({length})"

        if ccsid:
            text += f" CCSID {ccsid}"

        # TODO: Handle collation instead of CCSID
        # if type_.collation:
        #     text += ' COLLATE "%s"' % type_.collation
        return text

    def visit_CHAR(self, type_, **kw):
        return self._extend(type_, "CHAR", 1208)

    def visit_VARCHAR(self, type_, **kw):
        return self._extend(type_, "VARCHAR", 1208)

    def visit_CLOB(self, type_, **kw):
        return self._extend(type_, "CLOB", ccsid=1208, length=type_.length or "2G")

    def visit_NCHAR(self, type_, **kw):
        return self._extend(type_, "NCHAR")

    def visit_NVARCHAR(self, type_, **kw):
        return self._extend(type_, "NVARCHAR")

    def visit_NCLOB(self, type_, **kw):
        return self._extend(type_, "NCLOB", length=type_.length or "1G")

    def visit_TEXT(self, type_, **kw):
        return self.visit_CLOB(type_, **kw)

    def visit_BLOB(self, type_, **kw):
        length = type_.length or "2G"
        return f"BLOB({length})"

    def visit_numeric(self, type_, **kw):
        # For many databases, NUMERIC and DECIMAL are equivalent aliases, but for Db2
        # NUMERIC is zoned while DECIMAL is packed. Packed format gives better space
        # usage and performance, so we prefer that by default. If a user really wants
        # zoned, they can use types.NUMERIC class instead.
        return self.visit_DECIMAL(type_, **kw)

    # dialect-specific types

    # This is now part of SQLAlchemy as of 2.0. We can drop this function once
    # we drop support for earlier versions.
    def visit_DOUBLE(self, type_):
        return "DOUBLE"

    def visit_GRAPHIC(self, type_):
        return self._extend(type_, "GRAPHIC")

    def visit_VARGRAPHIC(self, type_):
        return self._extend(type_, "VARGRAPHIC")

    def visit_DBCLOB(self, type_, **kw):
        return self._extend(type_, "DBCLOB", length=type_.length or "1G")

    def visit_XML(self, type_):
        return "XML"


class DB2Compiler(compiler.SQLCompiler):
    """IBM i Db2 compiler class"""

    def get_cte_preamble(self, recursive):
        return "WITH"

    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"

    def for_update_clause(self, select, **kw):
        if select.for_update == "read":
            return " WITH RS USE AND KEEP SHARE LOCKS"
        if select.for_update:
            return " WITH RS USE AND KEEP UPDATE LOCKS"
        return ""

    def visit_mod_binary(self, binary, operator, **kw):
        return "mod(%s, %s)" % (self.process(binary.left), self.process(binary.right))

    def visit_match_op_binary(self, binary, operator, **kw):
        if self.dialect.text_server_available:
            return "CONTAINS (%s, %s) > 0" % (
                self.process(binary.left),
                self.process(binary.right),
            )
        binary.right.value = "%" + binary.right.value + "%"
        return "%s LIKE %s" % (self.process(binary.left), self.process(binary.right))

    def limit_clause(self, select, **kw):
        # On Db2 for i, there is a separate OFFSET clause, but there is no separate
        # LIMIT clause. Instead, LIMIT is treated as an alternate or "shortcut" syntax
        # of a FETCH clause.
        # Because of this, these work:  "LIMIT x", "LIMIT x OFFSET y"
        # but these do not:            "OFFSET y", "LIMIT x OFFSET y ROWS"
        #
        # Because of this, if we want to use the LIMIT alternate form, we'd have to
        # special case both LIMIT with OFFSET and OFFSET without LIMIT. However, if we
        # use the traditional FETCH form we need no special cases.
        #
        # OFFSET is supported since IBM i 7.1 TR11 / IBM i 7.2 TR3
        text = ""
        if select._offset_clause is not None:
            text += " OFFSET " + self.process(select._offset_clause, **kw) + " ROWS "
        if select._limit_clause is not None:
            text += (
                " FETCH FIRST "
                + self.process(select._limit_clause, **kw)
                + " ROWS ONLY "
            )
        return text

    def visit_sequence(self, sequence, **kw):
        return "NEXT VALUE FOR %s" % sequence.name

    def default_from(self):
        # Db2 uses SYSIBM.SYSDUMMY1 table for row count
        return " FROM SYSIBM.SYSDUMMY1"

    def visit_function(self, func, result_map=None, **kwargs):
        if func.name.upper() == "AVG":
            return "AVG(DOUBLE(%s))" % (self.function_argspec(func, **kwargs))

        if func.name.upper() == "CHAR_LENGTH":
            return "CHAR_LENGTH(%s, %s)" % (
                self.function_argspec(func, **kwargs),
                "OCTETS",
            )
        return compiler.SQLCompiler.visit_function(self, func, **kwargs)

    # TODO: this is wrong but need to know what Db2 is expecting here
    #    if func.name.upper() == "LENGTH":
    #        return "LENGTH('%s')" % func.compile().params[func.name + '_1']
    #    else:
    #        return compiler.SQLCompiler.visit_function(self, func, **kwargs)

    def visit_cast(self, cast, **kw):
        kw["_cast_applied"] = True
        return super().visit_cast(cast, **kw)

    def visit_savepoint(self, savepoint_stmt):
        return "SAVEPOINT %(sid)s ON ROLLBACK RETAIN CURSORS" % {
            "sid": self.preparer.format_savepoint(savepoint_stmt)
        }

    def visit_rollback_to_savepoint(self, savepoint_stmt):
        return "ROLLBACK TO SAVEPOINT %(sid)s" % {
            "sid": self.preparer.format_savepoint(savepoint_stmt)
        }

    def visit_release_savepoint(self, savepoint_stmt):
        return "RELEASE TO SAVEPOINT %(sid)s" % {
            "sid": self.preparer.format_savepoint(savepoint_stmt)
        }

    def visit_unary(self, unary, **kw):
        usql = super().visit_unary(unary, **kw)

        if unary.operator == operators.exists and kw.get(
            "within_columns_clause", False
        ):
            usql = f"CASE WHEN {usql} THEN 1 ELSE 0 END"
        return usql

    def visit_empty_set_op_expr(self, type_, expand_op):
        if expand_op is operators.not_in_op:
            return "(%s)) OR (1 = 1" % (
                ", ".join(
                    "CAST(NULL AS %s)"
                    % self.dialect.type_compiler.process(
                        INTEGER() if element._isnull else element
                    )
                    for element in type_
                )
            )
        elif expand_op is operators.in_op:
            return "(%s)) OR (1 != 1" % (
                ", ".join(
                    "CAST(NULL AS %s)"
                    % self.dialect.type_compiler.process(
                        INTEGER() if element._isnull else element
                    )
                    for element in type_
                )
            )
        else:
            return self.visit_empty_set_expr(type_)

    def visit_empty_set_expr(self, element_types):
        return "SELECT 1 FROM SYSIBM.SYSDUMMY1 WHERE 1!=1"

    def visit_null(self, expr, **kw):
        if not kw.get("within_columns_clause", False):
            return "NULL"

        # We can't use a NULL constant/literal in a parameter list without a type
        # or we'll get SQL0206 - Column or global variable NULL not found.
        # We can work around this by casting to a type, but at this point we don't
        # know what the type was, and when using the null() function, there will
        # not be a type anyway, so we pick an arbitrary type of INTEGER which is
        # most compatible with other types other than BLOB, XML, and some others.
        #
        # As an optimization, if we detect we're already in a CAST expression, then
        # we don't need to add another.
        if kw.get("_cast_applied", False):
            # We're in a cast expression, so no need to cast
            return "NULL"

        return "CAST(NULL AS INTEGER)"

    def visit_bindparam(
        self,
        bindparam,
        within_columns_clause=False,
        literal_binds=False,
        skip_bind_expression=False,
        literal_execute=False,
        render_postcompile=False,
        **kwargs,
    ):
        if within_columns_clause and not literal_binds:
            # Db2 doesn't support untyped parameter markers so we need to add a CAST
            # clause around them to the appropriate type.
            #
            # Default Python type to SQLAlchemy type mapping:
            # |    Python type    | SQLAlchemy type |
            # |-------------------|-----------------|
            # |        bool       |    Boolean      |
            # |        int        |    Integer      |
            # |       float       |    Float        |
            # |        str        |    Unicode      |
            # |       bytes       |    LargeBinary  |
            # |  decimal.Decimal  |    Numeric      |
            # | datetime.datetime |    DateTime     |
            # |   datetime.date   |    DateTime     |
            # |   datetime.time   |    DateTime     |
            #
            # Most types just need a cast, but some types we handle specially since we
            # don't know how big the value will be and by literals will have its
            # attributes set to default eg. length, precision, and scale all set to
            # None. Since we can't base anything from the bindparam value as all literal
            # values will end up caching to the same statement, we must assume the worst
            # case scenario and try to handle any possible value. We could render
            # everything as literals using bindparam.render_literal_execute(), but that
            # will impact statement caching on the server as well as cause problems with
            # bytes and str literals over 32k.
            #
            # - Integer: Cast to BigInteger
            # - Unicode: If no length was specified, set length to VARCHAR max length.
            #            This will cause issues if users specify a >32k literal, but
            #            this seems unlikely and using UnicodeText by default would
            #            cause extra network flows for each literal. If a user needs
            #            to query a >32k literal, they can specify the type for the
            #            literal themselves.
            # - Decimal: Render as a literal if no precision was specified. There's no
            #            precision and scale values we can use which could cover all
            #            Decimal literals.
            type_ = bindparam.type
            use_cast = True

            if isinstance(type_, sa_types.Numeric) and not isinstance(
                type_, sa_types.Float
            ):
                if not type_.precision:
                    # Render this value as a literal in post-process
                    use_cast = False
                    try:
                        bindparam = bindparam.render_literal_execute()
                    except AttributeError:
                        # SQLAlchemy 1.3 doesn't have render_literal_execute
                        literal_binds = True
            elif isinstance(type_, sa_types.Unicode):
                if not type_.length:
                    type_ = type_.copy()
                    type_.length = 32739
            elif isinstance(type_, sa_types.Integer):
                type_ = sa_types.BigInteger()
            elif isinstance(type_, sa_types.NullType):
                # Can't cast to a NULL, just leave it as-is
                use_cast = False

            if use_cast:
                return self.process(cast(bindparam, type_))

        return super().visit_bindparam(
            bindparam,
            within_columns_clause,
            literal_binds,
            skip_bind_expression,
            literal_execute=literal_execute,
            render_postcompile=render_postcompile,
            **kwargs,
        )


class DB2DDLCompiler(compiler.DDLCompiler):
    """DDL Compiler for IBM i Db2"""

    def get_column_specification(self, column, **kw):
        col_spec = [self.preparer.format_column(column)]

        col_spec.append(
            self.dialect.type_compiler.process(column.type, type_expression=column)
        )

        # column-options: "NOT NULL"
        if not column.nullable or column.primary_key:
            col_spec.append("NOT NULL")

        # default-clause:
        default = self.get_column_default_string(column)
        if default is not None:
            col_spec.append("WITH DEFAULT")
            col_spec.append(default)

        if column is column.table._autoincrement_column:
            col_spec.append("GENERATED BY DEFAULT")
            col_spec.append("AS IDENTITY")
            col_spec.append("(START WITH 1)")

        column_spec = " ".join(col_spec)
        return column_spec

    def define_constraint_cascades(self, constraint):
        text = ""
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete

        if constraint.onupdate is not None:
            util.warn("Db2 does not support UPDATE CASCADE for foreign keys.")

        return text

    def visit_drop_constraint(self, drop, **kw):
        constraint = drop.element
        if isinstance(constraint, sa_schema.ForeignKeyConstraint):
            qual = "FOREIGN KEY "
            const = self.preparer.format_constraint(constraint)
        elif isinstance(constraint, sa_schema.PrimaryKeyConstraint):
            qual = "PRIMARY KEY "
            const = ""
        elif isinstance(constraint, sa_schema.UniqueConstraint):
            qual = "UNIQUE "
            const = self.preparer.format_constraint(constraint)
        else:
            qual = ""
            const = self.preparer.format_constraint(constraint)

        if (
            hasattr(constraint, "uConstraint_as_index")
            and constraint.uConstraint_as_index
        ):
            return "DROP %s%s" % (qual, const)
        return "ALTER TABLE %s DROP %s%s" % (
            self.preparer.format_table(constraint.table),
            qual,
            const,
        )

    def visit_create_index(
        self, create, include_schema=True, include_table_schema=True
    ):
        sql = super().visit_create_index(create, include_schema, include_table_schema)
        if getattr(create.element, "uConstraint_as_index", None):
            sql += " EXCLUDE NULL KEYS"
        return sql


class DB2IdentifierPreparer(compiler.IdentifierPreparer):
    """IBM i Db2 specific identifier preparer"""

    reserved_words = RESERVED_WORDS
    illegal_initial_characters = {str(x) for x in range(0, 10)}.union(["_", "$"])


class DB2ExecutionContext(default.DefaultExecutionContext):
    """IBM i Db2 Execution Context class"""

    _select_lastrowid = False
    _lastrowid = None

    def get_lastrowid(self):
        return self._lastrowid

    def pre_exec(self):
        if self.isinsert:
            tbl = self.compiled.statement.table
            seq_column = tbl._autoincrement_column
            insert_has_sequence = seq_column is not None

            self._select_lastrowid = (
                insert_has_sequence
                and not self.compiled.returning
                and not self.compiled.inline
            )

    def post_exec(self):
        conn = self.root_connection
        if self._select_lastrowid:
            conn._cursor_execute(self.cursor, "VALUES IDENTITY_VAL_LOCAL()", (), self)
            row = self.cursor.fetchall()[0]
            if row[0] is not None:
                self._lastrowid = int(row[0])

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
            "VALUES NEXTVAL FOR "
            + self.connection.dialect.identifier_preparer.format_sequence(seq),
            type_,
        )


def to_bool(obj):
    if isinstance(obj, bool):
        return obj
    return strtobool(obj)


class IBMiDb2Dialect(default.DefaultDialect):
    driver = "pyodbc"
    name = "ibmi"
    max_identifier_length = 128
    encoding = "utf-8"
    default_paramstyle = "qmark"
    colspecs = COLSPECS
    ischema_names = ISCHEMA_NAMES
    postfetch_lastrowid = True
    supports_native_boolean = False
    supports_alter = True
    supports_sequences = True
    sequences_optional = True
    supports_sane_multi_rowcount = False
    supports_sane_rowcount_returning = True
    supports_native_decimal = True
    requires_name_normalize = True
    supports_default_values = False
    supports_empty_insert = False
    supports_statement_cache = True

    statement_compiler = DB2Compiler
    ddl_compiler = DB2DDLCompiler
    type_compiler = DB2TypeCompiler
    preparer = DB2IdentifierPreparer
    execution_ctx_cls = DB2ExecutionContext

    def __init__(self, isolation_level=None, fast_executemany=False, **kw):
        super().__init__(**kw)
        self.isolation_level = isolation_level
        self.fast_executemany = fast_executemany

    def on_connect(self):
        if self.isolation_level is not None:

            def connect(conn):
                self.set_isolation_level(conn, self.isolation_level)

            return connect
        else:
            return None

    def initialize(self, connection):
        super().initialize(connection)
        self.driver_version = self._get_driver_version(connection.connection)
        self.text_server_available = self._check_text_server(connection)

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysconst = self.sys_table_constraints
        syschkconst = self.sys_check_constraints

        query = select(
            [syschkconst.c.conname, syschkconst.c.chkclause],
            and_(
                syschkconst.c.conschema == sysconst.c.conschema,
                syschkconst.c.conname == sysconst.c.conname,
                sysconst.c.tabschema == current_schema,
                sysconst.c.tabname == table_name,
            ),
        )

        check_consts = []
        print(query)
        for res in connection.execute(query):
            check_consts.append(
                {"name": self.normalize_name(res[0]), "sqltext": res[1]}
            )

        return check_consts

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        if current_schema:
            whereclause = and_(
                self.sys_tables.c.tabschema == current_schema,
                self.sys_tables.c.tabname == table_name,
            )
        else:
            whereclause = self.sys_tables.c.tabname == table_name
        select_statement = select([self.sys_tables.c.tabcomment], whereclause)
        results = connection.execute(select_statement)
        return {"text": results.scalar()}

    @property
    def _isolation_lookup(self):
        return {
            # IBM i terminology
            "*CHG": self.dbapi.SQL_TXN_READ_UNCOMMITTED,
            "*CS": self.dbapi.SQL_TXN_READ_COMMITTED,
            "*ALL": self.dbapi.SQL_TXN_REPEATABLE_READ,
            "*RR": self.dbapi.SQL_TXN_SERIALIZABLE,
            # ODBC terminology
            "SERIALIZABLE": self.dbapi.SQL_TXN_SERIALIZABLE,
            "READ UNCOMMITTED": self.dbapi.SQL_TXN_READ_UNCOMMITTED,
            "READ COMMITTED": self.dbapi.SQL_TXN_READ_COMMITTED,
            "REPEATABLE READ": self.dbapi.SQL_TXN_REPEATABLE_READ,
        }

    # Methods merged from PyODBCConnector

    def get_isolation_level(self, dbapi_conn):
        return self.isolation_level

    def set_isolation_level(self, connection, level):
        self.isolation_level = level
        level = level.replace("_", " ")
        if level in self._isolation_lookup:
            connection.set_attr(
                self.dbapi.SQL_ATTR_TXN_ISOLATION, self._isolation_lookup[level]
            )
        else:
            raise exc.ArgumentError(
                "Invalid value '%s' for isolation_level. "
                "Valid isolation levels for %s are %s"
                % (level, self.name, ", ".join(self._isolation_lookup.keys()))
            )

    @classmethod
    def dbapi(cls):
        return __import__("pyodbc")

    DRIVER_KEYWORD_MAP = {
        # SQLAlchemy kwd: (ODBC keyword, type, default)
        #
        # NOTE: We use the upper-case driver connection string value to work
        # around a bug in the the 07.01.025 driver which causes it to do
        # case-sensitive lookups. This should be fixed in the 07.01.026 driver
        # and older versions are not affected, but we don't have to check
        # anything since they are case-insensitive and allow the all-uppercase
        # values just fine.
        "system": ("SYSTEM", str, None),
        "user": ("UID", str, None),
        "password": ("PWD", str, None),
        "database": ("DATABASE", str, None),
        "use_system_naming": ("NAM", to_bool, False),
        "trim_char_fields": ("TRIMCHAR", to_bool, None),
        "lob_threshold_kb": ("MAXFIELDLEN", int, None),
    }

    DRIVER_KEYWORDS_SPECIAL = {
        "current_schema",
        "library_list",
    }

    @classmethod
    def map_connect_opts(cls, opts):
        # Map our keywords to what ODBC expects
        for keyword, keyword_info in cls.DRIVER_KEYWORD_MAP.items():
            odbc_keyword, to_keyword, default_value = keyword_info

            value = opts.pop(keyword, default_value)
            if value is None:
                continue

            try:
                value = to_keyword(value)

                # pyodbc will stringify the bool to "True" or "False" instead
                # of "1" and "0" as the driver wants, so manually convert to
                # integer first.
                if isinstance(value, bool):
                    value = int(value)

                opts[odbc_keyword] = value
            except ValueError:
                raise ValueError("Invalid value specified for {}".format(keyword))

        # For current_schema and library_list we can't use the above loop, since these
        # must be combined in to one ODBC keyword
        if "current_schema" in opts or "library_list" in opts:
            current_schema = opts.pop("current_schema", "")
            library_list = opts.pop("library_list", "")

            if not isinstance(library_list, str):
                library_list = ",".join(library_list)

            opts["DefaultLibraries"] = f"{current_schema},{library_list}"

    def create_connect_args(self, url):
        opts = url.translate_connect_args(username="user", host="system")
        opts.update(url.query)

        # Allow both our specific keywords and the SQLAlchemy base keywords
        allowed_opts = (
            set(self.DRIVER_KEYWORD_MAP.keys())
            | self.DRIVER_KEYWORDS_SPECIAL
            | {"autocommit", "readonly", "timeout"}
        )

        if not allowed_opts.issuperset(opts.keys()):
            raise ValueError("Option entered not valid for IBM i Access ODBC Driver")

        self.map_connect_opts(opts)
        return [
            [
                "Driver=IBM i Access ODBC Driver"
                ";UNICODESQL=1"
                ";TRUEAUTOCOMMIT=1"
                ";XDYNAMIC=0"
            ],
            opts,
        ]

    def is_disconnect(self, e, connection, cursor):
        if isinstance(e, self.dbapi.ProgrammingError):
            return "The cursor's connection has been closed." in str(
                e
            ) or "Attempt to use a closed connection." in str(e)
        else:
            return False

    def _dbapi_version(self):
        if not self.dbapi:
            return ()
        return self._parse_dbapi_version(self.dbapi.version)

    def _parse_dbapi_version(self, vers):
        m = re.match(r"(?:py.*-)?([\d\.]+)(?:-(\w+))?", vers)
        if not m:
            return ()
        vers = tuple([int(x) for x in m.group(1).split(".")])
        if m.group(2):
            vers += (m.group(2),)
        return vers

    def _get_server_version_info(self, connection, allow_chars=True):
        dbapi_con = connection.connection
        version = [
            int(_) for _ in dbapi_con.getinfo(self.dbapi.SQL_DBMS_VER).split(".")
        ]
        return tuple(version[0:2])

    def _get_default_schema_name(self, connection):
        return self.normalize_name(connection.execute("VALUES CURRENT_SCHEMA").scalar())

    # Driver version for IBM i Access ODBC Driver is given as
    # VV.RR.SSSF where VV (major), RR (release), and SSS (service pack)
    # will be returned and F (test fix version) will be ignored
    def _get_driver_version(self, db_conn):
        version = db_conn.getinfo(self.dbapi.SQL_DRIVER_VER).split(".")
        sssf = version.pop(2)
        sss = sssf[:3]
        version.append(sss)
        return [int(_) for _ in version]

    ischema = MetaData()

    sys_schemas = Table(
        "SYSSCHEMAS",
        ischema,
        Column("SCHEMA_NAME", sa_types.Unicode, key="schemaname"),
        schema="QSYS2",
    )

    sys_tables = Table(
        "SYSTABLES",
        ischema,
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("TABLE_TYPE", sa_types.Unicode, key="tabtype"),
        Column("SYSTEM_TABLE", sa_types.Unicode, key="tabsys"),
        Column("LONG_COMMENT", sa_types.Unicode, key="tabcomment"),
        schema="QSYS2",
    )

    sys_table_constraints = Table(
        "SYSCST",
        ischema,
        Column("CONSTRAINT_SCHEMA", sa_types.Unicode, key="conschema"),
        Column("CONSTRAINT_NAME", sa_types.Unicode, key="conname"),
        Column("CONSTRAINT_TYPE", sa_types.Unicode, key="contype"),
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("TABLE_TYPE", sa_types.Unicode, key="tabtype"),
        schema="QSYS2",
    )

    sys_constraints_columns = Table(
        "SYSCSTCOL",
        ischema,
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("COLUMN_NAME", sa_types.Unicode, key="colname"),
        Column("CONSTRAINT_SCHEMA", sa_types.Unicode, key="conschema"),
        Column("CONSTRAINT_NAME", sa_types.Unicode, key="conname"),
        schema="QSYS2",
    )

    sys_key_constraints = Table(
        "SYSKEYCST",
        ischema,
        Column("CONSTRAINT_SCHEMA", sa_types.Unicode, key="conschema"),
        Column("CONSTRAINT_NAME", sa_types.Unicode, key="conname"),
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("COLUMN_NAME", sa_types.Unicode, key="colname"),
        Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
        schema="QSYS2",
    )

    sys_check_constraints = Table(
        "SYSCHKCST",
        ischema,
        Column("CONSTRAINT_SCHEMA", sa_types.Unicode, key="conschema"),
        Column("CONSTRAINT_NAME", sa_types.Unicode, key="conname"),
        Column("CHECK_CLAUSE", sa_types.Unicode, key="chkclause"),
        Column("ROUNDING_MODE", sa_types.Unicode, key="rndmode"),
        Column("SYSTEM_CONSTRAINT_SCHEMA", sa_types.Unicode, key="syscstchema"),
        Column("INSERT_ACTION", sa_types.Unicode, key="insact"),
        Column("UPDATE_ACTION", sa_types.Unicode, key="updact"),
        schema="QSYS2",
    )

    sys_columns = Table(
        "SYSCOLUMNS",
        ischema,
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("COLUMN_NAME", sa_types.Unicode, key="colname"),
        Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
        Column("DATA_TYPE", sa_types.Unicode, key="typename"),
        Column("LENGTH", sa_types.Integer, key="length"),
        Column("NUMERIC_SCALE", sa_types.Integer, key="scale"),
        Column("IS_NULLABLE", sa_types.Unicode, key="nullable"),
        Column("COLUMN_DEFAULT", sa_types.Unicode, key="defaultval"),
        Column("HAS_DEFAULT", sa_types.Unicode, key="hasdef"),
        Column("IS_IDENTITY", sa_types.Unicode, key="isid"),
        Column("IDENTITY_GENERATION", sa_types.Unicode, key="idgenerate"),
        schema="QSYS2",
    )

    sys_indexes = Table(
        "SYSINDEXES",
        ischema,
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("INDEX_SCHEMA", sa_types.Unicode, key="indschema"),
        Column("INDEX_NAME", sa_types.Unicode, key="indname"),
        Column("IS_UNIQUE", sa_types.Unicode, key="uniquerule"),
        schema="QSYS2",
    )

    sys_keys = Table(
        "SYSKEYS",
        ischema,
        Column("INDEX_SCHEMA", sa_types.Unicode, key="indschema"),
        Column("INDEX_NAME", sa_types.Unicode, key="indname"),
        Column("COLUMN_NAME", sa_types.Unicode, key="colname"),
        Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
        Column("ORDERING", sa_types.Unicode, key="ordering"),
        schema="QSYS2",
    )

    sys_foreignkeys = Table(
        "SQLFOREIGNKEYS",
        ischema,
        Column("FK_NAME", sa_types.Unicode, key="fkname"),
        Column("FKTABLE_SCHEM", sa_types.Unicode, key="fktabschema"),
        Column("FKTABLE_NAME", sa_types.Unicode, key="fktabname"),
        Column("FKCOLUMN_NAME", sa_types.Unicode, key="fkcolname"),
        Column("PK_NAME", sa_types.Unicode, key="pkname"),
        Column("PKTABLE_SCHEM", sa_types.Unicode, key="pktabschema"),
        Column("PKTABLE_NAME", sa_types.Unicode, key="pktabname"),
        Column("PKCOLUMN_NAME", sa_types.Unicode, key="pkcolname"),
        Column("KEY_SEQ", sa_types.Integer, key="colno"),
        schema="SYSIBM",
    )

    sys_views = Table(
        "SYSVIEWS",
        ischema,
        Column("TABLE_SCHEMA", sa_types.Unicode, key="viewschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="viewname"),
        Column("VIEW_DEFINITION", sa_types.Unicode, key="text"),
        schema="QSYS2",
    )

    sys_sequences = Table(
        "SYSSEQUENCES",
        ischema,
        Column("SEQUENCE_SCHEMA", sa_types.Unicode, key="seqschema"),
        Column("SEQUENCE_NAME", sa_types.Unicode, key="seqname"),
        schema="QSYS2",
    )

    def has_table(self, connection, table_name, schema=None):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        if current_schema:
            whereclause = and_(
                self.sys_tables.c.tabschema == current_schema,
                self.sys_tables.c.tabname == table_name,
            )
        else:
            whereclause = self.sys_tables.c.tabname == table_name
        select_statement = select([self.sys_tables], whereclause)
        results = connection.execute(select_statement)
        return results.first() is not None

    def has_sequence(self, connection, sequence_name, schema=None):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        sequence_name = self.denormalize_name(sequence_name)
        if current_schema:
            whereclause = and_(
                self.sys_sequences.c.seqschema == current_schema,
                self.sys_sequences.c.seqname == sequence_name,
            )
        else:
            whereclause = self.sys_sequences.c.seqname == sequence_name
        select_statement = select([self.sys_sequences.c.seqname], whereclause)
        results = connection.execute(select_statement)
        return results.first() is not None

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        sysschema = self.sys_schemas
        query = (
            select([sysschema.c.schemaname])
            .where(sysschema.c.schemaname.notlike("SYS%"))
            .where(sysschema.c.schemaname.notlike("Q%"))
            .order_by(sysschema.c.schemaname)
        )
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    # Retrieves a list of table names for a given schema
    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)

        query = (
            select([self.sys_tables.c.tabname])
            .where(self.sys_tables.c.tabschema == current_schema)
            .where(self.sys_tables.c.tabtype.in_(["T", "P"]))
            .where(self.sys_tables.c.tabsys == "N")
            .order_by(self.sys_tables.c.tabname)
        )

        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    def get_view_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)

        query = (
            select([self.sys_tables.c.tabname])
            .where(self.sys_tables.c.tabschema == current_schema)
            .where(self.sys_tables.c.tabtype.in_(["V"]))
            .where(self.sys_tables.c.tabsys == "N")
            .order_by(self.sys_tables.c.tabname)
        )

        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        viewname = self.denormalize_name(viewname)

        query = select([self.sys_views.c.text]).where(
            and_(
                self.sys_views.c.viewschema == current_schema,
                self.sys_views.c.viewname == viewname,
            )
        )

        return connection.execute(query).scalar()

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        syscols = self.sys_columns

        query = select(
            [
                syscols.c.colname,
                syscols.c.typename,
                syscols.c.defaultval,
                syscols.c.nullable,
                syscols.c.length,
                syscols.c.scale,
                syscols.c.isid,
                syscols.c.idgenerate,
            ],
            and_(
                syscols.c.tabschema == current_schema, syscols.c.tabname == table_name
            ),
            order_by=[syscols.c.colno],
        )
        sa_columns = []
        for row in connection.execute(query):
            coltype = row[1].upper()
            if coltype in ["DECIMAL", "NUMERIC"]:
                coltype = self.ischema_names.get(coltype)(
                    precision=int(row[4]), scale=int(row[5])
                )
            elif coltype in ["CHARACTER", "CHAR", "VARCHAR", "GRAPHIC", "VARGRAPHIC"]:
                coltype = self.ischema_names.get(coltype)(length=int(row[4]))
            else:
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn(
                        "Did not recognize type '%s' of column '%s'" % (coltype, row[0])
                    )
                    coltype = sa_types.NULLTYPE

            sa_columns.append(
                {
                    "name": self.normalize_name(row[0]),
                    "type": coltype,
                    "nullable": row[3] == "Y",
                    "default": row[2],
                    "autoincrement": (row[6] == "YES") and (row[7] is not None),
                }
            )
        return sa_columns

    @reflection.cache
    def get_pk_constraint(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysconst = self.sys_table_constraints
        syskeyconst = self.sys_key_constraints

        query = (
            select([syskeyconst.c.colname, sysconst.c.conname])
            .where(
                and_(
                    syskeyconst.c.conschema == sysconst.c.conschema,
                    syskeyconst.c.conname == sysconst.c.conname,
                    sysconst.c.tabschema == current_schema,
                    sysconst.c.tabname == table_name,
                    sysconst.c.contype == "PRIMARY KEY",
                )
            )
            .order_by(syskeyconst.c.colno)
        )

        pk_columns = []
        pk_name = None
        for key in connection.execute(query):
            pk_columns.append(self.normalize_name(key[0]))
            if not pk_name:
                pk_name = self.normalize_name(key[1])
        return {"constrained_columns": pk_columns, "name": pk_name}

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysconst = self.sys_table_constraints
        syskeyconst = self.sys_key_constraints

        query = (
            select([syskeyconst.c.colname, sysconst.c.tabname])
            .where(
                and_(
                    syskeyconst.c.conschema == sysconst.c.conschema,
                    syskeyconst.c.conname == sysconst.c.conname,
                    sysconst.c.tabschema == current_schema,
                    sysconst.c.tabname == table_name,
                    sysconst.c.contype == "PRIMARY KEY",
                )
            )
            .order_by(syskeyconst.c.colno)
        )

        return [self.normalize_name(key[0]) for key in connection.execute(query)]

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        default_schema = self.default_schema_name
        current_schema = self.denormalize_name(schema or default_schema)
        default_schema = self.normalize_name(default_schema)
        table_name = self.denormalize_name(table_name)
        sysfkeys = self.sys_foreignkeys
        query = select(
            [
                sysfkeys.c.fkname,
                sysfkeys.c.fktabschema,
                sysfkeys.c.fktabname,
                sysfkeys.c.fkcolname,
                sysfkeys.c.pkname,
                sysfkeys.c.pktabschema,
                sysfkeys.c.pktabname,
                sysfkeys.c.pkcolname,
            ],
            and_(
                sysfkeys.c.fktabschema == current_schema,
                sysfkeys.c.fktabname == table_name,
            ),
            order_by=[sysfkeys.c.colno],
        )
        fschema = {}
        for row in connection.execute(query):
            if row[0] not in fschema:
                referred_schema = self.normalize_name(row[5])

                # if no schema specified and referred schema here is the
                # default, then set to None
                if schema is None and referred_schema == default_schema:
                    referred_schema = None

                fschema[row[0]] = {
                    "name": self.normalize_name(row[0]),
                    "constrained_columns": [self.normalize_name(row[3])],
                    "referred_schema": referred_schema,
                    "referred_table": self.normalize_name(row[6]),
                    "referred_columns": [self.normalize_name(row[7])],
                }
            else:
                fschema[row[0]]["constrained_columns"].append(
                    self.normalize_name(row[3])
                )
                fschema[row[0]]["referred_columns"].append(self.normalize_name(row[7]))
        return [value for key, value in fschema.items()]

    # Retrieves a list of index names for a given schema
    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysidx = self.sys_indexes
        syskey = self.sys_keys

        query = select(
            [sysidx.c.indname, sysidx.c.uniquerule, syskey.c.colname],
            and_(
                syskey.c.indschema == sysidx.c.indschema,
                syskey.c.indname == sysidx.c.indname,
                sysidx.c.tabschema == current_schema,
                sysidx.c.tabname == table_name,
            ),
            order_by=[syskey.c.indname, syskey.c.colno],
        )
        indexes = {}
        for row in connection.execute(query):
            key = row[0].upper()
            if key in indexes:
                indexes[key]["column_names"].append(self.normalize_name(row[2]))
            else:
                indexes[key] = {
                    "name": self.normalize_name(row[0]),
                    "column_names": [self.normalize_name(row[2])],
                    "unique": row[1] == "Y",
                }
        return [value for key, value in indexes.items()]

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysconst = self.sys_table_constraints
        sysconstcol = self.sys_constraints_columns

        query = (
            select([sysconst.c.conname, sysconstcol.c.colname])
            .where(
                and_(
                    sysconstcol.c.conschema == sysconst.c.conschema,
                    sysconstcol.c.conname == sysconst.c.conname,
                    sysconst.c.tabschema == current_schema,
                    sysconst.c.tabname == table_name,
                    sysconst.c.contype == "UNIQUE",
                )
            )
            .order_by(
                sysconst.c.conname,
                sysconstcol.c.colname,
            )
        )

        constraints = defaultdict(list)
        for name, column_name in connection.execute(query):
            constraints[name].append(self.normalize_name(column_name))

        return [
            {
                "name": self.normalize_name(name),
                "column_names": value,
            }
            for name, value in constraints.items()
        ]

    @reflection.cache
    def get_sequence_names(self, connection, schema, **kw):
        current_schema = self.denormalize_name(schema or self.default_schema_name)
        query = select([self.sys_sequences.c.seqname]).where(
            self.sys_sequences.c.seqschema == current_schema,
        )
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    def _check_text_server(self, connection):
        stmt = "SELECT COUNT(*) FROM QSYS2.SYSTEXTSERVERS"
        return connection.execute(stmt).scalar()

    def do_executemany(self, cursor, statement, parameters, context=None):
        cursor.fast_executemany = self.fast_executemany
        cursor.executemany(statement, parameters)
