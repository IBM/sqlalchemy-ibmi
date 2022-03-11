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
This dialect uses the `pyODBC <https://github.com/mkleehammer/pyodbc>`_ DBAPI
and the `IBM i Access ODBC Driver
<https://www.ibm.com/support/pages/ibm-i-access-client-solutions>`_.

Connection string::

    engine = create_engine("ibmi://user:password@host/rdbname[?key=value&key=value...]")

Connection Arguments
-----------------

The sqlalchemy-ibmi dialect supports multiple connection arguments that are
passed in the URL to the `create_engine <https://docs.sqlalchemy.org/en/13/core/engines.html>`_ function.

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

* ``fast_executemany`` - Enables PyODBC's `fast_executemany <https://github.com/mkleehammer/pyodbc/wiki/Cursor#executemanysql-params-with-fast_executemanytrue>`_ option.
Conversion between input and target types is mostly unsupported when this
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


Query Function Strings
----------------------
Db2 for i doesn't support parameter markers in the SELECT clause of a statement.
As a result, the following command will not work with sqlalchemy-ibmi::

    session.query(func.count("*")).select_from(User).scalar()

Instead, please use the following::

    session.query(func.count()).select_from(User).scalar()

Addtionally, column names cannot be passed as strings, so convert your column
strings to literal columns as follows::

    from sqlalchemy.sql import literal_column
    session.query(func.count(literal_column("colname"))).select_from(User).scalar()

Text search support
-------------------
The ColumnOperators.match function is implemented using a basic LIKE operation
by default. However, when `OmniFind Text Search Server for Db2 for i <https://www.ibm.com/support/knowledgecenter/ssw_ibm_i_74/rzash/rzashkickoff.htm>`_ is
installed, match will take advantage of the CONTAINS function that it provides.

"""  # noqa E501 
import datetime
import re
from distutils.util import strtobool
from sqlalchemy import schema as sa_schema, exc
from sqlalchemy.sql import compiler
from sqlalchemy.sql import operators
from sqlalchemy.engine import default
from sqlalchemy.types import BLOB, CHAR, CLOB, DATE, DATETIME, INTEGER, \
    SMALLINT, BIGINT, DECIMAL, NUMERIC, REAL, TIME, TIMESTAMP, \
    VARCHAR, FLOAT
from .constants import RESERVED_WORDS
from sqlalchemy import sql, util
from sqlalchemy import Table, MetaData, Column
from sqlalchemy.engine import reflection
from sqlalchemy import types as sa_types

# as documented from:
# http://publib.boulder.ibm.com/infocenter/db2luw/v9/index.jsp?topic=/com.ibm.db2.udb.doc/admin/r0001095.htm


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
            return '1' if value else '0'
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
    __visit_name__ = 'DOUBLE'


class LONGVARCHAR(sa_types.VARCHAR):
    """Represents a Db2 Longvarchar Column"""
    __visit_name_ = 'LONGVARCHAR'


class DBCLOB(sa_types.CLOB):
    """Represents a Db2 Dbclob Column"""
    __visit_name__ = "DBCLOB"


class GRAPHIC(sa_types.CHAR):
    """Represents a Db2 Graphic Column"""
    __visit_name__ = "GRAPHIC"


class VARGRAPHIC(sa_types.Unicode):
    """Represents a Db2 Vargraphic Column"""
    __visit_name__ = "VARGRAPHIC"


class LONGVARGRAPHIC(sa_types.UnicodeText):
    """Represents a Db2 longvargraphic Column"""
    __visit_name__ = "LONGVARGRAPHIC"


class XML(sa_types.Text):
    """Represents a Db2 XML Column"""
    __visit_name__ = "XML"


COLSPECS = {
    sa_types.Boolean: IBMBoolean,
    sa_types.Date: IBMDate
}

ISCHEMA_NAMES = {
    'BLOB': BLOB,
    'CHAR': CHAR,
    'CHARACTER': CHAR,
    'CLOB': CLOB,
    'DATE': DATE,
    'DATETIME': DATETIME,
    'INTEGER': INTEGER,
    'SMALLINT': SMALLINT,
    'BIGINT': BIGINT,
    'DECIMAL': DECIMAL,
    'NUMERIC': NUMERIC,
    'REAL': REAL,
    'DOUBLE': DOUBLE,
    'FLOAT': FLOAT,
    'TIME': TIME,
    'TIMESTAMP': TIMESTAMP,
    'VARCHAR': VARCHAR,
    'LONGVARCHAR': LONGVARCHAR,
    'XML': XML,
    'GRAPHIC': GRAPHIC,
    'VARGRAPHIC': VARGRAPHIC,
    'LONGVARGRAPHIC': LONGVARGRAPHIC,
    'DBCLOB': DBCLOB
}


class DB2TypeCompiler(compiler.GenericTypeCompiler):
    """IBM i Db2 Type Compiler"""

    def visit_TIMESTAMP(self, type_, **kw):
        return "TIMESTAMP"

    def visit_DATE(self, type_, **kw):
        return "DATE"

    def visit_TIME(self, type_, **kw):
        return "TIME"

    def visit_DATETIME(self, type_, **kw):
        return self.visit_TIMESTAMP(type_)

    def visit_SMALLINT(self, type_, **kw):
        return "SMALLINT"

    def visit_INT(self, type_):
        return "INT"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_FLOAT(self, type_, **kw):
        return "FLOAT" if type_.precision is None else \
            "FLOAT(%(precision)s)" % {'precision': type_.precision}

    def visit_DOUBLE(self, type_):
        return "DOUBLE"

    def visit_XML(self, type_):
        return "XML"

    def visit_CLOB(self, type_, **kw):
        return "CLOB"

    def visit_BLOB(self, type_, **kw):
        return "BLOB(1M)" if type_.length in (None, 0) else \
            "BLOB(%(length)s)" % {'length': type_.length}

    def visit_DBCLOB(self, type_, **kw):
        return "DBCLOB(1M)" if type_.length in (None, 0) else \
            "DBCLOB(%(length)s)" % {'length': type_.length}

    def visit_VARCHAR(self, type_, **kw):
        return "VARCHAR(%(length)s) CCSID 1208" % {'length': type_.length}

    def visit_LONGVARCHAR(self, type_):
        return "LONG VARCHAR CCSID 1208"

    def visit_VARGRAPHIC(self, type_):
        return "VARGRAPHIC(%(length)s)" % {'length': type_.length}

    def visit_LONGVARGRAPHIC(self, type_):
        return "LONG VARGRAPHIC"

    def visit_CHAR(self, type_, **kw):
        return "CHAR" if type_.length in (None, 0) else \
            "CHAR(%(length)s)" % {'length': type_.length}

    def visit_GRAPHIC(self, type_):
        return "GRAPHIC" if type_.length in (None, 0) else \
            "GRAPHIC(%(length)s)" % {'length': type_.length}

    def visit_DECIMAL(self, type_, **kw):
        if not type_.precision:
            return "DECIMAL(31, 0)"
        if not type_.scale:
            return "DECIMAL(%(precision)s, 0)" % {'precision': type_.precision}

        return "DECIMAL(%(precision)s, %(scale)s)" % {
            'precision': type_.precision, 'scale': type_.scale}

    def visit_numeric(self, type_, **kw):
        return self.visit_DECIMAL(type_)

    def visit_datetime(self, type_, **kw):
        return self.visit_TIMESTAMP(type_)

    def visit_date(self, type_, **kw):
        return self.visit_DATE(type_)

    def visit_time(self, type_, **kw):
        return self.visit_TIME(type_)

    def visit_integer(self, type_, **kw):
        return self.visit_INT(type_)

    def visit_boolean(self, type_, **kw):
        return self.visit_SMALLINT(type_)

    def visit_float(self, type_, **kw):
        return self.visit_FLOAT(type_)

    def visit_unicode(self, type_):
        return self.visit_VARCHAR(type_)

    def visit_unicode_text(self, type_):
        return self.visit_LONGVARCHAR(type_)

    def visit_string(self, type_, **kw):
        return self.visit_VARCHAR(type_)

    def visit_TEXT(self, type_, **kw):
        return self.visit_CLOB(type_)

    def visit_large_binary(self, type_, **kw):
        return self.visit_BLOB(type_)


class DB2Compiler(compiler.SQLCompiler):
    """IBM i Db2 compiler class"""
    def get_cte_preamble(self, recursive):
        return "WITH"

    def visit_now_func(self, fn, **kw):
        return "CURRENT_TIMESTAMP"

    def for_update_clause(self, select, **kw):
        if select.for_update == 'read':
            return ' WITH RS USE AND KEEP SHARE LOCKS'
        if select.for_update:
            return ' WITH RS USE AND KEEP UPDATE LOCKS'
        return ''

    def visit_mod_binary(self, binary, operator, **kw):
        return "mod(%s, %s)" % (self.process(binary.left),
                                self.process(binary.right))

    def visit_match_op_binary(self, binary, operator, **kw):
        if self.dialect.text_server_available:
            return "CONTAINS (%s, %s) > 0" % (
                self.process(binary.left),
                self.process(binary.right),
            )
        binary.right.value = '%'+binary.right.value+'%'
        return "%s LIKE %s" % (
            self.process(binary.left),
            self.process(binary.right))

    def limit_clause(self, select, **kwargs):
        if (select._limit is not None) and (select._offset is None):
            return " FETCH FIRST %s ROWS ONLY" % select._limit
        return ""

    def visit_select(self, select, **kwargs):
        limit, offset = select._limit, select._offset
        sql_ori = compiler.SQLCompiler.visit_select(self, select, **kwargs)
        if offset is not None:
            __rownum = 'Z.__ROWNUM'
            sql_split = re.split(r"[\s+]FROM ", sql_ori, 1)
            sql_sec = ""
            sql_sec = " \nFROM %s " % (sql_split[1])

            dummyVal = "Z.__db2_"
            sql_pri = ""

            sql_sel = "SELECT "
            if select._distinct:
                sql_sel = "SELECT DISTINCT "

            sql_select_token = sql_split[0].split(",")
            i = 0
            while i < len(sql_select_token):
                if sql_select_token[i].count(
                        "TIMESTAMP(DATE(SUBSTR(CHAR(") == 1:
                    sql_sel = "%s \"%s%d\"," % (sql_sel, dummyVal, i + 1)
                    sql_pri = '%s %s,%s,%s,%s AS "%s%d",' % (
                        sql_pri,
                        sql_select_token[i],
                        sql_select_token[i + 1],
                        sql_select_token[i + 2],
                        sql_select_token[i + 3],
                        dummyVal, i + 1)
                    i = i + 4
                    continue

                if sql_select_token[i].count(" AS ") == 1:
                    temp_col_alias = sql_select_token[i].split(" AS ")
                    sql_pri = '%s %s,' % (sql_pri, sql_select_token[i])
                    sql_sel = "%s %s," % (sql_sel, temp_col_alias[1])
                    i = i + 1
                    continue

                sql_pri = '%s %s AS "%s%d",' % (
                    sql_pri, sql_select_token[i], dummyVal, i + 1)
                sql_sel = "%s \"%s%d\"," % (sql_sel, dummyVal, i + 1)
                i = i + 1

            sql_pri = sql_pri[:len(sql_pri) - 1]
            sql_pri = "%s%s" % (sql_pri, sql_sec)
            sql_sel = sql_sel[:len(sql_sel) - 1]
            sql = '%s, ( ROW_NUMBER() OVER() ) AS "%s" FROM ( %s ) AS M' % (
                sql_sel, __rownum, sql_pri)
            sql = '%s FROM ( %s ) Z WHERE' % (sql_sel, sql)

            if offset != 0:
                sql = '%s "%s" > %d' % (sql, __rownum, offset)
            if offset != 0 and limit is not None:
                sql = '%s AND ' % sql
            if limit is not None:
                sql = '%s "%s" <= %d' % (sql, __rownum, offset + limit)
            return "( %s )" % (sql,)
        return sql_ori

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
                self.function_argspec(func, **kwargs), 'OCTETS')
        return compiler.SQLCompiler.visit_function(self, func, **kwargs)

    # TODO: this is wrong but need to know what Db2 is expecting here
    #    if func.name.upper() == "LENGTH":
    #        return "LENGTH('%s')" % func.compile().params[func.name + '_1']
    #    else:
    #        return compiler.SQLCompiler.visit_function(self, func, **kwargs)

    def visit_cast(self, cast, **kw):
        type_ = cast.typeclause.type

        # TODO: verify that CAST shouldn't be called with
        # other types, I was able to CAST against VARCHAR
        # for example
        if isinstance(type_, (
                sa_types.DateTime, sa_types.Date, sa_types.Time,
                sa_types.DECIMAL, sa_types.String, sa_types.FLOAT,
                sa_types.NUMERIC, sa_types.INT)):
            return super(DB2Compiler, self).visit_cast(cast, **kw)

        return self.process(cast.clause)

    def get_select_precolumns(self, select, **kwargs):
        if isinstance(select._distinct, str):
            return select._distinct.upper() + " "
        if select._distinct:
            return "DISTINCT "
        return ""

    def visit_join(self, join, asfrom=False, **kwargs):
        # NOTE: this is the same method as that used in mysql/base.py
        # to render INNER JOIN
        return ''.join(
            (self.process(join.left, asfrom=True, **kwargs),
             (join.isouter and " LEFT OUTER JOIN " or " INNER JOIN "),
             self.process(join.right, asfrom=True, **kwargs),
             " ON ",
             self.process(join.onclause, **kwargs)))

    def visit_savepoint(self, savepoint_stmt):
        return "SAVEPOINT %(sid)s ON ROLLBACK RETAIN CURSORS" % {
            'sid': self.preparer.format_savepoint(savepoint_stmt)}

    def visit_rollback_to_savepoint(self, savepoint_stmt):
        return 'ROLLBACK TO SAVEPOINT %(sid)s' % {
            'sid': self.preparer.format_savepoint(savepoint_stmt)}

    def visit_release_savepoint(self, savepoint_stmt):
        return 'RELEASE TO SAVEPOINT %(sid)s' % {
            'sid': self.preparer.format_savepoint(savepoint_stmt)}

    def visit_unary(self, unary, **kw):
        if (unary.operator == operators.exists) and kw.get(
                'within_columns_clause', False):
            usql = super(DB2Compiler, self).visit_unary(unary, **kw)
            usql = "CASE WHEN " + usql + " THEN 1 ELSE 0 END"
            return usql
        else:
            return super(DB2Compiler, self).visit_unary(unary, **kw)


class DB2DDLCompiler(compiler.DDLCompiler):
    """DDL Compiler for IBM i Db2"""

    def get_column_specification(self, column, **kw):
        col_spec = [self.preparer.format_column(column)]

        col_spec.append(
            self.dialect.type_compiler.process(
                column.type,
                type_expression=column))

        # column-options: "NOT NULL"
        if not column.nullable or column.primary_key:
            col_spec.append('NOT NULL')

        # default-clause:
        default = self.get_column_default_string(column)
        if default is not None:
            col_spec.append('WITH DEFAULT')
            col_spec.append(default)

        if column is column.table._autoincrement_column:
            col_spec.append('GENERATED BY DEFAULT')
            col_spec.append('AS IDENTITY')
            col_spec.append('(START WITH 1)')

        column_spec = ' '.join(col_spec)
        return column_spec

    def define_constraint_cascades(self, constraint):
        text = ""
        if constraint.ondelete is not None:
            text += " ON DELETE %s" % constraint.ondelete

        if constraint.onupdate is not None:
            util.warn(
                "Db2 does not support UPDATE CASCADE for foreign keys.")

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

        if hasattr(
                constraint,
                'uConstraint_as_index') and constraint.uConstraint_as_index:
            return "DROP %s%s" % \
                   (qual, const)
        return "ALTER TABLE %s DROP %s%s" % \
               (self.preparer.format_table(constraint.table),
                qual, const)

    def visit_create_index(self, create, include_schema=True,
                           include_table_schema=True):
        sql = super(DB2DDLCompiler, self).visit_create_index(
            create,
            include_schema,
            include_table_schema)
        if getattr(create.element, 'uConstraint_as_index', None):
            sql += ' EXCLUDE NULL KEYS'
        return sql


class DB2IdentifierPreparer(compiler.IdentifierPreparer):
    """IBM i Db2 specific identifier preparer"""
    reserved_words = RESERVED_WORDS
    illegal_initial_characters = set(range(0, 10)).union(["_", "$"])


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

            self._select_lastrowid = \
                insert_has_sequence and \
                not self.compiled.returning and \
                not self.compiled.inline

    def post_exec(self):
        conn = self.root_connection
        if self._select_lastrowid:
            conn._cursor_execute(
                self.cursor,
                "VALUES IDENTITY_VAL_LOCAL()",
                (),
                self)
            row = self.cursor.fetchall()[0]
            if row[0] is not None:
                self._lastrowid = int(row[0])

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
            "VALUES NEXTVAL FOR " +
            self.connection.dialect.identifier_preparer.format_sequence(seq),
            type_)


def to_bool(obj):
    if isinstance(obj, bool):
        return obj
    return strtobool(obj)


class IBMiDb2Dialect(default.DefaultDialect):
    driver = "pyodbc"
    name = 'sqlalchemy_ibmi'
    max_identifier_length = 128
    encoding = 'utf-8'
    default_paramstyle = 'qmark'
    colspecs = COLSPECS
    ischema_names = ISCHEMA_NAMES
    supports_unicode_binds = True
    returns_unicode_strings = False
    postfetch_lastrowid = True
    supports_native_boolean = False
    preexecute_sequences = False
    supports_alter = True
    supports_sequences = True
    sequences_optional = True
    supports_unicode_statements = True
    supports_sane_rowcount = False
    supports_sane_multi_rowcount = False
    # TODO Investigate if supports_native_decimal needs to be True or False
    supports_native_decimal = True
    supports_char_length = True
    pyodbc_driver_name = "IBM i Access ODBC Driver"
    requires_name_normalize = True
    supports_default_values = False
    supports_empty_insert = False
    two_phase_transactions = False
    savepoints = True
    supports_sane_rowcount_returning = False

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
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysconst = self.sys_table_constraints
        syschkconst = self.sys_check_constraints

        query = sql.select([syschkconst.c.conname, syschkconst.c.chkclause],
                           sql.and_(
                               syschkconst.c.conschema == sysconst.c.conschema,
                               syschkconst.c.conname == sysconst.c.conname,
                               sysconst.c.tabschema == current_schema,
                               sysconst.c.tabname == table_name))

        check_consts = []
        print(query)
        for res in connection.execute(query):
            check_consts.append(
                {'name': self.normalize_name(res[0]), 'sqltext': res[1]})

        return check_consts

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        if current_schema:
            whereclause = sql.and_(
                self.sys_tables.c.tabschema == current_schema,
                self.sys_tables.c.tabname == table_name)
        else:
            whereclause = self.sys_tables.c.tabname == table_name
        select_statement = \
            sql.select([self.sys_tables.c.tabcomment], whereclause)
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
            connection.set_attr(self.dbapi.SQL_ATTR_TXN_ISOLATION,
                                self._isolation_lookup[level])
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
        'system': ('SYSTEM', str, None),
        'user': ('UID', str, None),
        'password': ('PWD', str, None),
        'database': ('DATABASE', str, None),
        'use_system_naming': ('NAM', to_bool, False),
        'trim_char_fields': ('TRIMCHAR', to_bool, None),
    }

    DRIVER_KEYWORDS_SPECIAL = {'current_schema', 'library_list'}

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
        if 'current_schema' in opts or 'library_list' in opts:
            current_schema = opts.pop('current_schema', '')
            library_list = opts.pop('library_list', '')

            if not isinstance(library_list, str):
                library_list = ','.join(library_list)

            opts['DefaultLibraries'] = f"{current_schema},{library_list}"


    def create_connect_args(self, url):
        opts = url.translate_connect_args(username='user', host='system')
        opts.update(url.query)

        # Allow both our specific keywords and the SQLAlchemy base keywords
        allowed_opts = set(self.DRIVER_KEYWORD_MAP.keys()) | \
                       self.DRIVER_KEYWORDS_SPECIAL | \
                       {'autocommit', 'readonly', 'timeout'}

        if not allowed_opts.issuperset(opts.keys()):
            raise ValueError("Option entered not valid for "
                             "IBM i Access ODBC Driver")

        self.map_connect_opts(opts)

        return [["Driver={%s}; UNICODESQL=1; TRUEAUTOCOMMIT=1; XDYNAMIC=0;" % (
                 self.pyodbc_driver_name)], opts]

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
        version = [int(_) for _ in
                   dbapi_con.getinfo(self.dbapi.SQL_DBMS_VER).split('.')]
        return tuple(version[0:2])

    def _get_default_schema_name(self, connection):
        """Return: current setting of the schema attribute"""
        default_schema_name = connection.execute(
            u'VALUES CURRENT_SCHEMA').scalar()
        if isinstance(default_schema_name, str):
            default_schema_name = default_schema_name.strip()
        return self.normalize_name(default_schema_name)

    # Driver version for IBM i Access ODBC Driver is given as
    # VV.RR.SSSF where VV (major), RR (release), and SSS (service pack)
    # will be returned and F (test fix version) will be ignored
    def _get_driver_version(self, db_conn):
        version = db_conn.getinfo(self.dbapi.SQL_DRIVER_VER).split('.')
        sssf = version.pop(2)
        sss = sssf[:3]
        version.append(sss)
        return [int(_) for _ in version]

    ischema = MetaData()

    sys_schemas = Table(
        "SQLSCHEMAS", ischema,
        Column("TABLE_SCHEM", sa_types.Unicode, key="schemaname"),
        schema="SYSIBM")

    sys_tables = Table(
        "SYSTABLES", ischema,
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("TABLE_TYPE", sa_types.Unicode, key="tabtype"),
        Column("LONG_COMMENT", sa_types.Unicode, key="tabcomment"),
        schema="QSYS2")

    sys_table_constraints = Table(
        "SYSCST", ischema,
        Column("CONSTRAINT_SCHEMA", sa_types.Unicode, key="conschema"),
        Column("CONSTRAINT_NAME", sa_types.Unicode, key="conname"),
        Column("CONSTRAINT_TYPE", sa_types.Unicode, key="contype"),
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("TABLE_TYPE", sa_types.Unicode, key="tabtype"),
        schema="QSYS2")

    sys_key_constraints = Table(
        "SYSKEYCST", ischema,
        Column("CONSTRAINT_SCHEMA", sa_types.Unicode, key="conschema"),
        Column("CONSTRAINT_NAME", sa_types.Unicode, key="conname"),
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("COLUMN_NAME", sa_types.Unicode, key="colname"),
        Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
        schema="QSYS2")

    sys_check_constraints = Table(
        "SYSCHKCST", ischema,
        Column("CONSTRAINT_SCHEMA", sa_types.Unicode, key="conschema"),
        Column("CONSTRAINT_NAME", sa_types.Unicode, key="conname"),
        Column("CHECK_CLAUSE", sa_types.Unicode, key="chkclause"),
        Column("ROUNDING_MODE", sa_types.Unicode, key="rndmode"),
        Column("SYSTEM_CONSTRAINT_SCHEMA", sa_types.Unicode, key="syscstchema"),
        Column("INSERT_ACTION", sa_types.Unicode, key="insact"),
        Column("UPDATE_ACTION", sa_types.Unicode, key="updact"),
        schema="QSYS2")

    sys_columns = Table(
        "SYSCOLUMNS", ischema,
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("COLUMN_NAME", sa_types.Unicode, key="colname"),
        Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
        Column("DATA_TYPE", sa_types.Unicode, key="typename"),
        Column("LENGTH", sa_types.Integer, key="length"),
        Column("NUMERIC_SCALE", sa_types.Integer, key="scale"),
        Column("IS_NULLABLE", sa_types.Integer, key="nullable"),
        Column("COLUMN_DEFAULT", sa_types.Unicode, key="defaultval"),
        Column("HAS_DEFAULT", sa_types.Unicode, key="hasdef"),
        Column("IS_IDENTITY", sa_types.Unicode, key="isid"),
        Column("IDENTITY_GENERATION", sa_types.Unicode, key="idgenerate"),
        schema="QSYS2")

    sys_indexes = Table(
        "SYSINDEXES", ischema,
        Column("TABLE_SCHEMA", sa_types.Unicode, key="tabschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="tabname"),
        Column("INDEX_SCHEMA", sa_types.Unicode, key="indschema"),
        Column("INDEX_NAME", sa_types.Unicode, key="indname"),
        Column("IS_UNIQUE", sa_types.Unicode, key="uniquerule"),
        schema="QSYS2")

    sys_keys = Table(
        "SYSKEYS", ischema,
        Column("INDEX_SCHEMA", sa_types.Unicode, key="indschema"),
        Column("INDEX_NAME", sa_types.Unicode, key="indname"),
        Column("COLUMN_NAME", sa_types.Unicode, key="colname"),
        Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
        Column("ORDERING", sa_types.Unicode, key="ordering"),
        schema="QSYS2")

    sys_foreignkeys = Table(
        "SQLFOREIGNKEYS", ischema,
        Column("FK_NAME", sa_types.Unicode, key="fkname"),
        Column("FKTABLE_SCHEM", sa_types.Unicode, key="fktabschema"),
        Column("FKTABLE_NAME", sa_types.Unicode, key="fktabname"),
        Column("FKCOLUMN_NAME", sa_types.Unicode, key="fkcolname"),
        Column("PK_NAME", sa_types.Unicode, key="pkname"),
        Column("PKTABLE_SCHEM", sa_types.Unicode, key="pktabschema"),
        Column("PKTABLE_NAME", sa_types.Unicode, key="pktabname"),
        Column("PKCOLUMN_NAME", sa_types.Unicode, key="pkcolname"),
        Column("KEY_SEQ", sa_types.Integer, key="colno"),
        schema="SYSIBM")

    sys_views = Table(
        "SYSVIEWS", ischema,
        Column("TABLE_SCHEMA", sa_types.Unicode, key="viewschema"),
        Column("TABLE_NAME", sa_types.Unicode, key="viewname"),
        Column("VIEW_DEFINITION", sa_types.Unicode, key="text"),
        schema="QSYS2")

    sys_sequences = Table(
        "SYSSEQUENCES", ischema,
        Column("SEQUENCE_SCHEMA", sa_types.Unicode, key="seqschema"),
        Column("SEQUENCE_NAME", sa_types.Unicode, key="seqname"),
        schema="QSYS2")

    def has_table(self, connection, table_name, schema=None):
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        if current_schema:
            whereclause = sql.and_(
                self.sys_tables.c.tabschema == current_schema,
                self.sys_tables.c.tabname == table_name)
        else:
            whereclause = self.sys_tables.c.tabname == table_name
        select_statement = sql.select([self.sys_tables], whereclause)
        results = connection.execute(select_statement)
        return results.first() is not None

    def has_sequence(self, connection, sequence_name, schema=None):
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        sequence_name = self.denormalize_name(sequence_name)
        if current_schema:
            whereclause = sql.and_(
                self.sys_sequences.c.seqschema == current_schema,
                self.sys_sequences.c.seqname == sequence_name)
        else:
            whereclause = self.sys_sequences.c.seqname == sequence_name
        select_statement = sql.select(
            [self.sys_sequences.c.seqname], whereclause)
        results = connection.execute(select_statement)
        return results.first() is not None

    @reflection.cache
    def get_schema_names(self, connection, **kw):
        sysschema = self.sys_schemas
        query = sql.select([sysschema.c.schemaname],
                           sql.not_(sysschema.c.schemaname.like('SYS%')),
                           sql.not_(sysschema.c.schemaname.like('Q%')),
                           order_by=[sysschema.c.schemaname]
                           )
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    # Retrieves a list of table names for a given schema
    @reflection.cache
    def get_table_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        systbl = self.sys_tables
        query = sql.select([systbl.c.tabname]).\
            where(systbl.c.tabtype == 'T').\
            where(systbl.c.tabschema == current_schema).\
            order_by(systbl.c.tabname)
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_names(self, connection, schema=None, **kw):
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)

        query = sql.select([self.sys_views.c.viewname],
                           self.sys_views.c.viewschema == current_schema,
                           order_by=[self.sys_views.c.viewname]
                           )
        return [self.normalize_name(r[0]) for r in connection.execute(query)]

    @reflection.cache
    def get_view_definition(self, connection, viewname, schema=None, **kw):
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        viewname = self.denormalize_name(viewname)

        query = sql.select([self.sys_views.c.text],
                           self.sys_views.c.viewschema == current_schema,
                           self.sys_views.c.viewname == viewname
                           )
        return connection.execute(query).scalar()

    @reflection.cache
    def get_columns(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        syscols = self.sys_columns

        query = sql.select(
            [syscols.c.colname, syscols.c.typename, syscols.c.defaultval,
             syscols.c.nullable, syscols.c.length, syscols.c.scale,
             syscols.c.isid, syscols.c.idgenerate],
            sql.and_(syscols.c.tabschema == current_schema,
                     syscols.c.tabname == table_name),
            order_by=[syscols.c.colno])
        sa_columns = []
        for row in connection.execute(query):
            coltype = row[1].upper()
            if coltype in ['DECIMAL', 'NUMERIC']:
                coltype = self.ischema_names.get(
                    coltype)(int(row[4]), int(row[5]))
            elif coltype in ['CHARACTER', 'CHAR', 'VARCHAR', 'GRAPHIC',
                             'VARGRAPHIC']:
                coltype = self.ischema_names.get(coltype)(int(row[4]))
            else:
                try:
                    coltype = self.ischema_names[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                              (coltype, row[0]))
                    coltype = sa_types.NULLTYPE

            sa_columns.append({
                'name': self.normalize_name(row[0]),
                'type': coltype,
                'nullable': row[3] == 'Y',
                'default': row[2],
                'autoincrement': (row[6] == 'YES') and (row[7] is not None),
            })
        return sa_columns

    @reflection.cache
    def get_primary_keys(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysconst = self.sys_table_constraints
        syskeyconst = self.sys_key_constraints

        query = sql.select([syskeyconst.c.colname, sysconst.c.tabname],
                           sql.and_(
                               syskeyconst.c.conschema == sysconst.c.conschema,
                               syskeyconst.c.conname == sysconst.c.conname,
                               sysconst.c.tabschema == current_schema,
                               sysconst.c.tabname == table_name,
                               sysconst.c.contype == 'PRIMARY KEY'),
                           order_by=[syskeyconst.c.colno])

        return [self.normalize_name(key[0])
                for key in connection.execute(query)]

    @reflection.cache
    def get_foreign_keys(self, connection, table_name, schema=None, **kw):
        default_schema = self.default_schema_name
        current_schema = self.denormalize_name(schema or default_schema)
        default_schema = self.normalize_name(default_schema)
        table_name = self.denormalize_name(table_name)
        sysfkeys = self.sys_foreignkeys
        query = sql.select(
            [sysfkeys.c.fkname, sysfkeys.c.fktabschema, sysfkeys.c.fktabname,
             sysfkeys.c.fkcolname, sysfkeys.c.pkname, sysfkeys.c.pktabschema,
             sysfkeys.c.pktabname, sysfkeys.c.pkcolname],
            sql.and_(sysfkeys.c.fktabschema == current_schema,
                     sysfkeys.c.fktabname == table_name),
            order_by=[sysfkeys.c.colno])
        fschema = {}
        for row in connection.execute(query):
            if row[0] not in fschema:
                referred_schema = self.normalize_name(row[5])

                # if no schema specified and referred schema here is the
                # default, then set to None
                if schema is None and \
                        referred_schema == default_schema:
                    referred_schema = None

                fschema[row[0]] = {
                    'name': self.normalize_name(row[0]),
                    'constrained_columns': [self.normalize_name(row[3])],
                    'referred_schema': referred_schema,
                    'referred_table': self.normalize_name(row[6]),
                    'referred_columns': [self.normalize_name(row[7])]
                }
            else:
                fschema[row[0]]['constrained_columns'].append(
                    self.normalize_name(row[3]))
                fschema[row[0]]['referred_columns'].append(
                    self.normalize_name(row[7]))
        return [value for key, value in fschema.items()]

    # Retrieves a list of index names for a given schema
    @reflection.cache
    def get_indexes(self, connection, table_name, schema=None, **kw):
        current_schema = self.denormalize_name(
            schema or self.default_schema_name)
        table_name = self.denormalize_name(table_name)
        sysidx = self.sys_indexes
        syskey = self.sys_keys

        query = sql.select([sysidx.c.indname,
                            sysidx.c.uniquerule,
                            syskey.c.colname],
                           sql.and_(
                               syskey.c.indschema == sysidx.c.indschema,
                               syskey.c.indname == sysidx.c.indname,
                               sysidx.c.tabschema == current_schema,
                               sysidx.c.tabname == table_name),
                           order_by=[syskey.c.indname, syskey.c.colno])
        indexes = {}
        for row in connection.execute(query):
            key = row[0].upper()
            if key in indexes:
                indexes[key]['column_names'].append(
                    self.normalize_name(row[2]))
            else:
                indexes[key] = {
                    'name': self.normalize_name(row[0]),
                    'column_names': [self.normalize_name(row[2])],
                    'unique': row[1] == 'Y'
                }
        return [value for key, value in indexes.items()]

    @reflection.cache
    def get_unique_constraints(self, connection, table_name, schema=None, **kw):
        unique_consts = []
        return unique_consts

    def _check_text_server(self, connection):
        stmt = "SELECT COUNT(*) FROM QSYS2.SYSTEXTSERVERS"
        return connection.execute(stmt).scalar()

    def do_executemany(self, cursor, statement, parameters, context=None):
        cursor.fast_executemany = self.fast_executemany
        cursor.executemany(statement, parameters)
