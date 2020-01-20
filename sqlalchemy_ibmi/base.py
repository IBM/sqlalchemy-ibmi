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
"""Support for IBM DB2 database

"""
import datetime
import re
from sqlalchemy import types as sa_types
from sqlalchemy import schema as sa_schema
from sqlalchemy import util, sql, Table, MetaData, Column
from sqlalchemy.sql import compiler, operators
from sqlalchemy.engine import default, reflection
from sqlalchemy import __version__ as SA_VERSION

from sqlalchemy.types import BLOB, CHAR, CLOB, DATE, DATETIME, INTEGER, \
    SMALLINT, BIGINT, DECIMAL, NUMERIC, REAL, TIME, TIMESTAMP, \
    VARCHAR, FLOAT

SA_VERSION = [int(ver_token) for ver_token in SA_VERSION.split('.')[0:2]]


class CoerceUnicode(sa_types.TypeDecorator):
    """ Coerce type Unicode """

    def process_literal_param(self, value, dialect):
        return value

    def process_bind_param(self, value, dialect):
        return value

    def process_result_value(self, value, dialect):
        return value

    @property
    def python_type(self):
        pass

    impl = sa_types.Unicode


# as documented from:
# http://publib.boulder.ibm.com/infocenter/db2luw/v9/index.jsp?topic=/com.ibm.db2.udb.doc/admin/r0001095.htm
RESERVED_WORDS = {'activate', 'disallow', 'locale', 'result', 'add', 'disconnect', 'localtime',
                  'result_set_locator', 'after', 'distinct', 'localtimestamp', 'return', 'alias',
                  'do', 'locator', 'returns', 'all', 'double', 'locators', 'revoke', 'allocate',
                  'drop', 'lock', 'right', 'allow', 'dssize', 'lockmax', 'rollback', 'alter',
                  'dynamic', 'locksize', 'routine', 'and', 'each', 'long', 'row', 'any',
                  'editproc', 'loop', 'row_number', 'as', 'else', 'maintained', 'rownumber',
                  'asensitive', 'elseif', 'materialized', 'rows', 'associate', 'enable',
                  'maxvalue', 'rowset', 'asutime', 'encoding', 'microsecond', 'rrn', 'at',
                  'encryption', 'microseconds', 'run', 'attributes', 'end', 'minute',
                  'savepoint', 'audit', 'end-exec', 'minutes', 'schema', 'authorization',
                  'ending', 'minvalue', 'scratchpad', 'aux', 'erase', 'mode', 'scroll',
                  'auxiliary', 'escape', 'modifies', 'search', 'before', 'every', 'month',
                  'second', 'begin', 'except', 'months', 'seconds', 'between', 'exception',
                  'new', 'secqty', 'binary', 'excluding', 'new_table', 'security', 'bufferpool',
                  'exclusive', 'nextval', 'select', 'by', 'execute', 'no', 'sensitive', 'cache',
                  'exists', 'nocache', 'sequence', 'call', 'exit', 'nocycle', 'session', 'called',
                  'explain', 'nodename', 'session_user', 'capture', 'external', 'nodenumber',
                  'set', 'cardinality', 'extract', 'nomaxvalue', 'signal', 'cascaded', 'fenced',
                  'nominvalue', 'simple', 'case', 'fetch', 'none', 'some', 'cast', 'fieldproc',
                  'noorder', 'source', 'ccsid', 'file', 'normalized', 'specific', 'char', 'final',
                  'not', 'sql', 'character', 'for', 'null', 'sqlid', 'check', 'foreign', 'nulls',
                  'stacked', 'close', 'free', 'numparts', 'standard', 'cluster', 'from', 'obid',
                  'start', 'collection', 'full', 'of', 'starting', 'collid', 'function', 'old',
                  'statement', 'column', 'general', 'old_table', 'static', 'comment', 'generated',
                  'on', 'stay', 'commit', 'get', 'open', 'stogroup', 'concat', 'global',
                  'optimization', 'stores', 'condition', 'go', 'optimize', 'style', 'connect',
                  'goto', 'option', 'substring', 'connection', 'grant', 'or', 'summary',
                  'constraint', 'graphic', 'order', 'synonym', 'contains', 'group', 'out',
                  'sysfun', 'continue', 'handler', 'outer', 'sysibm', 'count', 'hash', 'over',
                  'sysproc', 'count_big', 'hashed_value', 'overriding', 'system', 'create',
                  'having', 'package', 'system_user', 'cross', 'hint', 'padded', 'table',
                  'current', 'hold', 'pagesize', 'tablespace', 'current_date', 'hour',
                  'parameter', 'then', 'current_lc_ctype', 'hours', 'part', 'time',
                  'current_path', 'identity', 'partition', 'timestamp', 'current_schema',
                  'if', 'partitioned', 'to', 'current_server', 'immediate', 'partitioning',
                  'transaction', 'current_time', 'in', 'partitions', 'trigger',
                  'current_timestamp', 'including', 'password', 'trim', 'current_timezone',
                  'inclusive', 'path', 'type', 'current_user', 'increment', 'piecesize',
                  'undo', 'cursor', 'index', 'plan', 'union', 'cycle', 'indicator',
                  'position', 'unique', 'data', 'inherit', 'precision', 'until',
                  'database', 'inner', 'prepare', 'update', 'datapartitionname', 'inout',
                  'prevval', 'usage', 'datapartitionnum', 'insensitive', 'primary', 'user',
                  'date', 'insert', 'priqty', 'using', 'day', 'integrity', 'privileges',
                  'validproc', 'days', 'intersect', 'procedure', 'value', 'db2general',
                  'into', 'program', 'values', 'db2genrl', 'is', 'psid', 'variable',
                  'db2sql', 'isobid', 'query', 'variant', 'dbinfo', 'isolation',
                  'queryno', 'vcat', 'dbpartitionname', 'iterate', 'range', 'version',
                  'dbpartitionnum', 'jar', 'rank', 'view', 'deallocate', 'java', 'read',
                  'volatile', 'declare', 'join', 'reads', 'volumes', 'default', 'key',
                  'recovery', 'when', 'defaults', 'label', 'references', 'whenever',
                  'definition', 'language', 'referencing', 'where', 'delete', 'lateral',
                  'refresh', 'while', 'dense_rank', 'lc_ctype', 'release', 'with',
                  'denserank', 'leave', 'rename', 'without', 'describe', 'left', 'repeat',
                  'wlm', 'descriptor', 'like', 'reset', 'write', 'deterministic', 'linktype',
                  'resignal', 'xmlelement', 'diagnostics', 'local', 'restart', 'year',
                  'disable', 'localdate', 'restrict', 'years', '', 'abs', 'grouping',
                  'regr_intercept', 'are', 'int', 'regr_r2', 'array', 'integer',
                  'regr_slope', 'asymmetric', 'intersection', 'regr_sxx', 'atomic',
                  'interval', 'regr_sxy', 'avg', 'large', 'regr_syy', 'bigint', 'leading',
                  'rollup', 'blob', 'ln', 'scope', 'boolean', 'lower', 'similar', 'both',
                  'match', 'smallint', 'ceil', 'max', 'specifictype', 'ceiling', 'member',
                  'sqlexception', 'char_length', 'merge', 'sqlstate', 'character_length',
                  'method', 'sqlwarning', 'clob', 'min', 'sqrt', 'coalesce', 'mod',
                  'stddev_pop', 'collate', 'module', 'stddev_samp', 'collect', 'multiset',
                  'submultiset', 'convert', 'national', 'sum', 'corr', 'natural', 'symmetric',
                  'corresponding', 'nchar', 'tablesample', 'covar_pop', 'nclob',
                  'timezone_hour', 'covar_samp', 'normalize', 'timezone_minute', 'cube',
                  'nullif', 'trailing', 'cume_dist', 'numeric', 'translate',
                  'current_default_transform_group', 'octet_length', 'translation', 'current_role',
                  'only', 'treat', 'current_transform_group_for_type', 'overlaps', 'true', 'dec',
                  'overlay', 'uescape', 'decimal', 'percent_rank', 'unknown', 'deref',
                  'percentile_cont', 'unnest', 'element', 'percentile_disc', 'upper', 'exec',
                  'power', 'var_pop', 'exp', 'real', 'var_samp', 'false', 'recursive', 'varchar',
                  'filter', 'ref', 'varying', 'float', 'regr_avgx', 'width_bucket', 'floor',
                  'regr_avgy', 'window', 'fusion', 'regr_count', 'within', 'asc'}


class IBMBoolean(sa_types.Boolean):
    """ DB2 Boolean """
    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            return bool(value)

        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            if bool(value):
                return '1'
            return '0'

        return process


class IBMDate(sa_types.Date):
    """ DB2 Date """
    def result_processor(self, dialect, coltype):
        def process(value):
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                value = datetime.date(value.year, value.month, value.day)
            return value

        return process

    def bind_processor(self, dialect):
        def process(value):
            if value is None:
                return None
            if isinstance(value, datetime.datetime):
                value = datetime.date(value.year, value.month, value.day)
            return str(value)

        return process


class DOUBLE(sa_types.Numeric):
    """ Double Class """
    __visit_name__ = 'DOUBLE'


class LONGVARCHAR(sa_types.VARCHAR):
    """ LONGVARCHAR Class """
    __visit_name_ = 'LONGVARCHAR'


class DBCLOB(sa_types.CLOB):
    """ DBCLOB Class """
    __visit_name__ = "DBCLOB"


class GRAPHIC(sa_types.CHAR):
    """ GRAPHIC Class """
    __visit_name__ = "GRAPHIC"


class VARGRAPHIC(sa_types.Unicode):
    """ VARGRAPHIC Class """
    __visit_name__ = "VARGRAPHIC"


class LONGVARGRAPHIC(sa_types.UnicodeText):
    """ LONGVARGRAPHIC Class """
    __visit_name__ = "LONGVARGRAPHIC"


class XML(sa_types.Text):
    """ XML Class """
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
    """ Compiler for DB2 Types """
    def visit_TIMESTAMP(self, type_, **_):
        return "TIMESTAMP"

    def visit_DATE(self, type_, **_):
        return "DATE"

    def visit_TIME(self, type_, **_):
        return "TIME"

    def visit_DATETIME(self, type_, **_):
        return self.visit_TIMESTAMP(type_, )

    def visit_SMALLINT(self, type_, **_):
        return "SMALLINT"

    def visit_BIGINT(self, type_, **_):
        return "BIGINT"

    def visit_INTEGER(self, type_, **_):
        return "INT"

    def visit_FLOAT(self, type_, **_):
        return "FLOAT" if type_.precision is None else \
            "FLOAT(%(precision)s)" % {'precision': type_.precision}

    def visit_double(self, _):
        return "DOUBLE"

    def visit_xml(self, _):
        return "XML"

    def visit_CLOB(self, type_, **_):
        return "CLOB"

    def visit_BLOB(self, type_, **_):
        return "BLOB(1M)" if type_.length in (None, 0) else \
            "BLOB(%(length)s)" % {'length': type_.length}

    def visit_dbclob(self, type_):
        return "DBCLOB(1M)" if type_.length in (None, 0) else \
            "DBCLOB(%(length)s)" % {'length': type_.length}

    def visit_VARCHAR(self, type_, **_):
        return "VARCHAR(%(length)s)" % {'length': type_.length}

    def visit_longvarchar(self, _):
        return "LONG VARCHAR"

    def visit_vargraphic(self, type_):
        return "VARGRAPHIC(%(length)s)" % {'length': type_.length}

    def visit_longvargraphic(self, type_):
        return "LONG VARGRAPHIC"

    def visit_CHAR(self, type_, **_):
        return "CHAR" if type_.length in (None, 0) else \
            "CHAR(%(length)s)" % {'length': type_.length}

    def visit_graphic(self, type_):
        return "GRAPHIC" if type_.length in (None, 0) else \
            "GRAPHIC(%(length)s)" % {'length': type_.length}

    def visit_DECIMAL(self, type_, **_):
        if not type_.precision:
            return "DECIMAL(31, 0)"
        if not type_.scale:
            return "DECIMAL(%(precision)s, 0)" % {'precision': type_.precision}
        return "DECIMAL(%(precision)s, %(scale)s)" % {
            'precision': type_.precision, 'scale': type_.scale}

    def visit_numeric(self, type_, **_):
        return self.visit_DECIMAL(type_, )

    def visit_datetime(self, type_, **_):
        return self.visit_TIMESTAMP(type_, )

    def visit_date(self, type_, **_):
        return self.visit_DATE(type_, )

    def visit_time(self, type_, **_):
        return self.visit_TIME(type_, )

    def visit_integer(self, type_, **_):
        return self.visit_INTEGER(type_)

    def visit_boolean(self, type_, **_):
        return self.visit_SMALLINT(type_, )

    def visit_float(self, type_, **_):
        return self.visit_FLOAT(type_, )

    def visit_unicode(self, type_, **_):
        return self.visit_vargraphic(type_)

    def visit_unicode_text(self, type_, **_):
        return self.visit_longvargraphic(type_)

    def visit_string(self, type_, **_):
        return self.visit_VARCHAR(type_, )

    def visit_TEXT(self, type_, **_):
        return self.visit_CLOB(type_, )

    def visit_large_binary(self, type_, **_):
        return self.visit_BLOB(type_, )


class DB2Compiler(compiler.SQLCompiler):
    """ Compiler for DB2 """
    def visit_empty_set_expr(self, element_types):
        pass

    def update_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        pass

    def delete_extra_from_clause(self, update_stmt, from_table, extra_froms, from_hints, **kw):
        pass

    if SA_VERSION < [0, 9]:
        def visit_false(self, expr, **kw):
            return '0'

        def visit_true(self, expr, **kw):
            return '1'

    def get_cte_preamble(self, recursive):
        return "WITH"

    def visitNowFunc(self):
        return "CURRENT_TIMESTAMP"

    def for_update_clause(self, select, **kw):
        if select.for_update:
            return ' WITH RS USE AND KEEP UPDATE LOCKS'
        if select.for_update == 'read':
            return ' WITH RS USE AND KEEP SHARE LOCKS'
        return ''

    def visit_mod_binary(self, binary, operator, **kw):
        return "mod(%s, %s)" % (self.process(binary.left),
                                self.process(binary.right))

    def limit_clause(self, select, **kwargs):
        if (select._limit is not None) and (select._offset is None):
            return " FETCH FIRST %s ROWS ONLY" % select._limit
        return ""

    def visit_select(self, select, **kwargs):
        limit, offset = select._limit_clause, select._offset_clause
        sql_ori = compiler.SQLCompiler.visit_select(self, select, **kwargs)
        if offset is not None:
            __rownum = 'Z.__ROWNUM'
            sql_split = re.split(r"[\s+]FROM ", sql_ori, 1)
            sql_sec = ""
            sql_sec = " \nFROM %s " % (sql_split[1])

            dummy_val = "Z.__db2_"
            sql_pri = ""

            sql_sel = "SELECT "
            if select._distinct:
                sql_sel = "SELECT DISTINCT "

            sql_select_token = sql_split[0].split(",")
            i = 0
            while i < len(sql_select_token):
                if sql_select_token[i].count(
                        "TIMESTAMP(DATE(SUBSTR(CHAR(") == 1:
                    sql_sel = "%s \"%s%d\"," % (sql_sel, dummy_val, i + 1)
                    sql_pri = '%s %s,%s,%s,%s AS "%s%d",' % (
                        sql_pri,
                        sql_select_token[i],
                        sql_select_token[i + 1],
                        sql_select_token[i + 2],
                        sql_select_token[i + 3],
                        dummy_val, i + 1)
                    i = i + 4
                    continue

                if sql_select_token[i].count(" AS ") == 1:
                    temp_col_alias = sql_select_token[i].split(" AS ")
                    sql_pri = '%s %s,' % (sql_pri, sql_select_token[i])
                    sql_sel = "%s %s," % (sql_sel, temp_col_alias[1])
                    i = i + 1
                    continue

                sql_pri = '%s %s AS "%s%d",' % (
                    sql_pri, sql_select_token[i], dummy_val, i + 1)
                sql_sel = "%s \"%s%d\"," % (sql_sel, dummy_val, i + 1)
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
                sql = '%s AND ' % (sql)
            if limit is not None:
                sql = '%s "%s" <= %d' % (sql, __rownum, offset + limit)
            return "( %s )" % (sql,)
        return sql_ori

    def visit_sequence(self, sequence, **kwargs):
        return "NEXT VALUE FOR %s" % sequence.name

    def default_from(self):
        # DB2 uses SYSIBM.SYSDUMMY1 table for row count
        return " FROM SYSIBM.SYSDUMMY1"

    def visit_function(self, func, result_map=None, **kwargs):
        if func.name.upper() == "AVG":
            return "AVG(DOUBLE(%s))" % (self.function_argspec(func, **kwargs))

        if func.name.upper() == "CHAR_LENGTH":
            return "CHAR_LENGTH(%s, %s)" % (
                self.function_argspec(func, **kwargs), 'OCTETS')

        return compiler.SQLCompiler.visit_function(self, func, **kwargs)

    def visit_cast(self, cast, **kw):
        type_ = cast.typeclause.type
        if isinstance(type_, (
                sa_types.DateTime, sa_types.Date, sa_types.Time,
                sa_types.DECIMAL, sa_types.String)):
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
        if unary.operator == operators.exists and kw.get(
                'within_columns_clause', False):
            usql = super(DB2Compiler, self).visit_unary(unary, **kw)
            usql = "CASE WHEN " + usql + " THEN 1 ELSE 0 END"
            return usql
        return super(DB2Compiler, self).visit_unary(unary, **kw)


class DB2DDLCompiler(compiler.DDLCompiler):
    """Compiler for DB2 DDL statements"""
    def get_server_version_info(self, dialect):
        """Returns the DB2 server major and minor version as a list of ints."""
        if hasattr(dialect, 'dbms_ver'):
            return [int(ver_token)
                    for ver_token in dialect.dbms_ver.split('.')[0:2]]
        return []

    def _is_nullable_unique_constraint_supported(self, dialect):
        """Checks to see if the DB2 version is at least 10.5.
        This is needed for checking if unique constraints with null columns are supported.
        """

        dbms_name = getattr(dialect, 'dbms_name', None)
        if hasattr(dialect, 'dbms_name'):
            if dbms_name is not None and (dbms_name.find('DB2/') != -1):
                return self.get_server_version_info(dialect) >= [10, 5]
        else:
            return False

    def get_column_specification(self, column, **kw):
        col_spec = [self.preparer.format_column(column), self.dialect.type_compiler.process(
            column.type,
            type_expression=column)]

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
                "DB2 does not support UPDATE CASCADE for foreign keys.")

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
            if self._is_nullable_unique_constraint_supported(self.dialect):
                for column in constraint:
                    if column.nullable:
                        constraint.uConstraint_as_index = True
                if getattr(constraint, 'uConstraint_as_index', None):
                    qual = "INDEX "
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

    def create_table_constraints(self, table, **kw):
        if self._is_nullable_unique_constraint_supported(self.dialect):
            for constraint in table._sorted_constraints:
                if isinstance(constraint, sa_schema.UniqueConstraint):
                    for column in constraint:
                        if column.nullable:
                            constraint.use_alter = True
                            constraint.uConstraint_as_index = True
                            break
                    if getattr(constraint, 'uConstraint_as_index', None):
                        if not constraint.name:
                            index_name = "%s_%s_%s" % ('ukey', self.preparer.format_table(
                                constraint.table), '_'.join(column.name for column in constraint))
                        else:
                            index_name = constraint.name
                        index = sa_schema.Index(
                            index_name, *(column for column in constraint))
                        index.unique = True
                        index.uConstraint_as_index = True
        result = super(
            DB2DDLCompiler,
            self).create_table_constraints(
                table,
                **kw
            )
        return result

    def visit_create_index(
            self,
            create,
            include_schema=True,
            include_table_schema=True):
        if SA_VERSION < [0, 8]:
            sql = super(DB2DDLCompiler, self).visit_create_index(create)
        else:
            sql = super(
                DB2DDLCompiler,
                self).visit_create_index(
                    create,
                    include_schema,
                    include_table_schema
                )
        if getattr(create.element, 'uConstraint_as_index', None):
            sql += ' EXCLUDE NULL KEYS'
        return sql

    def visit_add_constraint(self, create):
        if self._is_nullable_unique_constraint_supported(self.dialect):
            if isinstance(create.element, sa_schema.UniqueConstraint):
                for column in create.element:
                    if column.nullable:
                        create.element.uConstraint_as_index = True
                        break
                if getattr(create.element, 'uConstraint_as_index', None):
                    if not create.element.name:
                        index_name = "%s_%s_%s" % ('uk_index', self.preparer.format_table(
                            create.element.table), '_'.join(
                                column.name for column in create.element))
                    else:
                        index_name = create.element.name
                    index = sa_schema.Index(
                        index_name, *(column for column in create.element))
                    index.unique = True
                    index.uConstraint_as_index = True
                    sql = self.visit_create_index(sa_schema.CreateIndex(index))
                    return sql
        sql = super(DB2DDLCompiler, self).visit_add_constraint(create)
        return sql


class DB2IdentifierPreparer(compiler.IdentifierPreparer):
    """Identifier preparer for DB2"""
    reserved_words = RESERVED_WORDS
    illegal_initial_characters = set(range(0, 10)).union(["_", "$"])


class _SelectLastRowIDMixin():
    """Select last row ID class"""
    _select_lastrowid = False
    _lastrowid = None

    def get_lastrowid(self):
        return self._lastrowid

    def pre_exec(self):
        if self.isinsert:
            tbl = self.compiled.statement.table
            seq_column = tbl._autoincrement_column
            insert_has_sequence = seq_column is not None

            self._select_lastrowid = insert_has_sequence and not \
                self.compiled.returning and not self.compiled.inline

    def post_exec(self):
        conn = self.root_connection
        if self._select_lastrowid:
            conn._cursor_execute(
                self.cursor,
                "SELECT IDENTITY_VAL_LOCAL() FROM SYSIBM.SYSDUMMY1",
                (),
                self)
            row = self.cursor.fetchall()[0]
            if row[0] is not None:
                self._lastrowid = int(row[0])


class DB2ExecutionContext(
        _SelectLastRowIDMixin,
        default.DefaultExecutionContext):
    """ Execcution context for DB2 """
    def create_server_side_cursor(self):
        pass

    def result(self):
        pass

    def get_rowcount(self):
        pass

    def fire_sequence(self, seq, type_):
        return self._execute_scalar(
            "SELECT NEXTVAL FOR " +
            self.connection.dialect.identifier_preparer.format_sequence(seq) +
            " FROM SYSIBM.SYSDUMMY1",
            type_)


class DB2Dialect(default.DefaultDialect):
    """ SQLAlchemy Dialect for DB2 """
    name = 'sqlalchemy_ibmi'
    max_identifier_length = 128
    encoding = 'utf-8'
    default_paramstyle = 'qmark'
    COLSPECS = COLSPECS
    ISCHEMA_NAMES = ISCHEMA_NAMES
    supports_char_length = False
    supports_unicode_statements = False
    supports_unicode_binds = False
    returns_unicode_strings = False
    postfetch_lastrowid = True
    supports_sane_rowcount = True
    supports_sane_multi_rowcount = True
    supports_native_decimal = False
    supports_native_boolean = False
    preexecute_sequences = False
    supports_alter = True
    supports_sequences = True
    sequences_optional = True

    requires_name_normalize = True

    supports_default_values = False
    supports_empty_insert = False

    two_phase_transactions = False
    savepoints = True

    statement_compiler = DB2Compiler
    ddl_compiler = DB2DDLCompiler
    type_compiler = DB2TypeCompiler
    preparer = DB2IdentifierPreparer
    execution_ctx_cls = DB2ExecutionContext

    ischema = MetaData()

    sys_schemas = Table("SQLSCHEMAS", ischema,
                        Column("TABLE_SCHEM", CoerceUnicode, key="schemaname"),
                        schema="SYSIBM")

    sys_tables = Table("SYSTABLES", ischema,
                       Column("TABLE_SCHEMA", CoerceUnicode, key="tabschema"),
                       Column("TABLE_NAME", CoerceUnicode, key="tabname"),
                       Column("TABLE_TYPE", CoerceUnicode, key="tabtype"),
                       schema="QSYS2")

    sys_table_constraints = Table(
        "SYSCST", ischema, Column(
            "CONSTRAINT_SCHEMA", CoerceUnicode, key="conschema"), Column(
                "CONSTRAINT_NAME", CoerceUnicode, key="conname"), Column(
                    "CONSTRAINT_TYPE", CoerceUnicode, key="contype"), Column(
                        "TABLE_SCHEMA", CoerceUnicode, key="tabschema"), Column(
                            "TABLE_NAME", CoerceUnicode, key="tabname"), Column(
                                "TABLE_TYPE", CoerceUnicode, key="tabtype"), schema="QSYS2")

    sys_key_constraints = Table("SYSKEYCST", ischema,
                                Column("CONSTRAINT_SCHEMA", CoerceUnicode, key="conschema"),
                                Column("CONSTRAINT_NAME", CoerceUnicode, key="conname"),
                                Column("TABLE_SCHEMA", CoerceUnicode, key="tabschema"),
                                Column("TABLE_NAME", CoerceUnicode, key="tabname"),
                                Column("COLUMN_NAME", CoerceUnicode, key="colname"),
                                Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
                                schema="QSYS2")

    sys_columns = Table("SYSCOLUMNS", ischema,
                        Column("TABLE_SCHEMA", CoerceUnicode, key="tabschema"),
                        Column("TABLE_NAME", CoerceUnicode, key="tabname"),
                        Column("COLUMN_NAME", CoerceUnicode, key="colname"),
                        Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
                        Column("DATA_TYPE", CoerceUnicode, key="typename"),
                        Column("LENGTH", sa_types.Integer, key="length"),
                        Column("NUMERIC_SCALE", sa_types.Integer, key="scale"),
                        Column("IS_NULLABLE", sa_types.Integer, key="nullable"),
                        Column("COLUMN_DEFAULT", CoerceUnicode, key="defaultval"),
                        Column("HAS_DEFAULT", CoerceUnicode, key="hasdef"),
                        Column("IS_IDENTITY", CoerceUnicode, key="isid"),
                        Column("IDENTITY_GENERATION", CoerceUnicode, key="idgenerate"),
                        schema="QSYS2")

    sys_indexes = Table("SYSINDEXES", ischema,
                        Column("TABLE_SCHEMA", CoerceUnicode, key="tabschema"),
                        Column("TABLE_NAME", CoerceUnicode, key="tabname"),
                        Column("INDEX_SCHEMA", CoerceUnicode, key="indschema"),
                        Column("INDEX_NAME", CoerceUnicode, key="indname"),
                        Column("IS_UNIQUE", CoerceUnicode, key="uniquerule"),
                        schema="QSYS2")

    sys_keys = Table("SYSKEYS", ischema,
                     Column("INDEX_SCHEMA", CoerceUnicode, key="indschema"),
                     Column("INDEX_NAME", CoerceUnicode, key="indname"),
                     Column("COLUMN_NAME", CoerceUnicode, key="colname"),
                     Column("ORDINAL_POSITION", sa_types.Integer, key="colno"),
                     Column("ORDERING", CoerceUnicode, key="ordering"),
                     schema="QSYS2")

    sys_foreignkeys = Table("SQLFOREIGNKEYS", ischema,
                            Column("FK_NAME", CoerceUnicode, key="fkname"),
                            Column("FKTABLE_SCHEM", CoerceUnicode, key="fktabschema"),
                            Column("FKTABLE_NAME", CoerceUnicode, key="fktabname"),
                            Column("FKCOLUMN_NAME", CoerceUnicode, key="fkcolname"),
                            Column("PK_NAME", CoerceUnicode, key="pkname"),
                            Column("PKTABLE_SCHEM", CoerceUnicode, key="pktabschema"),
                            Column("PKTABLE_NAME", CoerceUnicode, key="pktabname"),
                            Column("PKCOLUMN_NAME", CoerceUnicode, key="pkcolname"),
                            Column("KEY_SEQ", sa_types.Integer, key="colno"),
                            schema="SYSIBM")

    sys_views = Table("SYSVIEWS", ischema,
                      Column("TABLE_SCHEMA", CoerceUnicode, key="viewschema"),
                      Column("TABLE_NAME", CoerceUnicode, key="viewname"),
                      Column("VIEW_DEFINITION", CoerceUnicode, key="text"),
                      schema="QSYS2")

    sys_sequences = Table(
        "SYSSEQUENCES",
        ischema,
        Column(
            "SEQUENCE_SCHEMA",
            CoerceUnicode,
            key="seqschema"),
        Column(
            "SEQUENCE_NAME",
            CoerceUnicode,
            key="seqname"),
        schema="QSYS2")

    def get_temp_table_names(self, connection, schema=None, **kw):
        pass

    def get_temp_view_names(self, connection, schema=None, **kw):
        pass

    def get_check_constraints(self, connection, table_name, schema=None, **kw):
        pass

    def get_table_comment(self, connection, table_name, schema=None, **kw):
        pass

    def _get_server_version_info(self, connection):
        pass

    def do_begin_twophase(self, connection, xid):
        pass

    def do_prepare_twophase(self, connection, xid):
        pass

    def do_rollback_twophase(self, connection, xid, is_prepared=True, recover=False):
        pass

    def do_commit_twophase(self, connection, xid, is_prepared=True, recover=False):
        pass

    def do_recover_twophase(self, connection):
        pass

    def set_isolation_level(self, dbapi_conn, level):
        pass

    def get_isolation_level(self, dbapi_conn):
        pass


    # reflection: these all defer to an BaseDB2Reflector
    # object which selects between DB2 and AS/400 schemas
    def initialize(self, connection):
        super(DB2Dialect, self).initialize(connection)
        self.dbms_ver = getattr(connection.connection, 'dbms_ver', None)
        self.dbms_name = getattr(connection.connection, 'dbms_name', None)

    def _get_default_schema_name(self, connection):
        """Return: current setting of the schema attribute"""
        default_schema_name = connection.execute(
            u'SELECT CURRENT_SCHEMA FROM SYSIBM.SYSDUMMY1').scalar()
        if isinstance(default_schema_name, str):
            default_schema_name = default_schema_name.strip()
        return self.normalize_name(default_schema_name)

    @property
    def default_schema_name(self):
        return self.default_schema_name

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
        s = sql.select([self.sys_tables], whereclause)
        c = connection.execute(s)
        return c.first() is not None

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
        s = sql.select([self.sys_sequences.c.seqname], whereclause)
        c = connection.execute(s)
        return c.first() is not None

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
        query = sql.select([systbl.c.tabname]). \
            where(systbl.c.tabtype == 'T'). \
            where(systbl.c.tabschema == current_schema). \
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

        query = sql.select([syscols.c.colname,
                            syscols.c.typename,
                            syscols.c.defaultval, syscols.c.nullable,
                            syscols.c.length, syscols.c.scale,
                            syscols.c.isid, syscols.c.idgenerate],
                           sql.and_(
                               syscols.c.tabschema == current_schema,
                               syscols.c.tabname == table_name
                           ),
                           order_by=[syscols.c.colno]
                           )
        sa_columns = []
        for r in connection.execute(query):
            coltype = r[1].upper()
            if coltype in ['DECIMAL', 'NUMERIC']:
                coltype = self.ISCHEMA_NAMES.get(coltype)(int(r[4]), int(r[5]))
            elif coltype in ['CHARACTER', 'CHAR', 'VARCHAR',
                             'GRAPHIC', 'VARGRAPHIC']:
                coltype = self.ISCHEMA_NAMES.get(coltype)(int(r[4]))
            else:
                try:
                    coltype = self.ISCHEMA_NAMES[coltype]
                except KeyError:
                    util.warn("Did not recognize type '%s' of column '%s'" %
                              (coltype, r[0]))
                    coltype = coltype = sa_types.NULLTYPE

            sa_columns.append({
                'name': self.normalize_name(r[0]),
                'type': coltype,
                'nullable': r[3] == 'Y',
                'default': r[2],
                'autoincrement': (r[6] == 'YES') and (r[7] is not None),
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
        query = sql.select([sysfkeys.c.fkname, sysfkeys.c.fktabschema,
                            sysfkeys.c.fktabname, sysfkeys.c.fkcolname,
                            sysfkeys.c.pkname, sysfkeys.c.pktabschema,
                            sysfkeys.c.pktabname, sysfkeys.c.pkcolname],
                           sql.and_(
                               sysfkeys.c.fktabschema == current_schema,
                               sysfkeys.c.fktabname == table_name
                           ), order_by=[sysfkeys.c.colno]
                           )
        fschema = {}
        for r in connection.execute(query):
            if r[0] not in fschema:
                referred_schema = self.normalize_name(r[5])

                # if no schema specified and referred schema here is the
                # default, then set to None
                if schema is None and \
                        referred_schema == default_schema:
                    referred_schema = None

                fschema[r[0]] = {'name': self.normalize_name(r[0]),
                                 'constrained_columns': [self.normalize_name(r[3])],
                                 'referred_schema': referred_schema,
                                 'referred_table': self.normalize_name(r[6]),
                                 'referred_columns': [self.normalize_name(r[7])]}
            else:
                fschema[r[0]]['constrained_columns'].append(
                    self.normalize_name(r[3]))
                fschema[r[0]]['referred_columns'].append(
                    self.normalize_name(r[7]))
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
                            sysidx.c.uniquerule, syskey.c.colname], sql.and_(
                                syskey.c.indschema == sysidx.c.indschema,
                                syskey.c.indname == sysidx.c.indname,
                                sysidx.c.tabschema == current_schema,
                                sysidx.c.tabname == table_name
                            ),
                           order_by=[syskey.c.indname, syskey.c.colno]
                           )
        indexes = {}
        for r in connection.execute(query):
            key = r[0].upper()
            if key in indexes:
                indexes[key]['column_names'].append(self.normalize_name(r[2]))
            else:
                indexes[key] = {
                    'name': self.normalize_name(r[0]),
                    'column_names': [self.normalize_name(r[2])],
                    'unique': r[1] == 'Y'
                }
        return [value for key, value in indexes.items()]

    @reflection.cache
    def get_unique_constraints(
            self,
            connection,
            table_name,
            schema=None,
            **kw):
        unique_consts = []
        return unique_consts
