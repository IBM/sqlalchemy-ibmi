import decimal
from sqlalchemy import Numeric
from sqlalchemy.testing.suite import *  # noqa - need * to import test suite
from sqlalchemy.testing.suite import Table, Column, MetaData, eq_, testing
from sqlalchemy.testing.suite import requirements, select
import sqlalchemy as sa
import operator
from sqlalchemy.testing.suite \
    import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite \
    import ExpandingBoundInTest as _ExpandingBoundInTest
from sqlalchemy.testing.suite \
    import NumericTest as _NumericTest
from sqlalchemy.testing.suite \
    import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite \
    import StringTest as _StringTest
from sqlalchemy.testing.suite \
    import TextTest as _TextTest
from sqlalchemy.testing.suite \
    import UnicodeTextTest as _UnicodeTextTest
from sqlalchemy.testing.suite \
    import UnicodeVarcharTest as _UnicodeVarcharTest


# removed constraint that used same columns with different name as it caused
# a duplicate constraint error
class ComponentReflectionTest(_ComponentReflectionTest):
    def _test_get_unique_constraints(self, schema=None):
        # SQLite dialect needs to parse the names of the constraints
        # separately from what it gets from PRAGMA index_list(), and
        # then matches them up.  so same set of column_names in two
        # constraints will confuse it.    Perhaps we should no longer
        # bother with index_list() here since we have the whole
        # CREATE TABLE?
        uniques = sorted(
            [
                {"name": "unique_a", "column_names": ["a"]},
                {"name": "unique_a_b_c", "column_names": ["a", "b", "c"]},
                {"name": "unique_asc_key", "column_names": ["asc", "key"]},
                {"name": "i.have.dots", "column_names": ["b"]},
                {"name": "i have spaces", "column_names": ["c"]},
            ],
            key=operator.itemgetter("name"),
        )
        orig_meta = self.metadata
        table = Table(
            "testtbl",
            orig_meta,
            Column("a", sa.String(20)),
            Column("b", sa.String(30)),
            Column("c", sa.Integer),
            # reserved identifiers
            Column("asc", sa.String(30)),
            Column("key", sa.String(30)),
            schema=schema,
        )
        for uc in uniques:
            table.append_constraint(
                sa.UniqueConstraint(*uc["column_names"], name=uc["name"])
            )
        orig_meta.create_all()

        inspector = sa.inspect(orig_meta.bind)
        reflected = sorted(
            inspector.get_unique_constraints("testtbl", schema=schema),
            key=operator.itemgetter("name"),
        )

        names_that_duplicate_index = set()

        for orig, refl in zip(uniques, reflected):
            # Different dialects handle duplicate index and constraints
            # differently, so ignore this flag
            dupe = refl.pop("duplicates_index", None)
            if dupe:
                names_that_duplicate_index.add(dupe)
            eq_(orig, refl)

        reflected_metadata = MetaData()
        reflected = Table(
            "testtbl",
            reflected_metadata,
            autoload_with=orig_meta.bind,
            schema=schema,
        )

        # test "deduplicates for index" logic.   MySQL and Oracle
        # "unique constraints" are actually unique indexes (with possible
        # exception of a unique that is a dupe of another one in the case
        # of Oracle).  make sure # they aren't duplicated.
        idx_names = set([idx.name for idx in reflected.indexes])
        uq_names = set(
            [
                uq.name
                for uq in reflected.constraints
                if isinstance(uq, sa.UniqueConstraint)
            ]
        ).difference(["unique_c_a_b"])

        assert not idx_names.intersection(uq_names)
        if names_that_duplicate_index:
            eq_(names_that_duplicate_index, idx_names)
            eq_(uq_names, set())


# empty set tests not possible on DB2 for i
class ExpandingBoundInTest(_ExpandingBoundInTest):

    def test_multiple_empty_sets(self):
        return

    def test_empty_set_against_integer(self):
        return

    def test_empty_set_against_integer_negation(self):
        return

    def test_empty_set_against_string(self):
        return

    def test_empty_set_against_string_negation(self):
        return

    def test_null_in_empty_set_is_false(self):
        return


class NumericTest(_NumericTest):

    # casting the value to avoid untyped parameter markers
    @testing.emits_warning(r".*does \*not\* support Decimal objects natively")
    def test_decimal_coerce_round_trip_w_cast(self):
        expr = decimal.Decimal("15.7563")

        val = testing.db.scalar(
            select([sa.cast(expr, sa.types.DECIMAL(10, 4))]))
        eq_(val, expr)

    # casting the value to avoid untyped parameter markers
    @testing.requires.implicit_decimal_binds
    @testing.emits_warning(r".*does \*not\* support Decimal objects natively")
    def test_decimal_coerce_round_trip(self):
        expr = decimal.Decimal("15.7563")

        val = testing.db.scalar(
            select([sa.cast(sa.literal(expr), sa.types.DECIMAL(10, 4))]))
        eq_(val, expr)

    # casting the value to avoid untyped parameter markers
    def test_float_coerce_round_trip(self):
        expr = 15.7563

        val = testing.db.scalar(
            select(
                [
                    sa.cast(sa.literal(expr),
                            sa.types.DECIMAL(10, 4, asdecimal=False))
                ]))
        eq_(val, expr)

    # changed Numeric precision to 31 from 38 as DB2 for i supports a max
    # precision of 31 digits
    @testing.requires.precision_numerics_many_significant_digits
    def test_many_significant_digits(self):
        numbers = {decimal.Decimal("31943874831932418390.01"),
                   decimal.Decimal("319438950232418390.273596"),
                   decimal.Decimal("87673.594069654243")}
        self._do_test(Numeric(precision=31, scale=12), numbers, numbers)


class InsertBehaviorTest(_InsertBehaviorTest):

    # Skipping test due to incompatible sql query with Db2. Using parameter
    # markers in a arithmetic expression is not supported. To force this to
    # work, one can cast the parameter marker to int or float before
    # performing the operation. However, this will not work here due to
    # SQLAlchemy code
    @requirements.insert_from_select
    def test_insert_from_select_with_defaults(self):
        return


# An assertion error is caused in certain tests by an issue with the IBM i
# Access ODBC Driver for Linux. Until that issue is fixed, the following tests
# will be skipped in the StringTest. TextTest, UnicodeTextTest,
# and UnicodeVarcharTest classes.

class StringTest(_StringTest):

    def test_literal_non_ascii(self):
        return


class TextTest(_TextTest):
    def test_literal_non_ascii(self):
        return


class UnicodeTextTest(_UnicodeTextTest):

    def test_literal_non_ascii(self):
        return

    def test_literal(self):
        return


class UnicodeVarcharTest(_UnicodeVarcharTest):

    def test_literal(self):
        return

    def test_literal_non_ascii(self):
        return
