from .util import SA_Version
from sqlalchemy.testing.suite import *  # noqa - need * to import test suite
from sqlalchemy.testing.suite import testing

from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import ExpandingBoundInTest as _ExpandingBoundInTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import StringTest as _StringTest
from sqlalchemy.testing.suite import TextTest as _TextTest
from sqlalchemy.testing.suite import UnicodeTextTest as _UnicodeTextTest
from sqlalchemy.testing.suite import UnicodeVarcharTest as _UnicodeVarcharTest

if SA_Version < [1, 4]:

    class ComponentReflectionTest(_ComponentReflectionTest):
        @testing.requires.unique_constraint_reflection
        @testing.requires.schemas
        @testing.skip("ibmi", "Db2 doesn't support duplicate constraints")
        def test_get_unique_constraints_with_schema(self):
            pass

        @testing.requires.unique_constraint_reflection
        @testing.skip("ibmi", "Db2 doesn't support duplicate constraints")
        def test_get_unique_constraints(self):
            pass

else:

    class ComponentReflectionTest(_ComponentReflectionTest):
        @testing.combinations(
            (True, testing.requires.schemas), (False,), argnames="use_schema"
        )
        @testing.requires.unique_constraint_reflection
        @testing.skip("ibmi", "Db2 doesn't support duplicate constraints")
        def test_get_unique_constraints(self, metadata, connection, use_schema):
            pass


# empty set tests not possible on DB2 for i
class ExpandingBoundInTest(_ExpandingBoundInTest):
    @testing.skip("ibmi")
    def test_multiple_empty_sets(self):
        pass

    @testing.skip("ibmi")
    def test_empty_set_against_integer(self):
        pass

    @testing.skip("ibmi")
    def test_empty_set_against_integer_negation(self):
        pass

    @testing.skip("ibmi")
    def test_empty_set_against_string(self):
        pass

    @testing.skip("ibmi")
    def test_empty_set_against_string_negation(self):
        pass

    @testing.skip("ibmi")
    def test_null_in_empty_set_is_false(self):
        pass


class InsertBehaviorTest(_InsertBehaviorTest):
    # Skipping test due to incompatible sql query with Db2. Using parameter
    # markers in a arithmetic expression is not supported. To force this to
    # work, one can cast the parameter marker to int or float before
    # performing the operation. However, this will not work here due to
    # SQLAlchemy code
    @testing.skip("ibmi")
    def test_insert_from_select_with_defaults(self):
        pass


# An assertion error is caused in certain tests by an issue with the IBM i
# Access ODBC Driver for Linux. Until that issue is fixed, the following tests
# will be skipped in the StringTest. TextTest, UnicodeTextTest,
# and UnicodeVarcharTest classes.


class StringTest(_StringTest):
    @testing.skip("ibmi")
    def test_literal_non_ascii(self):
        pass


class TextTest(_TextTest):
    @testing.skip("ibmi")
    def test_literal_non_ascii(self):
        pass


class UnicodeTextTest(_UnicodeTextTest):
    @testing.skip("ibmi")
    def test_literal_non_ascii(self):
        pass

    @testing.skip("ibmi")
    def test_literal(self):
        pass


class UnicodeVarcharTest(_UnicodeVarcharTest):
    @testing.skip("ibmi")
    def test_literal(self):
        pass

    @testing.skip("ibmi")
    def test_literal_non_ascii(self):
        pass
