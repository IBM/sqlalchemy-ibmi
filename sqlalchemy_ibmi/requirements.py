from sqlalchemy.testing.requirements import SuiteRequirements
from sqlalchemy.testing import exclusions


class Requirements(SuiteRequirements):

    """sqlalchemy requirements for tests. This class provides the mechanism to
    set available functionality in the dialect"""

    # TODO These methods are overridden from the default dialect and should be
    #  implemented

    @property
    def on_update_cascade(self):
        """"target database must support ON UPDATE..CASCADE behavior in
        foreign keys."""

        return exclusions.closed()

    @property
    def time_microseconds(self):
        """target dialect supports representation of Python
        datetime.time() with microsecond objects."""

        return exclusions.closed()

    @property
    def unbounded_varchar(self):
        """Target database must support VARCHAR with no length"""

        return exclusions.closed()

    @property
    def window_functions(self):
        """Target database must support window functions."""
        return exclusions.open()

    @property
    def precision_numerics_enotation_small(self):
        """target backend supports Decimal() objects using E notation
        to represent very small values."""
        return exclusions.open()

    @property
    def precision_numerics_enotation_large(self):
        """target backend supports Decimal() objects using E notation
        to represent very large values."""
        return exclusions.closed()

    @property
    def precision_numerics_many_significant_digits(self):
        """target backend supports values with many digits on both sides,
        such as 319438950232418390.273596, 87673.594069654243
        """
        return exclusions.fails_if(
            lambda: True,
            "Throws error SQL0604N, regarding Decimal(38, 12)")

    @property
    def precision_numerics_retains_significant_digits(self):
        """A precision numeric type will return empty significant digits,
        i.e. a value such as 10.000 will come back in Decimal form with
        the .000 maintained."""

        return exclusions.open()

    @property
    def check_constraint_reflection(self):
        """target dialect supports reflection of check constraints"""
        return exclusions.open()

    # DB2 for i does not support temporary tables
    @property
    def temp_table_names(self):
        """target dialect supports listing of temporary table names"""
        return exclusions.closed()

    @property
    def temporary_tables(self):
        """target database supports temporary tables"""
        return exclusions.closed()

    @property
    def temporary_views(self):
        """target database supports temporary views"""
        return exclusions.closed()

    @property
    def temp_table_reflection(self):
        return exclusions.closed()

    # adding implicitly_named_constraints which is not included in the
    # requirements.py in testing suite
    @property
    def implicitly_named_constraints(self):
        """target database must apply names to unnamed constraints."""
        return exclusions.open()

    # closed due to sqlalchemy.exc.CompileError: This SELECT structure does
    # not use a simple integer value for limit
    @property
    def bound_limit_offset(self):
        return exclusions.closed()

    @property
    def floats_to_four_decimals(self):
        return exclusions.closed()

    @property
    def non_updating_cascade(self):
        """target database must *not* support ON UPDATE..CASCADE behavior in
        foreign keys."""
        return exclusions.open()
