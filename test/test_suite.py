from sqlalchemy.testing.suite import *
import sqlalchemy as sa
import operator
from sqlalchemy.testing.suite import ComponentReflectionTest as _ComponentReflectionTest
from sqlalchemy.testing.suite import ExpandingBoundInTest as _ExpandingBoundInTest
from sqlalchemy.testing.suite import InsertBehaviorTest as _InsertBehaviorTest
from sqlalchemy.testing.suite import SequenceCompilerTest as _SequenceCompilerTest


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


class InsertBehaviorTest(_InsertBehaviorTest):
    @requirements.insert_from_select
    def test_insert_from_select_with_defaults(self):
        return
        # table = self.tables.includes_defaults
        # config.db.execute(
        #     table.insert(),
        #     [
        #         dict(id=1, data="data1"),
        #         dict(id=2, data="data2"),
        #         dict(id=3, data="data3"),
        #     ],
        # )
        #
        # config.db.execute(
        #     table.insert(inline=True).from_select(
        #         ("id", "data"),
        #         select([table.c.id + 5, table.c.data]).where(
        #             table.c.data.in_(["data2", "data3"])
        #         ),
        #     )
        # )
        #
        # eq_(
        #     config.db.execute(
        #         select([table]).order_by(table.c.data, table.c.id)
        #     ).fetchall(),
        #     [
        #         (1, "data1", 5, 4),
        #         (2, "data2", 5, 4),
        #         (7, "data2", 5, 4),
        #         (3, "data3", 5, 4),
        #         (8, "data3", 5, 4),
        #     ],
        # )


class SequenceCompilerTest(_SequenceCompilerTest):
    __requires__ = ("sequences",)
    __backend__ = True

    def test_literal_binds_inline_compile(self):
        table = Table(
            "x",
            MetaData(),
            Column("y", Integer, Sequence("y_seq")),
            Column("q", Integer),
        )

        stmt = table.insert().values(q=5)

        seq_nextval = testing.db.dialect.statement_compiler(
            statement=None, dialect=testing.db.dialect
        ).visit_sequence(Sequence("y_seq"))

        self.assert_compile(
            stmt,
            "INSERT INTO x (y, q) VALUES (%s, 5)" % seq_nextval,
            literal_binds=True,
            dialect=testing.db.dialect,
        )
