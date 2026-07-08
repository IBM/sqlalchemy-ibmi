from decimal import Decimal
from datetime import date, time, datetime

from sqlalchemy import literal
from sqlalchemy import select
from sqlalchemy import testing
from sqlalchemy.testing import fixtures


class CachingTest(fixtures.TestBase):
    @testing.combinations(
        (True, False),
        (1891723, 21971283),
        (1.0, 2.0),
        (Decimal("1345.0"), Decimal("2987.1")),
        (datetime(2000, 1, 1), datetime(2023, 11, 28)),
        (date(2000, 1, 1), date(2023, 11, 28)),
        (time(1), time(12)),
        ("foo", "bar"),
        (b"foo", b"bar"),
        argnames="first,second",
    )
    def test_cache_literal(self, connection, first, second):
        """Test that we don't embed literals in cached statements"""
        exp = (first, second)
        results = tuple([connection.scalar(select(literal(_))) for _ in exp])
        assert exp == results
