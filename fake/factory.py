"""
Factory functions for spoofing data that aligns to a SQLAlchemy model or a serializable
data structure.
"""

import datetime as dt
import random
import string

import sqlalchemy

from ippon.data.fake.column import FakableColumn


def table_factory(table_name: str, columns: list[FakableColumn]) -> sqlalchemy.Table:
    """
    Construct a table class
    """
