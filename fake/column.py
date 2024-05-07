"""
Container class for a fakable column of data, with a name, a type, a method to generate
values, and a method to generate a header.
"""

from typing import Any, Callable, Optional

from sqlalchemy.sql.schema import Column


class FakableColumn:
    """
    Container class for a fakable column of data, with a name, a type, a method to generate
    values, and a method to generate a header.
    """

    def __init__(
        self,
        name: str,
        sqlalchemy_column: Column,
        spoof_method: Callable,
        header: Optional[str] = None,
    ):
        self.name = name
        self.sqlalchemy_column = sqlalchemy_column
        self.spoof_method = spoof_method
        self.header = header or name

    def generate(self, *args, **kwargs) -> Any:
        """
        Generate a value for this column
        """
        return self.spoof_method(*args, **kwargs)
