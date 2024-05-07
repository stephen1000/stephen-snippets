"""
Table for testing parity tests
"""

import datetime as dt
import inspect
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from hashlib import md5
from typing import Optional

import sqlalchemy
from parity import settings
from parity.logger import logger
from sqlalchemy.orm import declarative_base, mapper
from tqdm import tqdm

Base = declarative_base()


class ConstructedTable:
    """Base class for tables constructed with the factory"""


class ConstructedTableCache(Base):
    __tablename__ = "constructed_table_cache"

    id = sqlalchemy.Column(sqlalchemy.Integer, primary_key=True, autoincrement=True)
    table_name = sqlalchemy.Column(sqlalchemy.String(200))
    signature = sqlalchemy.Column(sqlalchemy.String(200))

    def __repr__(self):
        return f"ConstructedTableCache({self.table_name}, {self.id}, {self.signature})"

    def __str__(self):
        return f"ConstructedTableCache({self.table_name}, {self.id}, {self.signature})"


ALL_TABLES = []
PERSON_NAMES = ["John", "Paul", "George", "Ringo"]


def populate_all(engine: sqlalchemy.engine.Engine, schema: str = None):
    """
    Fill all tables with default values
    """
    futures = []

    with ThreadPoolExecutor() as executor:
        for table in ALL_TABLES:
            futures.append(executor.submit(table.populate, engine, schema=schema))

        for future in tqdm(
            as_completed(futures),
            desc=f"Populating Tables",
            total=len(futures),
            unit=" tables",
        ):
            future.result()


def get_table_data(**mutators: dict[str, callable]) -> dict:
    """
    Returns a dictionary of data for the test table. Optionally, you can pass in
    mutators to modify the data as keywork arguments. The mutators should be
    callables that take in a value and row index and return a new (type compliant)
    value.

    NOTE: ``row_count`` is a reserved keyword argument that will be used to
    determine the number of rows to return. If not provided, it will default to 50.
    """

    row_count = mutators.get("row_count", 50)

    mutators.pop("row_count", None)

    for index in range(row_count):
        row = {
            "id": index + 1,
            "name": "test" if index % 2 == 0 else "TEST",
            "sometimes_null": "test" if index % 2 == 0 else None,
            "value": (index + 1) * 10,
            "created_at": dt.datetime(2020, 1, 1, 0, 0, 0),
            "updated_at": dt.datetime(2020, 1, 1, 0, 0, 0) + dt.timedelta(days=index),
            "is_true": True,
            "is_false": False,
            "is_null": None,
            "is_not_null": True if index % 2 == 0 else False,
        }

        for pair in mutators.items():
            key, mutator = pair
            if key in row:
                row[key] = mutator(index, row)

        yield row


def table_factory(table_name: str, mutations={}) -> sqlalchemy.Table:
    """
    Construct a table class
    """
    table = sqlalchemy.Table(
        table_name,
        Base.metadata,
        sqlalchemy.Column("id", sqlalchemy.Integer, primary_key=True),
        sqlalchemy.Column("name", sqlalchemy.String(50)),
        sqlalchemy.Column("sometimes_null", sqlalchemy.String(50), nullable=True),
        sqlalchemy.Column("value", sqlalchemy.Integer),
        sqlalchemy.Column("created_at", sqlalchemy.DateTime),
        sqlalchemy.Column("updated_at", sqlalchemy.DateTime),
        sqlalchemy.Column("is_true", sqlalchemy.Boolean),
        sqlalchemy.Column("is_false", sqlalchemy.Boolean),
        sqlalchemy.Column("is_null", sqlalchemy.Boolean, nullable=True),
        sqlalchemy.Column("is_not_null", sqlalchemy.Boolean, nullable=False),
    )

    @dataclass
    class TableClass(ConstructedTable):
        __tablename__ = table_name
        __table_index__ = len(ALL_TABLES)
        __sqlalchemy_table__ = table

        id: int
        name: str
        sometimes_null: Optional[str]
        value: int
        created_at: dt.datetime
        updated_at: dt.datetime
        is_true: bool
        is_false: bool
        is_null: Optional[bool]
        is_not_null: bool

        MUTATIONS = mutations

        def __str__(self):
            return f"{self.__tablename__}({self.id}, {self.name})"

        def __repr__(self):
            return f"{self.__tablename__}({self.id}, {self.name})"

        @classmethod
        def populate(
            cls, connection: sqlalchemy.engine.Connectable, schema: str = None
        ):
            """
            Fill this table with default values
            """

            logger.debug("Checking if table is already populated...")
            table_cache_query = sqlalchemy.select(ConstructedTableCache).where(
                ConstructedTableCache.table_name == cls.__tablename__
            )

            table_cache = connection.execute(table_cache_query).fetchone()

            logger.debug(f"Cache Result: {table_cache}")

            if table_cache:
                if table_cache.signature == cls.key():
                    logger.debug(
                        f"... {cls.__tablename__} already populated ({table_cache.signature})!"
                    )
                    return
                else:
                    logger.debug(
                        f"... {cls.__tablename__} populated with different ({cls.key()} != {table_cache.signature}) signature. Repopulating..."
                    )
                    with sqlalchemy.orm.Session(connection) as session:
                        session.execute(
                            sqlalchemy.delete(ConstructedTableCache).where(
                                ConstructedTableCache.table_name == cls.__tablename__
                            )
                        )
                        session.commit()
            else:
                logger.debug(f"... {cls.__tablename__} not populated. Populating...")

            logger.debug(f"Recreating {cls.__tablename__}...")
            try:
                cls.__sqlalchemy_table__.drop(bind=connection)
            except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.ProgrammingError):
                pass

            logger.debug(f"Ensuring {cls.__tablename__} exists...")
            try:
                if schema:
                    cls.__sqlalchemy_table__.schema = schema
                cls.__sqlalchemy_table__.create(bind=connection)
            except (sqlalchemy.exc.OperationalError, sqlalchemy.exc.ProgrammingError):
                pass  # Table already exists

            logger.debug(f"Populating {cls.__tablename__}...")
            rows_to_generate = get_table_data(**cls.MUTATIONS)
            data = (cls(**row) for row in rows_to_generate)

            futures = []
            current_row_count = 0
            total_row_count = cls.MUTATIONS.get("row_count", 50)
            batch_size = 1000
            batch = []

            with ThreadPoolExecutor() as executor:
                for row in tqdm(
                    data,
                    desc=f"Generating Data {cls.__tablename__}",
                    total=total_row_count,
                    unit=" rows",
                ):
                    batch.append(row)
                    if len(batch) >= batch_size:

                        def insert_batch(batch):
                            with sqlalchemy.orm.Session(connection) as session:
                                session.bulk_save_objects(batch)

                        futures.append(executor.submit(insert_batch, batch))
                        batch = []

                    current_row_count += 1

                for future in tqdm(
                    as_completed(futures),
                    desc=f"Populating {cls.__tablename__}",
                    total=len(futures),
                    unit=" batches",
                ):
                    future.result()

            with sqlalchemy.orm.Session(connection) as session:
                cache_entry = ConstructedTableCache(
                    id=cls.__table_index__,
                    table_name=cls.__tablename__,
                    signature=cls.key(),
                )
                query = sqlalchemy.select(ConstructedTableCache).where(
                    ConstructedTableCache.table_name == cls.__tablename__
                )
                if session.execute(query).fetchone():
                    logger.debug(
                        f"Deleting existing cache entry for {cls.__tablename__}"
                    )
                    session.execute(
                        sqlalchemy.delete(ConstructedTableCache).where(
                            ConstructedTableCache.table_name == cls.__tablename__
                        )
                    )
                logger.debug(f"Adding cache entry: {cache_entry}")
                session.add(cache_entry)

                session.commit()
            logger.debug(f"... {cls.__tablename__} populated ({cls.key()})!")

        @classmethod
        def key(cls):
            """
            Returns a key for this table
            """
            d = {
                "table_name": cls.__tablename__,
                "id": cls.__table_index__,
                "mutations": {
                    key: inspect.getsource(value) if callable(value) else value
                    for key, value in cls.MUTATIONS.items()
                },
            }
            text = json.dumps(d, sort_keys=True)
            return md5(text.encode("utf-8")).hexdigest()

    mapper(TableClass, table)
    ALL_TABLES.append(TableClass)

    return TableClass


logger.debug("Defining test tables...")
# Base table
TestTable = table_factory("test_table")
TestTable2 = table_factory("test_table2")

# Tables with mutations for parity tests
FailsRowCount = table_factory("fails_row_count", mutations={"row_count": 6})
FailsCardinality = table_factory(
    "fails_cardinality", mutations={"name": lambda index, row: row["name"].lower()}
)
FailsColumnValue = table_factory(
    "fails_column_value", mutations={"is_true": lambda index, row: False}
)
FailsColumnValue2 = table_factory(
    "fails_column_value2", mutations={"value": lambda index, row: row["value"] + 1}
)
FailsNullCount = table_factory(
    "fails_null_count",
    mutations={"sometimes_null": lambda index, row: "test" if index % 3 == 0 else None},
)
FailsCaseSensitive = table_factory(
    "fails_case_sensitive", mutations={"name": lambda index, row: row["name"].upper()}
)
UpperCaseName = table_factory(
    "UpperCaseName",
    mutations={"name": lambda index, row: row["name"].upper()},
)

# Tables with mutations for PII scanning
FailsPII = table_factory(
    "fails_pii",
    mutations={
        "name": lambda index, row: PERSON_NAMES[index % len(PERSON_NAMES)],
        "sometimes_null": lambda index, row: (PERSON_NAMES + [None])[
            index % (len(PERSON_NAMES) + 1)
        ],
    },
)

# Tables with mutations for length
HundredRows = table_factory(
    "hundred_rows",
    mutations={"row_count": 101, "name": lambda index, row: f"row_{index}"},
)
HundredThousandRows = table_factory(
    "hundred_thousand_rows",
    mutations={"row_count": 100_000, "name": lambda index, row: f"row_{index}"},
)
HundredThousandBadRows = table_factory(
    "hundred_thousand_bad_rows",
    mutations={
        "row_count": 100_000,
        "name": lambda index, row: f"bad_row_{index}",
        "value": lambda index, row: row["value"] * 3,
        "created_at": lambda index, row: row["created_at"] + dt.timedelta(hours=index),
        "updated_at": lambda index, row: row["updated_at"]
        + dt.timedelta(hours=index * 2),
    },
)
MillionRows = table_factory(
    "million_rows",
    mutations={"row_count": 1_000_000, "name": lambda index, row: f"row_{index}"},
)
MillionBadRows = table_factory(
    "million_bad_rows",
    mutations={
        "row_count": 1_000_000,
        "name": lambda index, row: f"bad_row_{index}",
        "value": lambda index, row: row["value"] * 2,
        "created_at": lambda index, row: row["created_at"] + dt.timedelta(hours=index),
        "updated_at": lambda index, row: row["updated_at"]
        + dt.timedelta(hours=index * 2),
    },
)

# Tables with systemic mutations

HalfBadValue = table_factory(
    "half_bad_value",
    mutations={
        "value": lambda index, row: row["value"] + 1 if index < 25 else row["value"]
    },
)
RealishId = table_factory(
    "realish_id",
    mutations={"name": lambda index, row: f"ABC000{index + 1:03}", "row_count": 1000},
)
RealishIdBadValue = table_factory(
    "realish_id_bad_dates",
    mutations={
        "name": lambda index, row: f"ABC000{index + 1:03}",
        "row_count": 1000,
        "value": lambda index, row: (
            row["value"] + 1 if 475 < index < 525 else row["value"]
        ),
    },
)

logger.debug("... Test tables defined!")
