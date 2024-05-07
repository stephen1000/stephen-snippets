#!/usr/bin/env python3
"""
`ippon.data.fake.cli`- Data Spoofing Tools
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Anything and everything you could need for reading, writing, and executing SQL.

.. click:: ippon.data.fake.cli:fake_cli
    :prog: ippon data fake
    :nested: full
"""
import sys
from csv import DictWriter
from io import StringIO

import click

from ippon.commands.config_command import FileConfigCommand
from ippon.data.fake.fakable import get_model_class


@click.group
def fake_cli(name="fake"):
    """CLI tool for generating fake data"""


@fake_cli.command()
@click.argument("model_module", type=str)
@click.argument("model_name", type=str)
@click.argument("total_records", type=int)
@click.option(
    "--chunksize",
    "-c",
    type=int,
    default=1,
    help="Number of records to generate before flushing to stdout",
)
@click.option(
    "--header",
    "-h",
    is_flag=True,
    default=False,
    show_default=True,
    help="Apply a header to output",
)
@click.option(
    "--seed",
    "-s",
    type=int,
    default=None,
    help="Seed the random number generator",
)
def n(
    model_module,
    model_name,
    total_records: int,
    chunksize: int = 1,
    header: bool = False,
    seed: int = None,
) -> None:
    """Fake ``n`` lines of data for a spoofable model"""

    if seed is not None:
        import random

        random.seed(seed)

    Model = get_model_class(model_module, model_name)
    record_count = 0
    buffer = StringIO(newline="")
    writer = DictWriter(buffer, fieldnames=Model.field_list())
    if header:
        writer.writeheader()
    while record_count < total_records:
        this_chunk = 0
        if record_count > 0:
            buffer = StringIO(newline="")
        writer = DictWriter(buffer, fieldnames=Model.field_list())
        while this_chunk < chunksize:
            record = Model.spoof().as_dict()
            writer.writerow(record)
            this_chunk += 1
            record_count += 1
        buffer.seek(0)
        print(buffer.read().strip(), file=sys.stdout)


@fake_cli.command(name="load-to-db")
@click.argument("namespace", type=str)
@click.argument("model", type=str)
@click.option("--from-file", "-f", type=click.File("r"), default=sys.stdin)
@click.option("--generate", "-g", type=bool, default=False)
def load_to_db(namespace, model, from_file, generate):
    """
    Load a model into the database specified from `namespace`.
    """

    Model = get_model_class(model_module, model_name)

    if generate:
        for record in from_file:
            Model.create(**record)
    else:
        for record in from_file:
            Model.create(**record)
            Model.commit()
            Model.flush()
            Model.close()
