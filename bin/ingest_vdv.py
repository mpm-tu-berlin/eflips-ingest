#!/user/bin/env python3
import argparse
import logging
import os
import warnings
from pathlib import Path
from tempfile import gettempdir
from uuid import uuid4, UUID
from zipfile import ZipFile

from eflips.model import Station
from sqlalchemy import create_engine

import eflips
from eflips.ingest.vdv import VdvIngester

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--database_url",
        "--database-url",
        type=str,
        help="The url of the database to be used. If it is not specified, the environment variable DATABASE_URL is used.",
        required=False,
    )
    parser.add_argument(
        help="The input files to be ingested. Can be a directory (.x10 and .X10 files are ingested), or a list of "
        "files.",
        nargs="+",
        dest="input_files",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Print verbose output. Multiple -v options increase the verbosity.",
        action="count",
    )
    parser.add_argument(
        "--create_schema",
        "--create-schema",
        "-c",
        action="store_true",
        help="Whether to create the schema in the database. If not set, it is assumed that the schema already exists.",
    )

    args = parser.parse_args()

    if args.database_url is None:
        if "DATABASE_URL" not in os.environ:
            raise ValueError(
                "The database url must be specified either as an argument or as the environment variable DATABASE_URL."
            )
        args.database_url = os.environ["DATABASE_URL"]

    engine = create_engine(args.database_url)
    if args.create_schema:
        eflips.model.setup_database(engine)

    # Take the verbose argument and set the logging level accordingly
    match args.verbose:
        case None:
            logging.basicConfig(level=logging.ERROR)
        case 0:
            logging.basicConfig(level=logging.ERROR)
        case 1:
            logging.basicConfig(level=logging.WARNING)
        case 2:
            logging.basicConfig(level=logging.INFO)
        case 3:
            logging.basicConfig(level=logging.DEBUG)
        case _:
            warnings.warn("Verbose argument is too high. Setting logging level to DEBUG.")
            logging.basicConfig(level=logging.DEBUG)

    # Check the input files. If they are directories, get all .x10 and .X10 files in them
    input_files = []
    for file in args.input_files:
        if os.path.isdir(file):
            input_files += [os.path.join(file, f) for f in os.listdir(file) if f.endswith(".x10") or f.endswith(".X10")]
        else:
            input_files.append(file)

    # Create a zip file containing all of the input files
    zip_file_name = os.path.join(gettempdir(), str(uuid4()) + ".zip")
    with ZipFile(zip_file_name, "w") as zf:
        for file in input_files:
            zf.write(file, arcname=os.path.basename(file))

    # Ingest the files
    ingester = VdvIngester(args.database_url)
    success, error_list_or_uuid = ingester.prepare(progress_callback=None, x10_zip_file=Path(zip_file_name))
    if not success:
        assert isinstance(error_list_or_uuid, list)
        for error in error_list_or_uuid:
            logging.error(error)
        raise ValueError("Error during preparation")
    else:
        assert success is True
        assert isinstance(error_list_or_uuid, UUID)
    logging.info("Preparation successful")
    ingester.ingest(progress_callback=None, uuid=error_list_or_uuid)
    logging.info("Ingestion successful")
    os.remove(zip_file_name)
