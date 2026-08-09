#!/usr/bin/env python3
"""
Quick and dirty smoke test for BvgxmlIngester: ingests a user-supplied zip of BVG-XML
Linienfahrplan files into an in-memory SQLite (SpatiaLite) database and prints a summary.

Nothing is persisted -- the database disappears when the process exits. This is meant for
poking at a zip file interactively, not for anything that needs to survive the run; use
bin/ingest_vdv.py-style plumbing against a real Postgres/PostGIS database for that.

Usage:
    python bin/ingest_bvgxml.py path/to/export.zip [-v] [--spatialite /path/to/mod_spatialite.so]
"""
import argparse
import glob
import logging
import os
import sys
from pathlib import Path
from uuid import UUID

from sqlalchemy.orm import Session
from sqlalchemy.pool import StaticPool

import eflips.model
from eflips.model import create_engine

from eflips.ingest.bvgxml import BvgxmlIngester

# A cache=shared in-memory SQLite database is visible from any connection that opens it with
# the same URI, but only for as long as *some* connection to it stays open -- the moment the
# last one closes, SQLite drops the data. BvgxmlIngester.ingest() opens its own engine/session
# internally, separate from the one used here to create the schema, so we hold a dedicated
# connection open for the whole script to keep the database alive across both.
DATABASE_URL = "sqlite:///file:bvgxml_test?mode=memory&cache=shared&uri=true"


def _find_spatialite() -> str | None:
    """Best-effort search for mod_spatialite, so the script works without env setup on a
    machine that has it installed via Homebrew or apt."""
    candidates = [
        "/opt/homebrew/lib/mod_spatialite.dylib",
        "/usr/local/lib/mod_spatialite.dylib",
        "/usr/lib/x86_64-linux-gnu/mod_spatialite.so",
        "/usr/lib/mod_spatialite.so",
    ]
    for candidate in candidates:
        if os.path.exists(candidate):
            return candidate
    for pattern in ("/opt/homebrew/**/mod_spatialite*.dylib", "/usr/local/**/mod_spatialite*.dylib"):
        matches = glob.glob(pattern, recursive=True)
        if matches:
            return matches[0]
    return None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "zip_file",
        type=Path,
        help="Path to a .zip archive containing BVG-XML Linienfahrplan (*.xml) files.",
    )
    parser.add_argument(
        "--verbose",
        "-v",
        help="Print verbose output. Multiple -v options increase the verbosity.",
        action="count",
        default=0,
    )
    parser.add_argument(
        "--spatialite",
        type=str,
        default=None,
        help="Path to the mod_spatialite shared library. Defaults to $SPATIALITE_LIBRARY_PATH, "
        "or an auto-detected Homebrew/apt install if that is unset.",
    )
    parser.add_argument(
        "--real-altitude",
        action="store_true",
        help="Look up real station altitudes (needs OPENELEVATION_URL or GOOGLE_MAPS_API_KEY set). "
        "By default this script uses eflips-model's dummy altitude (always 0m), same as the test suite.",
    )
    args = parser.parse_args()

    if not args.real_altitude:
        os.environ.setdefault("ELEVATION_DUMMY_MODE", "True")

    logging.basicConfig(level=max(logging.DEBUG, logging.ERROR - 10 * args.verbose))
    log = logging.getLogger("ingest_bvgxml")

    if not args.zip_file.is_file():
        parser.error(f"{args.zip_file} is not a file.")

    if args.spatialite:
        os.environ["SPATIALITE_LIBRARY_PATH"] = args.spatialite
    elif "SPATIALITE_LIBRARY_PATH" not in os.environ:
        found = _find_spatialite()
        if found is None:
            sys.exit(
                "Could not find mod_spatialite. Install it (e.g. `brew install libspatialite` or "
                "`apt install libsqlite3-mod-spatialite`) and either set SPATIALITE_LIBRARY_PATH "
                "or pass --spatialite /path/to/mod_spatialite."
            )
        os.environ["SPATIALITE_LIBRARY_PATH"] = found
        log.info("Using mod_spatialite at %s", found)

    # Keep one connection open for the lifetime of the script -- see the DATABASE_URL comment.
    schema_engine = create_engine(DATABASE_URL, poolclass=StaticPool)
    pinned_connection = schema_engine.connect()
    eflips.model.setup_database(schema_engine)

    ingester = BvgxmlIngester(DATABASE_URL)
    success, error_dict_or_uuid = ingester.prepare(xml_zip_file=args.zip_file, progress_callback=None)
    if not success:
        assert isinstance(error_dict_or_uuid, dict)
        for field, message in error_dict_or_uuid.items():
            log.error("%s: %s", field, message)
        sys.exit("Error during prepare()")
    assert isinstance(error_dict_or_uuid, UUID)
    uuid = error_dict_or_uuid
    log.info("prepare() succeeded, uuid=%s", uuid)

    try:
        ingester.ingest(uuid=uuid, progress_callback=None)
    except ValueError as e:
        # BvgxmlIngester.ingest() ends with fix_max_sequence(), which needs a *second*,
        # independent connection to the database to fix up SQLite's autoincrement counter --
        # impossible for an in-memory database, since a second connection just gets its own
        # empty one (our cache=shared URL only makes concurrent connections see the same data,
        # it does not give fix_max_sequence a file to reopen). The data itself has already been
        # committed by this point, so this is safe to ignore here.
        if "does not exist" not in str(e) and "in-memory SQLite" not in str(e):
            raise
        log.warning("Skipping fix_max_sequence() (known limitation for in-memory SQLite): %s", e)
    log.info("ingest() succeeded")

    with Session(schema_engine) as session:
        scenario = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id == uuid).one()
        print(f"Scenario: {scenario.name} (id={scenario.id})")
        for model in (
            eflips.model.Station,
            eflips.model.Line,
            eflips.model.Route,
            eflips.model.Rotation,
            eflips.model.Trip,
            eflips.model.StopTime,
        ):
            count = session.query(model).filter(model.scenario_id == scenario.id).count()
            print(f"  {model.__name__}: {count}")

    pinned_connection.close()
