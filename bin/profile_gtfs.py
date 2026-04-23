"""Profiling harness for GtfsIngester.

Runs prepare() and ingest() against a given GTFS zip (SQLite in-memory by
default) and captures per-phase wall time plus a pyinstrument HTML/text
profile and a cProfile .pstats dump.

Usage:
    poetry run python bin/profile_gtfs.py <gtfs_zip> [--start-date YYYY-MM-DD]
        [--duration DAY|WEEK] [--agency-id ID] [--agency-name NAME]
        [--out-dir DIR] [--label LABEL]

Network calls (altitude lookups) are patched out so the profile reflects
CPU/DB-only work.
"""

from __future__ import annotations

import argparse
import cProfile
import logging
import os
import pstats
import sys
import time
from pathlib import Path
from typing import Tuple
from uuid import UUID

# Patch out network altitude lookups before eflips.ingest is imported.
import eflips.ingest.util as _util  # noqa: E402


def _fake_altitude(latlon: Tuple[float, float]) -> float:
    return 0.0


_util.get_altitude = _fake_altitude  # type: ignore[assignment]
_util.get_altitude_google = _fake_altitude  # type: ignore[assignment]
_util.get_altitude_openelevation = _fake_altitude  # type: ignore[assignment]

from eflips.model import Base, create_engine, setup_database  # noqa: E402
from pyinstrument import Profiler  # noqa: E402

from eflips.ingest.gtfs import GtfsIngester  # noqa: E402


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("gtfs_zip", type=Path)
    p.add_argument("--start-date", default=None, help="YYYY-MM-DD; auto-picked from feed if omitted")
    p.add_argument("--duration", default="WEEK", choices=["DAY", "WEEK"])
    p.add_argument("--agency-id", default="")
    p.add_argument("--agency-name", default="")
    p.add_argument("--bus-only", action=argparse.BooleanOptionalAction, default=True)
    p.add_argument("--out-dir", type=Path, default=Path("bin/profile_out"))
    p.add_argument("--label", default=None, help="Filename prefix; defaults to zip stem")
    p.add_argument(
        "--db-url",
        default=None,
        help="SQLAlchemy URL. Defaults to a fresh sqlite file under --out-dir.",
    )
    return p.parse_args()


def _auto_start_date(zip_path: Path) -> str:
    """Pick a start date inside the feed's validity period.

    Preference: (feed_start + 1 day) to avoid edge cases at the boundary.
    """
    import gtfs_kit as gk

    feed = gk.read_feed(zip_path, dist_units="m")
    start = GtfsIngester.get_feed_validity_period(feed)
    if start is None:
        raise RuntimeError("Feed has no calendar info; pass --start-date explicitly")
    from datetime import datetime, timedelta

    start_str, _end_str = start
    d = datetime.strptime(start_str, "%Y%m%d").date() + timedelta(days=1)
    return d.isoformat()


def main() -> int:
    args = _parse_args()
    args.out_dir.mkdir(parents=True, exist_ok=True)
    label = args.label or args.gtfs_zip.stem

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    log = logging.getLogger("profile_gtfs")

    if args.db_url is None:
        db_file = args.out_dir / f"{label}.sqlite"
        if db_file.exists():
            db_file.unlink()
        db_url = f"sqlite:///{db_file.resolve()}"
    else:
        db_url = args.db_url

    engine = create_engine(db_url)
    Base.metadata.drop_all(engine)
    setup_database(engine)

    start_date = args.start_date or _auto_start_date(args.gtfs_zip)
    log.info("Profiling %s (start_date=%s duration=%s)", args.gtfs_zip, start_date, args.duration)

    ingester = GtfsIngester(db_url)

    # --- prepare() ---
    prof_prepare = Profiler(interval=0.001)
    cprof_prepare = cProfile.Profile()
    prepare_kwargs = dict(
        gtfs_zip_file=args.gtfs_zip,
        start_date=start_date,
        duration=args.duration,
        agency_name=args.agency_name,
        agency_id=args.agency_id,
        bus_only=args.bus_only,
    )

    t0 = time.perf_counter()
    prof_prepare.start()
    cprof_prepare.enable()
    ok, result = ingester.prepare(**prepare_kwargs)  # type: ignore[arg-type]
    cprof_prepare.disable()
    prof_prepare.stop()
    t_prepare = time.perf_counter() - t0

    if not ok:
        log.error("prepare() failed: %s", result)
        return 2
    assert isinstance(result, UUID)
    uid = result
    log.info("prepare() OK in %.2fs -> %s", t_prepare, uid)

    # --- ingest() ---
    prof_ingest = Profiler(interval=0.001)
    cprof_ingest = cProfile.Profile()
    t0 = time.perf_counter()
    prof_ingest.start()
    cprof_ingest.enable()
    ingester.ingest(uid, always_flush=False)
    cprof_ingest.disable()
    prof_ingest.stop()
    t_ingest = time.perf_counter() - t0
    log.info("ingest() OK in %.2fs", t_ingest)

    # --- write reports ---
    prepare_html = args.out_dir / f"{label}.prepare.html"
    ingest_html = args.out_dir / f"{label}.ingest.html"
    prepare_txt = args.out_dir / f"{label}.prepare.txt"
    ingest_txt = args.out_dir / f"{label}.ingest.txt"
    prepare_pstats = args.out_dir / f"{label}.prepare.pstats"
    ingest_pstats = args.out_dir / f"{label}.ingest.pstats"

    prepare_html.write_text(prof_prepare.output_html())
    ingest_html.write_text(prof_ingest.output_html())
    prepare_txt.write_text(prof_prepare.output_text(unicode=True, color=False, show_all=False))
    ingest_txt.write_text(prof_ingest.output_text(unicode=True, color=False, show_all=False))
    pstats.Stats(cprof_prepare).dump_stats(str(prepare_pstats))
    pstats.Stats(cprof_ingest).dump_stats(str(ingest_pstats))

    summary = args.out_dir / f"{label}.summary.txt"
    with summary.open("w") as f:
        f.write(f"label: {label}\n")
        f.write(f"gtfs_zip: {args.gtfs_zip}\n")
        f.write(f"start_date: {start_date}  duration: {args.duration}\n")
        f.write(f"prepare: {t_prepare:.2f}s\n")
        f.write(f"ingest:  {t_ingest:.2f}s\n")
        f.write(f"total:   {t_prepare + t_ingest:.2f}s\n")

    print(
        f"\n=== {label} ===\n"
        f"  prepare: {t_prepare:.2f}s\n"
        f"  ingest:  {t_ingest:.2f}s\n"
        f"  reports: {args.out_dir}/{label}.*\n",
        file=sys.stderr,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
