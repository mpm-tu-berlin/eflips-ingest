import gc
import logging
import pickle
import shutil
import socket
import warnings
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Tuple
from uuid import UUID, uuid4
from zipfile import BadZipFile, ZipFile

import eflips.model
from eflips.model import ConsistencyWarning, create_engine
from sqlalchemy.orm import Session

from eflips.ingest.base import AbstractIngester
from eflips.ingest.bvgxml._emit import emit, fix_max_sequence
from eflips.ingest.bvgxml._network import build_network
from eflips.ingest.bvgxml._read import PreparedInput, merge_corpus, read_files
from eflips.ingest.bvgxml._rotations import build_rotations
from eflips.ingest.bvgxml._routes import resolve_routes
from eflips.ingest.bvgxml._schedule import build_schedule


class BvgxmlIngester(AbstractIngester):
    """
    Ingester for BVG-XML ``Linienfahrplan`` files.

    The pipeline is three phases with a database-free model in the middle::

        read ──▶ RawCorpus ──▶ Network ──▶ RouteTable ──▶ Schedule ──▶ rows
                  merged        stations    routes and     rotations
                  input         and depots  timings        and trips

    :meth:`prepare` extracts a user-supplied zip, parses and validates every contained
    ``*.xml``, and merges them into a corpus so that contradictions between files are
    reported before anything is written. :meth:`ingest` resolves that corpus and writes it
    in a single insert-only pass.

    A real export dump is not a clean set of documents, so :meth:`prepare` skips two kinds
    of file rather than rejecting the zip: the export's "no data" answer for a line that is
    not in service on the day, and files that are corrupt. Both are counted in the ingest
    report, and only a zip with nothing usable left in it is an error.

    The merge is what makes the resolution total. The export slices the network by
    ``(Linie, Stichtag)``, so no single file is a complete picture: about 8 % of a file's
    ``BPunkt`` grid points have no ``Hst`` twin in that same file, ``Strecke/ID`` is
    file-local, and a vehicle rotation is spread across one file per line it touches.
    """

    def prepare(  # type: ignore[override]
        self,
        xml_zip_file: Path,
        progress_callback: None | Callable[[float], None] = None,
    ) -> Tuple[bool, UUID | Dict[str, str]]:
        if not isinstance(xml_zip_file, Path) or not xml_zip_file.is_file():
            return False, {"xml_zip_file": "xml_zip_file must be a path to an existing file."}
        if xml_zip_file.suffix.lower() != ".zip":
            return False, {"xml_zip_file": "xml_zip_file must end in .zip."}

        uuid = uuid4()
        target_dir = self.path_for_uuid(uuid)
        target_dir.mkdir(parents=True, exist_ok=False)

        xml_dir = target_dir / "xml"
        xml_dir.mkdir()

        try:
            with ZipFile(xml_zip_file, "r") as zf:
                zf.extractall(xml_dir)
        except BadZipFile as e:
            shutil.rmtree(target_dir)
            return False, {"xml_zip_file": f"Could not read zip file: {e}"}

        xml_paths = sorted(xml_dir.rglob("*.xml"))
        if not xml_paths:
            # A dump often arrives as a zip of a zip. Unpacking it here would mean silently
            # extracting whatever a user handed us, so say what is wrong instead and let
            # them do it: the message is the whole difference between a two-second fix and
            # an unexplained rejection.
            inner_zips = sorted(path.name for path in xml_dir.rglob("*.zip"))
            shutil.rmtree(target_dir)
            if inner_zips:
                return False, {
                    "xml_zip_file": (
                        f"The zip contains no .xml files, but it does contain "
                        f"{len(inner_zips)} zip file(s) of its own ({', '.join(inner_zips[:5])}). "
                        f"Unpack the inner archive and supply a zip whose .xml files are "
                        f"directly inside it."
                    )
                }
            return False, {"xml_zip_file": "Zip contains no .xml files."}

        # Reserve the last 10 % for the merge and the pickle write, so the bar does not
        # claim 100 % before the file exists on disk.
        prepared = read_files(
            xml_paths,
            progress_callback=(lambda f: progress_callback(0.9 * f)) if progress_callback else None,
        )

        if not prepared.files:
            shutil.rmtree(target_dir)
            if prepared.skipped_invalid:
                return False, prepared.skipped_invalid
            return False, {
                "xml_zip_file": (
                    f"None of the {len(xml_paths)} .xml files in the zip carry any data: every "
                    f"one of them is the export's answer that it has no timetable for that "
                    f"line and day."
                )
            }

        # Merge now rather than in ingest(): this is where files that contradict each other
        # are caught, and a user would rather learn that from prepare() than half-way
        # through a write.
        try:
            merge_corpus(prepared.files)
        except ValueError as e:
            shutil.rmtree(target_dir)
            return False, {"xml_zip_file": str(e)}

        with open(target_dir / "schedules.pkl", "wb") as fp:
            pickle.dump(prepared, fp, protocol=pickle.HIGHEST_PROTOCOL)

        # The parsed documents are in the pickle now and ingest() never opens the XML again,
        # so the extracted copy is dead weight — 862 MB of it for a full-city import, left
        # in the temporary directory for as long as the UUID lives.
        shutil.rmtree(xml_dir, ignore_errors=True)

        if progress_callback:
            progress_callback(1.0)
        return True, uuid

    def ingest(self, uuid: UUID, progress_callback: None | Callable[[float], None] = None) -> None:
        logger = logging.getLogger(__name__)

        pkl_path = self.path_for_uuid(uuid) / "schedules.pkl"
        if not pkl_path.is_file():
            raise ValueError(f"No prepared data found at {pkl_path}; was prepare() called for this UUID?")
        with open(pkl_path, "rb") as fp:
            prepared: PreparedInput = pickle.load(fp)

        def report(fraction: float) -> None:
            if progress_callback:
                progress_callback(min(1.0, max(0.0, fraction)))

        # Resolution is pure and touches no database, so all of it happens before the
        # session is opened.
        corpus = merge_corpus(prepared.files)
        report(0.15)
        network = build_network(corpus)
        report(0.25)
        route_table = resolve_routes(corpus, network)
        report(0.55)
        rotation_table = build_rotations(corpus)
        report(0.6)
        schedule = build_schedule(corpus, network, route_table, rotation_table)
        schedule.report.absorb_prepared_input(prepared)
        report(0.65)

        # The Schedule holds no reference back to the parsed XML, so let the whole corpus
        # go before the write starts. On a full-city import that is 1400-odd parsed
        # documents' worth of memory, and the write is the part that needs the headroom.
        del corpus, route_table, rotation_table, prepared
        gc.collect()

        engine = create_engine(self.database_url)
        with Session(engine) as session:
            scenario = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id == uuid).one_or_none()
            if scenario is None:
                scenario = eflips.model.Scenario(
                    name=(
                        f"Created by BVG-XML Ingestion on {socket.gethostname()} "
                        f"at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                    ),
                    task_id=uuid,
                )
                session.add(scenario)
                session.flush()

            with warnings.catch_warnings():
                # The model warns while a rotation is half-built; it is consistent by the
                # time the session is committed.
                warnings.simplefilter("ignore", category=ConsistencyWarning)
                emit(
                    schedule,
                    scenario.id,
                    session,
                    progress_callback=lambda f: report(0.65 + 0.3 * f),
                )
            session.commit()

        fix_max_sequence(self.database_url)
        report(1.0)
        logger.info("%s", schedule.report.summary())

    @classmethod
    def prepare_param_names(cls) -> Dict[str, str | Dict[Enum, str]]:
        return {"xml_zip_file": "BVG-XML Zip File"}

    @classmethod
    def prepare_param_description(cls) -> Dict[str, str | Dict[Enum, str]]:
        return {
            "xml_zip_file": (
                "A .zip archive containing one or more BVG-XML Linienfahrplan files (*.xml). "
                "Each file is validated against the bundled bvg_xml.xsd schema during prepare(), "
                "and the files are then merged and checked against each other. Files the export "
                "answered with 'no data' for, and files that are corrupt, are skipped and "
                "counted rather than rejecting the whole archive. Include every line the "
                "exported vehicle rotations touch: a rotation whose other lines are missing "
                "cannot be imported, and the ingest log will say which lines those are."
            ),
        }
