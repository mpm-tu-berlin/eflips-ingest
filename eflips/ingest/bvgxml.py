import logging
import pickle
import shutil
import socket
import warnings
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, List, Tuple
from uuid import UUID, uuid4
from zipfile import BadZipFile, ZipFile

import eflips.model
from eflips.model import ConsistencyWarning, create_engine
from geoalchemy2.functions import ST_Distance
from sqlalchemy import func
from sqlalchemy.orm import Session

from eflips.ingest.base import AbstractIngester
from eflips.ingest.legacy.bvgxml import (
    TimeProfile,
    create_routes_and_time_profiles,
    create_stations,
    create_trip_prototypes,
    create_trips_and_vehicle_schedules,
    fix_max_sequence,
    identify_and_delete_overlapping_rotations,
    load_and_validate_xml,
    merge_identical_rotations,
    merge_identical_stations,
    recenter_station,
)
from eflips.ingest.legacy.xmldata import Linienfahrplan


class BvgxmlIngester(AbstractIngester):
    """
    Ingester for BVG-XML Linienfahrplan files.

    ``prepare()`` extracts a user-supplied zip, parses every contained ``*.xml`` file into a
    :class:`Linienfahrplan` and validates it against the bundled ``bvg_xml.xsd`` schema. The parsed
    schedules are pickled under :meth:`path_for_uuid` for later use.

    ``ingest()`` re-uses the legacy helpers in :mod:`eflips.ingest.legacy.bvgxml` to write stations,
    routes, trips and rotations into the database, then runs the post-processing fix-ups (geometry
    recentring, identical-station/rotation merging, overlapping-rotation deletion, sequence reset).
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
            shutil.rmtree(target_dir)
            return False, {"xml_zip_file": "Zip contains no .xml files."}

        schedules: List[Linienfahrplan] = []
        errors: Dict[str, str] = {}
        for i, path in enumerate(xml_paths):
            try:
                schedules.append(load_and_validate_xml(path))
            except Exception as e:  # noqa: BLE001 — xsdata/lxml can raise various types
                errors[path.name] = str(e)
            if progress_callback:
                progress_callback((i + 1) / (len(xml_paths) + 1))

        if errors:
            shutil.rmtree(target_dir)
            return False, errors

        with open(target_dir / "schedules.pkl", "wb") as fp:
            pickle.dump(schedules, fp, protocol=pickle.HIGHEST_PROTOCOL)

        if progress_callback:
            progress_callback(1.0)
        return True, uuid

    def ingest(self, uuid: UUID, progress_callback: None | Callable[[float], None] = None) -> None:
        logger = logging.getLogger(__name__)

        pkl_path = self.path_for_uuid(uuid) / "schedules.pkl"
        if not pkl_path.is_file():
            raise ValueError(f"No prepared data found at {pkl_path}; was prepare() called for this UUID?")
        with open(pkl_path, "rb") as fp:
            schedules: List[Linienfahrplan] = pickle.load(fp)

        TOTAL_PHASES = 10

        def report(phase: int) -> None:
            if progress_callback:
                progress_callback(phase / TOTAL_PHASES)

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
            scenario_id = scenario.id
            report(1)

            for schedule in schedules:
                create_stations(schedule, scenario_id, session)
            report(2)

            create_route_results: List[
                Tuple[
                    Linienfahrplan,
                    Dict[int, Dict[int, List[TimeProfile.TimeProfilePoint]]],
                    Dict[int, None | eflips.model.Route],
                ]
            ] = []
            for schedule in schedules:
                trip_time_profiles, db_routes_by_lfd_nr = create_routes_and_time_profiles(
                    schedule, scenario_id, session
                )
                create_route_results.append((schedule, trip_time_profiles, db_routes_by_lfd_nr))
            report(3)

            all_trip_prototypes: List[Dict[int, None | TimeProfile]] = []
            for schedule, trip_time_profiles, db_routes_by_lfd_nr in create_route_results:
                all_trip_prototypes.append(create_trip_prototypes(schedule, trip_time_profiles, db_routes_by_lfd_nr))

            trip_prototypes: Dict[int, None | TimeProfile] = {}
            for the_dict in all_trip_prototypes:
                for fahrt_id, time_profile in the_dict.items():
                    if fahrt_id in trip_prototypes:
                        if trip_prototypes[fahrt_id] != time_profile:
                            raise ValueError(f"Trip {fahrt_id} has two different time profiles in different schedules")
                    else:
                        trip_prototypes[fahrt_id] = time_profile
            report(4)

            for schedule in schedules:
                with warnings.catch_warnings():
                    warnings.simplefilter("ignore", category=ConsistencyWarning)
                    create_trips_and_vehicle_schedules(schedule, trip_prototypes, scenario_id, session)
            report(5)

            stations_without_geom_q = (
                session.query(eflips.model.Station)
                .join(eflips.model.AssocRouteStation)
                .filter(eflips.model.Station.scenario_id == scenario_id)
                .distinct(eflips.model.Station.id)
            )
            for station in stations_without_geom_q:
                recenter_station(station, session)
            session.flush()
            session.expire_all()
            report(6)

            long_route_q = (
                session.query(eflips.model.Route)
                .filter(eflips.model.Route.scenario_id == scenario_id)
                .filter(eflips.model.Route.distance >= 1e6 * 1000)
            )
            for route in long_route_q:
                first_point = route.departure_station.geom
                last_point = route.arrival_station.geom
                first_point_soldner = func.ST_Transform(first_point, 3068)
                last_point_soldner = func.ST_Transform(last_point, 3068)
                dist = session.query(ST_Distance(first_point_soldner, last_point_soldner)).one()[0]
                with session.no_autoflush:
                    route.distance = dist
                    route.assoc_route_stations[-1].elapsed_distance = dist
                route.name = "CHECK DISTANCE: " + route.name
            session.flush()
            session.expire_all()
            report(7)

            merge_identical_stations(scenario_id, session)
            session.flush()
            session.expire_all()
            report(8)

            merge_identical_rotations(scenario_id, session)
            report(9)

            identify_and_delete_overlapping_rotations(scenario_id, session)
            session.commit()

        fix_max_sequence(self.database_url)
        report(10)
        logger.info("BVG-XML ingestion for UUID %s complete.", uuid)

    @classmethod
    def prepare_param_names(cls) -> Dict[str, str | Dict[Enum, str]]:
        return {"xml_zip_file": "BVG-XML Zip File"}

    @classmethod
    def prepare_param_description(cls) -> Dict[str, str | Dict[Enum, str]]:
        return {
            "xml_zip_file": (
                "A .zip archive containing one or more BVG-XML Linienfahrplan files (*.xml). "
                "Each file is validated against the bundled bvg_xml.xsd schema during prepare()."
            ),
        }
