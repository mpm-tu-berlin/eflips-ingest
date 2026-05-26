import glob
import os
from pathlib import Path
from typing import List, Dict
from uuid import UUID, uuid4
from zipfile import ZipFile

import eflips.model
import pytest
from eflips.model import create_engine
from sqlalchemy.orm import Session

from eflips.ingest.bvgxml import BvgxmlIngester
from eflips.ingest.bvgxml._pipeline import (
    load_and_validate_xml,
    create_stations,
    setup_working_dictionaries,
    create_routes_and_time_profiles,
    create_trip_prototypes,
    TimeProfile,
    create_trips_and_vehicle_schedules,
    recenter_station,
)
from eflips.ingest.bvgxml._xmldata import Linienfahrplan
from tests.base import BaseIngester


class TestBVGXML:
    @pytest.fixture(autouse=True)
    def disable_altitude_lookups(self, monkeypatch) -> None:
        """Bypass network altitude lookups for all tests in this class."""
        monkeypatch.setenv("ELEVATION_DUMMY_MODE", "True")

    @pytest.fixture
    def xml_path(self) -> Path:
        """
        :return: The path to a sample BVG XML file
        """
        current_file_path = os.path.dirname(os.path.abspath(__file__))
        xml_path = os.path.join(
            current_file_path,
            "..",
            "samples",
            "BVGXML",
            "Linienfahrplan_10_05.07.2023_125_0_N_N.response.xml",
        )
        return Path(xml_path)

    @pytest.fixture
    def linienfahrplan(self, xml_path) -> Linienfahrplan:
        return load_and_validate_xml(xml_path)

    @pytest.fixture
    def multiple_linienfahrplan(self) -> List[Linienfahrplan]:
        current_file_path = os.path.dirname(os.path.abspath(__file__))
        xml_path = os.path.join(
            current_file_path,
            "..",
            "samples",
            "BVGXML",
            "*.xml",
        )
        all_files = glob.glob(xml_path)
        return [load_and_validate_xml(Path(file)) for file in all_files]

    def test_load_and_validate(self, xml_path):
        loaded_fahrplan = load_and_validate_xml(xml_path)
        assert loaded_fahrplan is not None
        assert isinstance(loaded_fahrplan, Linienfahrplan)

    def test_create_stations(self, linienfahrplan):
        engine = create_engine(os.environ["DATABASE_URL"])
        eflips.model.Base.metadata.drop_all(engine)
        eflips.model.setup_database(engine)

        with Session(engine) as session:
            scenario = eflips.model.Scenario(
                name="Test Scenario",
            )
            session.add(scenario)
            session.flush()  # flush to get the id
            scenario_id = scenario.id

            create_stations(linienfahrplan, scenario_id, session)

            all_stations = session.query(eflips.model.Station).all()
            assert len(all_stations) > 0
            for station in all_stations:
                assert station.name is not None
                assert station.name != ""
                assert station.name_short is not None
                assert station.name_short != ""

    def test_create_routes(self, linienfahrplan):
        engine = create_engine(os.environ["DATABASE_URL"])
        eflips.model.Base.metadata.drop_all(engine)
        eflips.model.setup_database(engine)

        with Session(engine) as session:
            scenario = eflips.model.Scenario(
                name="Test Scenario",
            )
            session.add(scenario)
            session.flush()
            scenario_id = scenario.id

            create_stations(linienfahrplan, scenario_id, session)
            create_routes_and_time_profiles(linienfahrplan, scenario_id, session)

            all_routes = session.query(eflips.model.Route).all()
            assert len(all_routes) > 0
            for route in all_routes:
                assert route.name is not None
                assert route.name != ""
                assert route.name_short is not None
                assert route.name_short != ""
                assert len(route.assoc_route_stations) > 0
                for assoc in route.assoc_route_stations:
                    assert assoc.station is not None
                    assert assoc.location is not None

    def test_create_trip_prototypes(self, linienfahrplan):
        engine = create_engine(os.environ["DATABASE_URL"])
        eflips.model.Base.metadata.drop_all(engine)
        eflips.model.setup_database(engine)

        with Session(engine) as session:
            scenario = eflips.model.Scenario(
                name="Test Scenario",
            )
            session.add(scenario)
            session.flush()
            scenario_id = scenario.id

            create_stations(linienfahrplan, scenario_id, session)
            trip_time_profiles, db_routes_by_lfd_nr = create_routes_and_time_profiles(
                linienfahrplan, scenario_id, session
            )
            trips_by_id: Dict[int, TimeProfile] = create_trip_prototypes(
                linienfahrplan, trip_time_profiles, db_routes_by_lfd_nr
            )
            assert len(trips_by_id) > 0
            for trip in trips_by_id.values():
                assert trip.route in db_routes_by_lfd_nr.values()
                assert len(trip.time_profile_points) > 0
                assert len(trip.time_profile_points) == len(trip.route.assoc_route_stations)

    def test_create_trips_and_vehicle_schedules(self, linienfahrplan):
        engine = create_engine(os.environ["DATABASE_URL"])
        eflips.model.Base.metadata.drop_all(engine)
        eflips.model.setup_database(engine)

        with Session(engine) as session:
            scenario = eflips.model.Scenario(
                name="Test Scenario",
            )
            session.add(scenario)
            session.flush()
            scenario_id = scenario.id

            create_stations(linienfahrplan, scenario_id, session)
            trip_time_profiles, db_routes_by_lfd_nr = create_routes_and_time_profiles(
                linienfahrplan, scenario_id, session
            )
            trips_by_id: Dict[int, TimeProfile] = create_trip_prototypes(
                linienfahrplan, trip_time_profiles, db_routes_by_lfd_nr
            )
            create_trips_and_vehicle_schedules(linienfahrplan, trips_by_id, scenario_id, session)

    def test_create_working_data(self, linienfahrplan):
        grid_points, segments, route_datas, route_lfd_nrs = setup_working_dictionaries(linienfahrplan)

        assert len(grid_points) > 0
        assert isinstance(
            grid_points[list(grid_points.keys())[0]],
            Linienfahrplan.StreckennetzDaten.Netzpunkte.Netzpunkt,
        )

        assert len(segments) > 0
        assert isinstance(
            segments[list(segments.keys())[0]],
            Linienfahrplan.StreckennetzDaten.Strecken.Strecke,
        )

        assert len(route_datas) > 0
        assert isinstance(
            route_datas[list(route_datas.keys())[0]],
            Linienfahrplan.LinienDaten.Linie.RoutenDaten.Route,
        )

        assert len(route_lfd_nrs) > 0
        assert isinstance(route_lfd_nrs[list(route_lfd_nrs.keys())[0]], int)
        for route_id in route_lfd_nrs.values():
            assert isinstance(route_id, int)
            assert isinstance(
                route_datas[route_id],
                Linienfahrplan.LinienDaten.Linie.RoutenDaten.Route,
            )

    def test_recenter_stations(self, linienfahrplan):
        engine = create_engine(os.environ["DATABASE_URL"])
        eflips.model.Base.metadata.drop_all(engine)
        eflips.model.setup_database(engine)

        with Session(engine) as session:
            scenario = eflips.model.Scenario(
                name="Test Scenario",
            )
            session.add(scenario)
            session.flush()
            scenario_id = scenario.id

            create_stations(linienfahrplan, scenario_id, session)
            create_routes_and_time_profiles(linienfahrplan, scenario_id, session)
            session.flush()

            # Load a station with as
            stat = session.query(eflips.model.Station).join(eflips.model.AssocRouteStation).first()
            recenter_station(stat, session)

            assert stat.geom is not None
            assert stat.geom != f"POINTZ(0 0 0)"
            session.flush()


class TestBvgxmlIngester(BaseIngester):
    @pytest.fixture(autouse=True)
    def disable_altitude_lookups(self, monkeypatch) -> None:
        """Bypass network altitude lookups for all tests in this class."""
        monkeypatch.setenv("ELEVATION_DUMMY_MODE", "True")

    @pytest.fixture()
    def ingester(self) -> BvgxmlIngester:
        return BvgxmlIngester(self.database_url)

    @pytest.fixture()
    def sample_xml_paths(self) -> List[Path]:
        current_file_path = Path(os.path.dirname(os.path.abspath(__file__)))
        sample_dir = current_file_path / ".." / "samples" / "BVGXML"
        return [Path(p) for p in sorted(glob.glob(str(sample_dir / "*.xml")))]

    @pytest.fixture()
    def bvg_zip_file(self, tmp_path, sample_xml_paths) -> Path:
        """Bundle every sample XML into a zip and return its path."""
        zip_path = tmp_path / f"{uuid4()}.zip"
        with ZipFile(zip_path, "w") as zf:
            for xml in sample_xml_paths:
                zf.write(xml, arcname=xml.name)
        return zip_path

    @pytest.fixture()
    def single_xml_zip(self, tmp_path, sample_xml_paths) -> Path:
        """A zip containing only one XML file, for faster end-to-end tests."""
        zip_path = tmp_path / f"{uuid4()}.zip"
        with ZipFile(zip_path, "w") as zf:
            zf.write(sample_xml_paths[0], arcname=sample_xml_paths[0].name)
        return zip_path

    def test_prepare(self, ingester, bvg_zip_file) -> None:
        progress_values: List[float] = []
        success, result = ingester.prepare(
            xml_zip_file=bvg_zip_file,
            progress_callback=progress_values.append,
        )
        assert success is True
        assert isinstance(result, UUID)
        # The ingester pickles parsed schedules under path_for_uuid(uuid).
        assert (ingester.path_for_uuid(result) / "schedules.pkl").is_file()
        # progress_callback should have been called and reach 1.0.
        assert progress_values
        assert progress_values[-1] == pytest.approx(1.0)
        assert all(0.0 <= p <= 1.0 for p in progress_values)

    def test_prepare_rejects_non_path(self, ingester) -> None:
        success, errors = ingester.prepare(xml_zip_file="not-a-path")  # type: ignore[arg-type]
        assert success is False
        assert isinstance(errors, dict)
        assert "xml_zip_file" in errors

    def test_prepare_rejects_missing_file(self, ingester, tmp_path) -> None:
        missing = tmp_path / "does_not_exist.zip"
        success, errors = ingester.prepare(xml_zip_file=missing)
        assert success is False
        assert isinstance(errors, dict)
        assert "xml_zip_file" in errors

    def test_prepare_rejects_wrong_extension(self, ingester, tmp_path) -> None:
        not_a_zip = tmp_path / "data.txt"
        not_a_zip.write_text("hello")
        success, errors = ingester.prepare(xml_zip_file=not_a_zip)
        assert success is False
        assert isinstance(errors, dict)
        assert "xml_zip_file" in errors

    def test_prepare_rejects_corrupt_zip(self, ingester, tmp_path) -> None:
        bad_zip = tmp_path / "bad.zip"
        bad_zip.write_bytes(b"this is not a zip file")
        success, errors = ingester.prepare(xml_zip_file=bad_zip)
        assert success is False
        assert isinstance(errors, dict)
        assert "xml_zip_file" in errors

    def test_prepare_rejects_empty_zip(self, ingester, tmp_path) -> None:
        empty_zip = tmp_path / "empty.zip"
        with ZipFile(empty_zip, "w") as zf:
            zf.writestr("readme.txt", "no xml here")
        success, errors = ingester.prepare(xml_zip_file=empty_zip)
        assert success is False
        assert isinstance(errors, dict)
        assert "xml_zip_file" in errors

    def test_prepare_reports_xml_errors(self, ingester, tmp_path) -> None:
        broken_zip = tmp_path / "broken.zip"
        with ZipFile(broken_zip, "w") as zf:
            zf.writestr("broken.xml", "<not valid xml")
        success, errors = ingester.prepare(xml_zip_file=broken_zip)
        assert success is False
        assert isinstance(errors, dict)
        assert "broken.xml" in errors

    def test_ingest(self, ingester, single_xml_zip) -> None:
        progress_values: List[float] = []
        success, uuid = ingester.prepare(xml_zip_file=single_xml_zip)
        assert success is True
        assert isinstance(uuid, UUID)

        # Use a fresh ingester instance as documented in BaseIngester.test_ingest.
        fresh_ingester = BvgxmlIngester(self.database_url)
        fresh_ingester.ingest(uuid, progress_callback=progress_values.append)

        assert progress_values
        assert progress_values[-1] == pytest.approx(1.0)

        engine = create_engine(self.database_url)
        with Session(engine) as session:
            scenario = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id == uuid).one_or_none()
            assert scenario is not None, "ingest() should create a Scenario for the UUID"
            n_routes = session.query(eflips.model.Route).filter(eflips.model.Route.scenario_id == scenario.id).count()
            n_trips = session.query(eflips.model.Trip).filter(eflips.model.Trip.scenario_id == scenario.id).count()
            assert n_routes > 0
            assert n_trips > 0

    def test_ingest_without_prepare_raises(self, ingester) -> None:
        with pytest.raises(ValueError, match="No prepared data found"):
            ingester.ingest(uuid4())
