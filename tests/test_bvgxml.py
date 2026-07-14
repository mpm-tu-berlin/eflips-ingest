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
    RegisteredRotation,
    RotationRegistry,
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

            station_mapping: dict[int, eflips.model.Station] = {}
            create_stations(linienfahrplan, scenario_id, session, station_mapping)

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

            station_mapping: dict[int, eflips.model.Station] = {}
            create_stations(linienfahrplan, scenario_id, session, station_mapping)
            create_routes_and_time_profiles(linienfahrplan, scenario_id, session, station_mapping)

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

            station_mapping: dict[int, eflips.model.Station] = {}
            create_stations(linienfahrplan, scenario_id, session, station_mapping)
            trip_time_profiles, db_routes_by_lfd_nr = create_routes_and_time_profiles(
                linienfahrplan, scenario_id, session, station_mapping
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

            station_mapping: dict[int, eflips.model.Station] = {}
            create_stations(linienfahrplan, scenario_id, session, station_mapping)
            trip_time_profiles, db_routes_by_lfd_nr = create_routes_and_time_profiles(
                linienfahrplan, scenario_id, session, station_mapping
            )
            trips_by_id: Dict[int, TimeProfile] = create_trip_prototypes(
                linienfahrplan, trip_time_profiles, db_routes_by_lfd_nr
            )
            create_trips_and_vehicle_schedules(linienfahrplan, trips_by_id, scenario_id, session, RotationRegistry())

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

            station_mapping: dict[int, eflips.model.Station] = {}
            create_stations(linienfahrplan, scenario_id, session, station_mapping)
            create_routes_and_time_profiles(linienfahrplan, scenario_id, session, station_mapping)
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

    def test_ingest_twice_same_data_independent_scenarios(self, ingester, single_xml_zip) -> None:
        """Running the same ingest twice must produce two independent scenarios.

        Ingesters are required to be re-runnable in arbitrary order and any
        number of times: each invocation should yield a fresh, independent
        scenario without colliding with rows written by earlier runs. Today the
        BVG-XML ingester explicitly assigns ``Station.id`` from the upstream
        ``Haltestellenbereich`` numbers, so the second ingest collides on the
        Station primary key.
        """
        success_a, uuid_a = ingester.prepare(xml_zip_file=single_xml_zip)
        assert success_a is True
        assert isinstance(uuid_a, UUID)
        BvgxmlIngester(self.database_url).ingest(uuid_a)

        success_b, uuid_b = ingester.prepare(xml_zip_file=single_xml_zip)
        assert success_b is True
        assert isinstance(uuid_b, UUID)
        assert uuid_b != uuid_a
        BvgxmlIngester(self.database_url).ingest(uuid_b)

        engine = create_engine(self.database_url)
        with Session(engine) as session:
            scenario_a = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id == uuid_a).one()
            scenario_b = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id == uuid_b).one()
            assert scenario_a.id != scenario_b.id

            for scenario in (scenario_a, scenario_b):
                n_stations = (
                    session.query(eflips.model.Station).filter(eflips.model.Station.scenario_id == scenario.id).count()
                )
                n_routes = (
                    session.query(eflips.model.Route).filter(eflips.model.Route.scenario_id == scenario.id).count()
                )
                n_trips = session.query(eflips.model.Trip).filter(eflips.model.Trip.scenario_id == scenario.id).count()
                assert n_stations > 0
                assert n_routes > 0
                assert n_trips > 0

    def test_ingest_twice_different_data_independent_scenarios(self, ingester, sample_xml_paths, tmp_path) -> None:
        """Running two different prepared inputs must also yield independent scenarios.

        This is the more realistic operational case: a user ingests the BVG-XML
        for one timetable slice, then later ingests a different slice. Both
        scenarios must coexist without ID collisions.
        """
        if len(sample_xml_paths) < 2:
            pytest.skip("need at least two sample XML files for this test")

        zip_a = tmp_path / f"a_{uuid4()}.zip"
        with ZipFile(zip_a, "w") as zf:
            zf.write(sample_xml_paths[0], arcname=sample_xml_paths[0].name)

        zip_b = tmp_path / f"b_{uuid4()}.zip"
        with ZipFile(zip_b, "w") as zf:
            zf.write(sample_xml_paths[1], arcname=sample_xml_paths[1].name)

        success_a, uuid_a = ingester.prepare(xml_zip_file=zip_a)
        assert success_a is True
        assert isinstance(uuid_a, UUID)
        BvgxmlIngester(self.database_url).ingest(uuid_a)

        success_b, uuid_b = ingester.prepare(xml_zip_file=zip_b)
        assert success_b is True
        assert isinstance(uuid_b, UUID)
        BvgxmlIngester(self.database_url).ingest(uuid_b)

        engine = create_engine(self.database_url)
        with Session(engine) as session:
            scenarios = (
                session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id.in_([uuid_a, uuid_b])).all()
            )
            assert len(scenarios) == 2
            assert scenarios[0].id != scenarios[1].id


# ---------------------------------------------------------------------------
# Rotation reassembly (RotationRegistry / create_trips_and_vehicle_schedules)
# and window-edge cleanup (delete_incomplete_rotations).
#
# These are DB-free: fakes stand in for the ORM session, mirroring the style
# of the chain_rotation_fragments tests in test_vdv_unit.py.
# ---------------------------------------------------------------------------

from datetime import date, datetime, timedelta
from types import SimpleNamespace

from eflips.ingest.bvgxml import _pipeline
from eflips.ingest.bvgxml._pipeline import (
    _parse_kalenderdatum,
    delete_incomplete_rotations,
)


class _NoAutoflush:
    def __enter__(self) -> "_NoAutoflush":
        return self

    def __exit__(self, *exc: object) -> bool:
        return False


class _FakeQuery:
    def __init__(self, items: list) -> None:
        self._items = items

    def filter(self, *args: object) -> "_FakeQuery":
        return self

    def all(self) -> list:
        return self._items


class _FakeSession:
    def __init__(self, rotations: list | None = None) -> None:
        self.added: list = []
        self.deleted: list = []
        self.no_autoflush = _NoAutoflush()
        self._rotations = rotations if rotations is not None else []

    def add(self, obj: object) -> None:
        self.added.append(obj)

    def add_all(self, objs: list) -> None:
        self.added.extend(objs)

    def delete(self, obj: object) -> None:
        self.deleted.append(obj)

    def flush(self) -> None:
        pass

    def query(self, *args: object) -> _FakeQuery:
        return _FakeQuery(self._rotations)


class _FakePrototype:
    """Stands in for a TimeProfile; records which rotation each trip was attached to."""

    _STATION = SimpleNamespace(name="Somewhere", id=0)

    def __init__(self) -> None:
        self.calls: list = []

    def to_trip(self, rotation: object, the_date: date) -> SimpleNamespace:
        self.calls.append((rotation, the_date))
        return SimpleNamespace(rotation=rotation, stop_times=[SimpleNamespace(station=self._STATION)])


def _schedule(*fahrzeugumlaeufe: SimpleNamespace) -> SimpleNamespace:
    return SimpleNamespace(fahrzeugumlauf_daten=SimpleNamespace(fahrzeugumlauf=list(fahrzeugumlaeufe)))


def _fahrzeugumlauf(
    umlaeufe: "list[tuple[int, str, str]]",
    fahrt_ids_by_umlauf: "dict[int, list[int]]",
    betriebshof: int = 9424,
    fahrzeugtyp: str = "GEG",
) -> SimpleNamespace:
    """
    Build a Fahrzeugumlauf fake. ``umlaeufe`` is a list of (umlauf_id, kalenderdatum,
    bezeichnung); ``fahrt_ids_by_umlauf`` gives each Umlauf's materialised FahrtIDs in this
    file's slice (an empty list models the empty Fahrtreihenfolge of a foreign line's copy).
    """
    umlauf_fakes = []
    for umlauf_id, kalenderdatum, bezeichnung in umlaeufe:
        fahrten = [SimpleNamespace(fahrt_id=fid) for fid in fahrt_ids_by_umlauf.get(umlauf_id, [])]
        teilgruppe = SimpleNamespace(
            fahrzeugtyp=[fahrzeugtyp],
            fahrtreihenfolge=SimpleNamespace(fahrt=fahrten) if fahrten else None,
        )
        umlauf_fakes.append(
            SimpleNamespace(
                umlauf_id=umlauf_id,
                kalenderdatum=kalenderdatum,
                umlaufbezeichnung=bezeichnung,
                umlaufteilgruppen=SimpleNamespace(umlaufteilgruppe=[teilgruppe]),
            )
        )
    return SimpleNamespace(
        betriebshof=betriebshof,
        fahrzeugtyp=fahrzeugtyp,
        umlaeufe=SimpleNamespace(umlauf=umlauf_fakes),
    )


@pytest.fixture
def fake_vehicle_types(monkeypatch) -> dict:
    """Replace add_or_ret_vehicle_type with a per-name singleton cache of VehicleType fakes."""
    cache: dict = {}

    def _fake(scenario_id: int, fahrzeugtyp: str, session: object) -> SimpleNamespace:
        if fahrzeugtyp not in cache:
            cache[fahrzeugtyp] = eflips.model.VehicleType(
                scenario_id=scenario_id,
                name=fahrzeugtyp,
                name_short=fahrzeugtyp,
                battery_capacity=100,
                charging_curve=[[0, 150], [1, 150]],
                opportunity_charging_capable=False,
            )
        return cache[fahrzeugtyp]

    monkeypatch.setattr(_pipeline, "add_or_ret_vehicle_type", _fake)
    return cache


class TestParseKalenderdatum:
    def test_parses_german_date(self) -> None:
        assert _parse_kalenderdatum("16.06.2025") == date(2025, 6, 16)


class TestRotationRegistry:
    KEY_A = ((1, date(2025, 6, 16)), (2, date(2025, 6, 17)))
    KEY_B = ((3, date(2025, 6, 16)),)

    def _entry(self) -> RegisteredRotation:
        return RegisteredRotation(rotation=SimpleNamespace(), betriebshof=9424, name="100/1 N02/1")

    def test_get_unknown_key_returns_none(self) -> None:
        assert RotationRegistry().get(self.KEY_A) is None

    def test_add_then_get_returns_entry(self) -> None:
        registry = RotationRegistry()
        entry = self._entry()
        registry.add(self.KEY_A, entry)
        assert registry.get(self.KEY_A) is entry
        assert registry.get(self.KEY_B) is None

    def test_conflicting_grouping_raises(self) -> None:
        # A second file groups Umlauf (2, 17.06.) with a different partner: the files contradict
        # each other about which Umlaeufe form one vehicle working. Must be loud, not silent.
        registry = RotationRegistry()
        registry.add(self.KEY_A, self._entry())
        overlapping_key = ((2, date(2025, 6, 17)), (4, date(2025, 6, 18)))
        with pytest.raises(ValueError, match="Inconsistent Fahrzeugumlauf grouping"):
            registry.get(overlapping_key)


class TestRotationReassembly:
    """create_trips_and_vehicle_schedules must join the per-line file slices of one physical
    vehicle working into a single rotation, and crash on contradictory input."""

    def test_two_file_slices_share_one_rotation(self, fake_vehicle_types) -> None:
        # The '106/2' working serves lines 106 and 204. The 106 file materialises only the 106
        # trips, the 204 file only the 204 trips; both carry the full Umlauf group.
        session = _FakeSession()
        registry = RotationRegistry()
        prototypes = {11: _FakePrototype(), 12: _FakePrototype(), 21: _FakePrototype()}

        file_106 = _schedule(_fahrzeugumlauf([(1, "16.06.2025", "106/2")], {1: [11, 12]}))
        file_204 = _schedule(_fahrzeugumlauf([(1, "16.06.2025", "106/2")], {1: [21]}))
        create_trips_and_vehicle_schedules(file_106, prototypes, 1, session, registry)
        create_trips_and_vehicle_schedules(file_204, prototypes, 1, session, registry)

        rotations = [obj for obj in session.added if isinstance(obj, eflips.model.Rotation)]
        assert len(rotations) == 1
        assert rotations[0].name == "106/2"
        attached_to = {proto.calls[0][0] for proto in prototypes.values()}
        assert attached_to == {rotations[0]}
        entry = registry.get(((1, date(2025, 6, 16)),))
        assert entry is not None and entry.materialized_fahrt_ids == {11, 12, 21}

    def test_multi_umlauf_working_keeps_one_rotation_and_name(self, fake_vehicle_types) -> None:
        # A day+night working ('163/6 N63/9') grouped into one Fahrzeugumlauf, seen in two files.
        session = _FakeSession()
        registry = RotationRegistry()
        prototypes = {11: _FakePrototype(), 21: _FakePrototype()}
        group = [(1, "16.06.2025", "163/6"), (2, "17.06.2025", "N63/9")]

        create_trips_and_vehicle_schedules(
            _schedule(_fahrzeugumlauf(group, {1: [11]})), prototypes, 1, session, registry
        )
        create_trips_and_vehicle_schedules(
            _schedule(_fahrzeugumlauf(group, {2: [21]})), prototypes, 1, session, registry
        )

        rotations = [obj for obj in session.added if isinstance(obj, eflips.model.Rotation)]
        assert len(rotations) == 1
        assert rotations[0].name == "163/6 N63/9"

    def test_duplicate_fahrt_across_files_raises(self, fake_vehicle_types) -> None:
        session = _FakeSession()
        registry = RotationRegistry()
        prototypes = {11: _FakePrototype()}
        fu = [(1, "16.06.2025", "106/2")]

        create_trips_and_vehicle_schedules(_schedule(_fahrzeugumlauf(fu, {1: [11]})), prototypes, 1, session, registry)
        with pytest.raises(ValueError, match="materialised by more than one input file"):
            create_trips_and_vehicle_schedules(
                _schedule(_fahrzeugumlauf(fu, {1: [11]})), prototypes, 1, session, registry
            )

    def test_name_contradiction_raises(self, fake_vehicle_types) -> None:
        session = _FakeSession()
        registry = RotationRegistry()
        prototypes = {11: _FakePrototype(), 21: _FakePrototype()}

        create_trips_and_vehicle_schedules(
            _schedule(_fahrzeugumlauf([(1, "16.06.2025", "106/2")], {1: [11]})), prototypes, 1, session, registry
        )
        with pytest.raises(ValueError, match="is named"):
            create_trips_and_vehicle_schedules(
                _schedule(_fahrzeugumlauf([(1, "16.06.2025", "204/8")], {1: [21]})), prototypes, 1, session, registry
            )

    def test_vehicle_type_contradiction_raises(self, fake_vehicle_types) -> None:
        session = _FakeSession()
        registry = RotationRegistry()
        prototypes = {11: _FakePrototype(), 21: _FakePrototype()}
        fu = [(1, "16.06.2025", "106/2")]

        create_trips_and_vehicle_schedules(
            _schedule(_fahrzeugumlauf(fu, {1: [11]}, fahrzeugtyp="GEG")), prototypes, 1, session, registry
        )
        with pytest.raises(ValueError, match="vehicle type"):
            create_trips_and_vehicle_schedules(
                _schedule(_fahrzeugumlauf(fu, {1: [21]}, fahrzeugtyp="EED")), prototypes, 1, session, registry
            )

    def test_betriebshof_contradiction_raises(self, fake_vehicle_types) -> None:
        session = _FakeSession()
        registry = RotationRegistry()
        prototypes = {11: _FakePrototype(), 21: _FakePrototype()}
        fu = [(1, "16.06.2025", "106/2")]

        create_trips_and_vehicle_schedules(
            _schedule(_fahrzeugumlauf(fu, {1: [11]}, betriebshof=9424)), prototypes, 1, session, registry
        )
        with pytest.raises(ValueError, match="Betriebshof"):
            create_trips_and_vehicle_schedules(
                _schedule(_fahrzeugumlauf(fu, {1: [21]}, betriebshof=9430)), prototypes, 1, session, registry
            )


def _rotation_fake(name: str, start_station: str, end_station: str, n_trips: int = 2) -> SimpleNamespace:
    base = datetime(2025, 6, 16, 8, 0)
    trips = []
    for i in range(n_trips):
        dep_station = start_station if i == 0 else "Mid"
        arr_station = end_station if i == n_trips - 1 else "Mid"
        trips.append(
            SimpleNamespace(
                route=SimpleNamespace(
                    departure_station=SimpleNamespace(name=dep_station),
                    arrival_station=SimpleNamespace(name=arr_station),
                ),
                departure_time=base + timedelta(hours=i),
                arrival_time=base + timedelta(hours=i, minutes=30),
                stop_times=[SimpleNamespace()],
            )
        )
    return SimpleNamespace(name=name, id=1, trips=trips)


class TestDeleteIncompleteRotations:
    def test_depot_to_depot_kept(self) -> None:
        rotation = _rotation_fake("100/1", "Betriebshof Cicerostr. Einsetzen", "Betriebshof Cicerostr. Aussetzen")
        session = _FakeSession(rotations=[rotation])
        delete_incomplete_rotations(1, session)
        assert session.deleted == []

    def test_cross_depot_working_kept(self) -> None:
        # Real workings park overnight on a satellite Abstellflaeche: the endpoints are both
        # depots but NOT the same station. These must survive.
        rotation = _rotation_fake("194/21", "Betriebshof Lichtenberg Einsetzen", "Kaulsdorf Abstellfläche Aussetzen")
        session = _FakeSession(rotations=[rotation])
        delete_incomplete_rotations(1, session)
        assert session.deleted == []

    def test_window_edge_fragment_deleted_with_trips(self) -> None:
        rotation = _rotation_fake("N02/71", "S+U Hauptbahnhof", "Betriebshof Indira-Gandhi-Str. Aussetzen")
        session = _FakeSession(rotations=[rotation])
        delete_incomplete_rotations(1, session)
        assert rotation in session.deleted
        for trip in rotation.trips:
            assert trip in session.deleted
            for stop_time in trip.stop_times:
                assert stop_time in session.deleted

    def test_rotation_without_trips_deleted(self) -> None:
        rotation = SimpleNamespace(name="ghost", id=2, trips=[])
        session = _FakeSession(rotations=[rotation])
        delete_incomplete_rotations(1, session)
        assert session.deleted == [rotation]
