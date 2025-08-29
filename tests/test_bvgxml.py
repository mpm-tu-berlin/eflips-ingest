import glob
import os
from pathlib import Path
from typing import List, Dict

import eflips.model
import pytest
from eflips.model import create_engine
from sqlalchemy.orm import Session

from eflips.ingest.legacy.bvgxml import (
    load_and_validate_xml,
    create_stations,
    setup_working_dictionaries,
    create_routes_and_time_profiles,
    create_trip_prototypes,
    TimeProfile,
    create_trips_and_vehicle_schedules,
    recenter_station,
)
from eflips.ingest.legacy.xmldata import Linienfahrplan


class TestBVGXML:
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

    @pytest.mark.skipif(
        not os.getenv("OPENELEVATION_URL"),
        reason="OPENELEVATION_URL not set",
    )
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

    @pytest.mark.skipif(
        not os.getenv("OPENELEVATION_URL"),
        reason="OPENELEVATION_URL not set",
    )
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

    @pytest.mark.skipif(
        not os.getenv("OPENELEVATION_URL"),
        reason="OPENELEVATION_URL not set",
    )
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

    @pytest.mark.skipif(
        not os.getenv("OPENELEVATION_URL"),
        reason="OPENELEVATION_URL not set",
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
