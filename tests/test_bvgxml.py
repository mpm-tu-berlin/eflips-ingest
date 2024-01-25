import glob
import os
from datetime import date
from pathlib import Path
from typing import List

import pytest
import eflips.model
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from eflips.ingest.bvgxml import (
    import_line,
    load_and_validate_xml,
    create_stations,
    setup_working_dictionaries,
    create_routes_and_time_profiles,
)
from eflips.ingest.xmldata import Linienfahrplan


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

    @pytest.mark.skipif(
        not os.getenv("DATABASE_URL"),
        reason="DATABASE_URL not set",
    )
    def test_create_stations(self, linienfahrplan):
        engine = create_engine(os.environ["DATABASE_URL"])
        eflips.model.Base.metadata.drop_all(engine)
        eflips.model.Base.metadata.create_all(engine)

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
        eflips.model.Base.metadata.create_all(engine)

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
        not os.getenv("DATABASE_URL"),
        reason="DATABASE_URL not set",
    )
    @pytest.mark.skip
    def test_import_line(self, xml_path):
        data = load_and_validate_xml(xml_path)
        preprocess_result = preprocess_bvgxml(data)

        assert len(preprocess_result.dates) == 1

        # Assemble an input dict for the import function
        date = preprocess_result.dates.pop()
        input_dict = {date: data}

        engine = create_engine(os.environ["DATABASE_URL"])
        eflips.model.Base.metadata.drop_all(engine)
        eflips.model.Base.metadata.create_all(engine)

        with Session(engine) as session:
            scenario = eflips.model.Scenario(
                name="Test Scenario",
            )
            session.add(scenario)
            session.commit()
            scenario_id = scenario.id

        # Import the data
        import_line(scenario_id, input_dict, os.environ["DATABASE_URL"])

    @pytest.mark.skipif(
        not os.getenv("DATABASE_URL"),
        reason="DATABASE_URL not set",
    )
    @pytest.mark.skip
    def test_impoort_line(self):
        engine = create_engine(os.environ["DATABASE_URL"])
        eflips.model.Base.metadata.drop_all(engine)
        eflips.model.Base.metadata.create_all(engine)

        with Session(engine) as session:
            scenario = eflips.model.Scenario(
                name="Test Scenario",
            )
            session.add(scenario)
            session.commit()
            scenario_id = scenario.id

        # TODO: Remove this. it is just for testing
        for path in glob.glob("/home/ludger/Downloads/passengerCount_BO_2023-07-03/*.xml"):
            print(path)
            xml_path = Path(path)
            data = load_and_validate_xml(xml_path)
            preprocess_result = preprocess_bvgxml(data)

            # assert len(preprocess_result.dates) == 1

            # Assemble an input dict for the import function
            date = preprocess_result.dates.pop()
            input_dict = {date: data}

            # Import the data
            import_line(scenario_id, input_dict, os.environ["DATABASE_URL"])
