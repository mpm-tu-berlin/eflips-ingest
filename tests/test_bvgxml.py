import glob
import os
from datetime import date
from pathlib import Path

import pytest
import eflips.model
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from eflips.ingest.bvgxml import import_line, load_and_validate_xml, preprocess_bvgxml
from eflips.ingest.xmldata import Linienfahrplan


class TestBVGXML:
    @pytest.fixture
    def xml_path(self):
        current_file_path = os.path.dirname(os.path.abspath(__file__))
        xml_path = os.path.join(
            current_file_path,
            "..",
            "samples",
            "BVGXML",
            "Linienfahrplan_10_05.07.2023_125_0_N_N.response.xml",
        )
        return Path(xml_path)

    def test_bvgxml_load_and_validate(self, xml_path):
        data = load_and_validate_xml(xml_path)
        assert data is not None
        assert isinstance(data, Linienfahrplan)

    def test_bvgxml_preoprocess(self, xml_path):
        data = load_and_validate_xml(xml_path)
        result = preprocess_bvgxml(data)
        assert result.line_name == "125"
        assert result.date == date(2023, 7, 5)

    @pytest.mark.skipif(
        not os.getenv("DATABASE_URL"),
        reason="DATABASE_URL not set",
    )
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
    def test_import_line(self):
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
        for path in glob.glob(
            "/home/ludger/Downloads/passengerCount_BO_2023-07-03/*.xml"
        ):
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
