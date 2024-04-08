from typing import Dict
from uuid import UUID

import pytest
from eflips.model import Scenario
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from eflips.ingest.base import AbstractIngester
from eflips.ingest.dummy import DummyIngester, BusType
from tests.base import BaseIngester


class TestDummyIngester(BaseIngester):
    def test_prepare(self, ingester) -> None:
        name = "Entenhausen"
        depot_count = 5
        line_count = 3
        rotation_per_line = 4
        opportunity_charging = True
        bus_type = BusType.ARTICULATED

        # This case should work
        ret_val = ingester.prepare(
            name=name,
            depot_count=depot_count,
            line_count=line_count,
            rotation_per_line=rotation_per_line,
            opportunity_charging=opportunity_charging,
            bus_type=bus_type,
            progress_callback=self.progress_callback,
        )

        result: bool = ret_val[0]
        assert isinstance(result, bool)
        assert result is True

        uuid: UUID | Dict[str, str] = ret_val[1]
        assert isinstance(uuid, UUID)

    def test_prepare_incorrect(self, ingester) -> None:
        name = "Entenhausen"
        depot_count = 5
        line_count = 3
        rotation_per_line = 4
        opportunity_charging = True
        bus_type = BusType.ARTICULATED

        # This case should fail
        ret_val = ingester.prepare(
            name=name,
            depot_count="one",
            line_count=line_count,
            rotation_per_line=rotation_per_line,
            opportunity_charging=opportunity_charging,
            bus_type=bus_type,
            progress_callback=self.progress_callback,
        )
        result = ret_val[0]
        assert isinstance(result, bool)
        assert result is False

        errors: Dict[str, str] = ret_val[1]
        assert isinstance(errors, dict)
        assert "Wrong Depot Count" in errors

        # In reality, it would be better to check all error messages here

    @staticmethod
    def progress_callback(progress: float) -> None:
        assert 0 <= progress <= 1

    def test_ingest(self, ingester) -> None:
        name = "Entenhausen"
        depot_count = 5
        line_count = 3
        rotation_per_line = 4
        opportunity_charging = True
        bus_type = BusType.ARTICULATED

        # This case should work
        ret_val = ingester.prepare(
            name=name,
            depot_count=depot_count,
            line_count=line_count,
            rotation_per_line=rotation_per_line,
            opportunity_charging=opportunity_charging,
            bus_type=bus_type,
            progress_callback=self.progress_callback,
        )

        assert ret_val[0] is True
        uuid: UUID | Dict[str, str] = ret_val[1]

        # Create a new ingester -- imporing should not rely on maintaining state in the ingester
        # Only in the data file identified by the UUID

        ingester = DummyIngester(self.database_url)
        ingester.ingest(uuid)

    def test_ingest_existing_scenario(self, ingester) -> None:
        engine = create_engine(ingester.database_url)
        with Session(engine) as session:
            scenario = Scenario(
                name="Test Scenario",
            )

            name = "Entenhausen"
            depot_count = 5
            line_count = 3
            rotation_per_line = 4
            opportunity_charging = True
            bus_type = BusType.ARTICULATED

            # This case should work
            ret_val = ingester.prepare(
                name=name,
                depot_count=depot_count,
                line_count=line_count,
                rotation_per_line=rotation_per_line,
                opportunity_charging=opportunity_charging,
                bus_type=bus_type,
                progress_callback=self.progress_callback,
            )

            assert ret_val[0] is True
            uuid: UUID | Dict[str, str] = ret_val[1]
            scenario.task_id = uuid

            session.add(scenario)
            session.commit()

            # Create a new ingester -- imporing should not rely on maintaining state in the ingester
            # Only in the data file identified by the UUID

            ingester = DummyIngester(self.database_url)
            ingester.ingest(uuid)

            # Check if the scenario now has rotations
            assert scenario.rotations is not None
            assert len(scenario.rotations) == line_count * rotation_per_line * depot_count

    @pytest.fixture()
    def ingester(self) -> AbstractIngester:
        return DummyIngester(self.database_url)
