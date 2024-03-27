from typing import Dict
from uuid import UUID

import pytest

from eflips.ingest.base import AbstractIngester
from eflips.ingest.dummy import DummyIngester, BusType
from tests.test_base import TestBaseIngester


class TestDummyIngester(TestBaseIngester):
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
        )

        result: bool = ret_val[0]
        assert isinstance(result, bool)
        assert result is True

        uuid: UUID | Dict[str, str] = ret_val[1]
        assert isinstance(uuid, UUID)

        # This case should fail
        ret_val = ingester.prepare(
            name=name,
            depot_count="one",
            line_count=line_count,
            rotation_per_line=rotation_per_line,
            opportunity_charging=opportunity_charging,
            bus_type=bus_type,
        )
        result = ret_val[0]
        assert isinstance(result, bool)
        assert result is False

        errors: Dict[str, str] = ret_val[1]
        assert isinstance(errors, dict)
        assert "Wrong Depot Count" in errors

        # In reality, it would be better to check all error messages here

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
        )

        assert ret_val[0] is True
        uuid: UUID | Dict[str, str] = ret_val[1]

        # Create a new ingester -- imporing should not rely on maintaining state in the ingester
        # Only in the data file identified by the UUID

        ingester = DummyIngester(self.database_url)
        ingester.ingest(uuid)

    @pytest.fixture()
    def ingester(self) -> AbstractIngester:
        return DummyIngester(self.database_url)
