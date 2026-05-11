import pytest

from eflips.ingest.util import soldner_to_pointz
from eflips.model.util import geometry_has_z


class TestGeography:
    @pytest.fixture(autouse=True)
    def disable_altitude_lookups(self, monkeypatch) -> None:
        """Bypass network altitude lookups by enabling dummy mode."""
        monkeypatch.setenv("ELEVATION_DUMMY_MODE", "True")

    def test_soldner_to_pointz(self):
        wkt_str = soldner_to_pointz(16522000, 29765400)
        if geometry_has_z():
            assert wkt_str == "SRID=4326;POINTZ(13.278952671184285 52.59436500848306 9999.0)"
        else:
            assert wkt_str == "SRID=4326;POINT(13.278952671184285 52.59436500848306)"
