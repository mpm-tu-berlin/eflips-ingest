import pytest

from eflips.ingest.util import altitude_map, soldner_to_latlon, soldner_to_pointz, soldner_to_pointz_many
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

    def test_soldner_to_latlon(self):
        lat, lon = soldner_to_latlon(16522000, 29765400)
        assert abs(lat - 52.59436500848306) < 1e-9
        assert abs(lon - 13.278952671184285) < 1e-9

    def test_soldner_to_pointz_many_looks_up_once(self, monkeypatch):
        calls = []

        def fake_get_altitudes(latlons):
            calls.append(list(latlons))
            return [float(index) for index in range(len(latlons))]

        monkeypatch.delenv("ELEVATION_DUMMY_MODE")
        monkeypatch.setattr("eflips.ingest.util.get_altitudes", fake_get_altitudes)
        xys = [(16522000, 29765400), (16600000, 29800000), (16522000, 29765400)]
        geoms = soldner_to_pointz_many(xys)

        assert set(geoms) == {(16522000, 29765400), (16600000, 29800000)}
        assert len(calls) == 1 and len(calls[0]) == 2
        if geometry_has_z():
            assert geoms[(16522000, 29765400)].endswith(" 0.0)")
            assert geoms[(16600000, 29800000)].endswith(" 1.0)")
        assert soldner_to_pointz(16522000, 29765400).startswith("SRID=4326;POINT")

    def test_altitude_map_dedupes_and_keeps_pairs(self, monkeypatch):
        calls = []

        def fake_get_altitudes(latlons):
            calls.append(list(latlons))
            return [10.0 * (index + 1) for index in range(len(latlons))]

        monkeypatch.delenv("ELEVATION_DUMMY_MODE")
        monkeypatch.setattr("eflips.ingest.util.get_altitudes", fake_get_altitudes)
        result = altitude_map([(52.5, 13.4), (48.1, 11.6), (52.5, 13.4)])
        if geometry_has_z():
            assert result == {(52.5, 13.4): 10.0, (48.1, 11.6): 20.0}
            assert calls == [[(52.5, 13.4), (48.1, 11.6)]]
        else:
            assert result == {} and calls == []

    def test_altitude_map_empty(self, monkeypatch):
        monkeypatch.delenv("ELEVATION_DUMMY_MODE")
        assert altitude_map([]) == {}
