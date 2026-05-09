import logging
import os.path
from datetime import datetime, timedelta, timezone
from pathlib import Path
from uuid import UUID
from zoneinfo import ZoneInfo

import gtfs_kit as gk
import pytest

from eflips.ingest.gtfs import GtfsIngester
from tests.base import BaseIngester
from tests.conftest import mock_get_altitude


class TestGtfsIngester(BaseIngester):
    """Tests for the GTFS ingester.

    These tests focus on:
    - SWU.zip: Medium-sized single-agency feed for comprehensive testing
    - sample-feed-1.zip: Minimal GTFS feed for basic functionality
    - VBB.zip: Multi-agency feed used ONLY for agency filtering tests (DAY mode only)
    """

    @pytest.fixture(autouse=True)
    def setup_altitude_mock(self, monkeypatch) -> None:
        """Automatically mock altitude lookups for all tests in this class."""
        monkeypatch.setattr(
            "eflips.ingest.util.get_altitude",
            mock_get_altitude,
        )
        monkeypatch.setattr(
            "eflips.ingest.util.get_altitude_google",
            mock_get_altitude,
        )
        monkeypatch.setattr(
            "eflips.ingest.util.get_altitude_openelevation",
            mock_get_altitude,
        )

    @pytest.fixture()
    def ingester(self) -> GtfsIngester:
        """Create a GTFS ingester instance."""
        return GtfsIngester(self.database_url)

    @pytest.fixture()
    def swu_feed(self) -> Path:
        """Path to the SWU GTFS feed (single-agency, comprehensive)."""
        path_of_this_file = Path(os.path.dirname(__file__))
        sample_data_directory = path_of_this_file / ".." / "samples" / "GTFS"
        swu_path = sample_data_directory / "SWU.zip"
        if not swu_path.exists():
            raise FileNotFoundError(f"SWU.zip not found at {swu_path}")
        return swu_path.resolve()

    @pytest.fixture()
    def sample_feed_1(self) -> Path:
        """Path to sample-feed-1.zip (minimal GTFS feed for basic tests)."""
        path_of_this_file = Path(os.path.dirname(__file__))
        sample_data_directory = path_of_this_file / ".." / "samples" / "GTFS"
        sample_path = sample_data_directory / "sample-feed-1.zip"
        if not sample_path.exists():
            raise FileNotFoundError(f"sample-feed-1.zip not found at {sample_path}")
        return sample_path.resolve()

    @pytest.fixture()
    def multi_agency_feed_zip(self, sample_feed_1, tmp_path) -> Path:
        """Multi-agency GTFS zip built in-memory from sample-feed-1.

        The source feed has a single agency; we triplicate the agency row with
        distinct ids/names and round-robin distribute the routes across them
        so each agency owns at least one route. All trip/stop_times/calendar
        rows keep their original route_ids, so downstream filtering still
        works.
        """
        feed = gk.read_feed(sample_feed_1, dist_units="m")
        base_row = feed.agency.iloc[0].to_dict()
        new_agencies = []
        for aid, aname in [
            ("A1", "Agency One"),
            ("A2", "Agency Two"),
            ("A3", "Agency Three"),
        ]:
            row = dict(base_row)
            row["agency_id"] = aid
            row["agency_name"] = aname
            new_agencies.append(row)
        import pandas as pd

        feed.agency = pd.DataFrame(new_agencies)

        routes = feed.routes.reset_index(drop=True).copy()
        assigned_ids = [["A1", "A2", "A3"][i % 3] for i in range(len(routes))]
        routes["agency_id"] = assigned_ids
        feed.routes = routes

        if feed.stops is not None and "parent_station" not in feed.stops.columns:
            feed.stops["parent_station"] = pd.NA

        out_path = tmp_path / "multi_agency.zip"
        feed.to_file(out_path)
        return out_path

    @pytest.fixture()
    def vbb_feed(self) -> Path:
        """Path to VBB GTFS feed (multi-agency, used ONLY for agency filtering tests)."""
        path_of_this_file = Path(os.path.dirname(__file__))
        sample_data_directory = path_of_this_file / ".." / "samples" / "GTFS"
        vbb_path = sample_data_directory / "VBB.zip"
        if not vbb_path.exists():
            raise FileNotFoundError(f"VBB.zip not found at {vbb_path}")
        return vbb_path.resolve()

    # ====================
    # Required Abstract Methods
    # ====================

    def test_prepare(self, ingester, swu_feed) -> None:
        """
        Test the prepare method (required by BaseIngester).
        Tests both success and failure cases.
        """
        feed = gk.read_feed(swu_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)

        # Test success case
        success, result = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=swu_feed,
            start_date=start_date.isoformat(),
            duration="DAY",
        )
        assert success
        assert isinstance(result, UUID)

        # Test failure case (invalid date range)
        end_date = datetime.strptime(validity[1], "%Y%m%d").date()
        success, error_dict = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=swu_feed,
            start_date=end_date.isoformat(),
            duration="WEEK",
        )
        assert not success
        assert isinstance(error_dict, dict)
        assert "date_range" in error_dict

    def test_ingest(self, ingester, swu_feed) -> None:
        """
        Test the ingest method (required by BaseIngester).
        Creates a new ingester to test that UUID-based ingestion works.
        """
        feed = gk.read_feed(swu_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)

        # Prepare with first ingester
        success, uuid = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=swu_feed,
            start_date=start_date.isoformat(),
            duration="DAY",
        )
        assert success
        assert isinstance(uuid, UUID)

        # Create new ingester and ingest using the UUID
        new_ingester = GtfsIngester(self.database_url)
        new_ingester.ingest(uuid)

    # ====================
    # Basic Preparation Tests
    # ====================

    def test_prepare_sample_feed_1(self, ingester, sample_feed_1) -> None:
        """Test that we can prepare the minimal sample-feed-1.zip."""
        feed = gk.read_feed(sample_feed_1, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()

        success, result = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=sample_feed_1,
            start_date=start_date.isoformat(),
            duration="DAY",
            bus_only=False,
        )
        assert success
        assert isinstance(result, UUID)

    def test_prepare_sample_feed_1_bus_only(self, ingester, sample_feed_1) -> None:
        """Test that sample-feed-1.zip works with bus_only=True despite missing parent_station column."""
        feed = gk.read_feed(sample_feed_1, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()

        success, result = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=sample_feed_1,
            start_date=start_date.isoformat(),
            duration="DAY",
            bus_only=True,
        )
        assert success
        assert isinstance(result, UUID)

    def test_prepare_swu_feed(self, ingester, swu_feed) -> None:
        """Test that we can prepare the SWU feed."""
        feed = gk.read_feed(swu_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)

        success, result = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=swu_feed,
            start_date=start_date.isoformat(),
            duration="DAY",
        )
        assert success
        assert isinstance(result, UUID)

    # ====================
    # Full Ingestion Tests
    # ====================

    def test_ingest_sample_feed_1_day(self, ingester, sample_feed_1) -> None:
        """Test complete ingestion of sample-feed-1.zip (DAY mode)."""
        feed = gk.read_feed(sample_feed_1, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()

        success, uuid = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=sample_feed_1,
            start_date=start_date.isoformat(),
            duration="DAY",
            bus_only=False,
        )
        assert success
        assert isinstance(uuid, UUID)
        ingester.ingest(uuid)

    def test_ingest_swu_day(self, ingester, swu_feed) -> None:
        """Test complete ingestion of SWU feed (DAY mode)."""
        feed = gk.read_feed(swu_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)

        success, uuid = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=swu_feed,
            start_date=start_date.isoformat(),
            duration="DAY",
        )
        assert success
        assert isinstance(uuid, UUID)
        ingester.ingest(uuid)

    def test_ingest_swu_week(self, ingester, swu_feed) -> None:
        """Test complete ingestion of SWU feed (WEEK mode)."""
        feed = gk.read_feed(swu_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)
        # Start on a Monday
        while start_date.weekday() != 0:
            start_date += timedelta(days=1)

        success, uuid = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=swu_feed,
            start_date=start_date.isoformat(),
            duration="WEEK",
        )
        assert success
        assert isinstance(uuid, UUID)
        ingester.ingest(uuid)

    # ====================
    # Date Range Validation Tests
    # ====================

    def test_prepare_invalid_date_range(self, ingester, swu_feed) -> None:
        """Test that preparing with a date outside the feed validity period fails."""
        feed = gk.read_feed(swu_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        end_date = datetime.strptime(validity[1], "%Y%m%d").date()

        # Try to ingest a week starting from the last valid day (will exceed validity)
        success, failure_dict = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=swu_feed,
            start_date=end_date.isoformat(),
            duration="WEEK",
        )
        assert not success
        assert "date_range" in failure_dict

    # ====================
    # Agency Filtering Tests (using VBB in DAY mode only)
    # ====================

    def test_multi_agency_without_agency_name(self, ingester, vbb_feed) -> None:
        """Test that multi-agency feeds require agency_name parameter."""
        feed = gk.read_feed(vbb_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)

        success, error_dict = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=vbb_feed,
            start_date=start_date.isoformat(),
            duration="DAY",
        )
        assert not success
        assert "agency_name" in error_dict
        assert "agencies" in error_dict["agency_name"]

    def test_multi_agency_with_empty_agency_name(self, ingester, vbb_feed) -> None:
        """Test that multi-agency feeds reject empty agency_name."""
        feed = gk.read_feed(vbb_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)

        success, error_dict = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=vbb_feed,
            start_date=start_date.isoformat(),
            duration="DAY",
            agency_name="",
        )
        assert not success
        assert "agency_name" in error_dict

    def test_multi_agency_with_invalid_agency_name(self, ingester, vbb_feed) -> None:
        """Test that multi-agency feeds reject invalid agency_name."""
        feed = gk.read_feed(vbb_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)

        success, error_dict = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=vbb_feed,
            start_date=start_date.isoformat(),
            duration="DAY",
            agency_name="NonExistent Agency",
        )
        assert not success
        assert "agency_name" in error_dict

    def test_multi_agency_with_valid_agency_name(self, ingester, vbb_feed) -> None:
        """Test that multi-agency feeds work with valid agency name (DAY mode)."""
        feed = gk.read_feed(vbb_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)

        success, result = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=vbb_feed,
            start_date=start_date.isoformat(),
            duration="DAY",
            agency_name="S-Bahn Berlin GmbH",
        )
        assert success
        assert isinstance(result, UUID)

    # ====================
    # Single-Agency Edge Cases
    # ====================

    def test_single_agency_ignores_agency_name(self, ingester, swu_feed) -> None:
        """Test that single-agency feeds ignore the agency_name parameter."""
        feed = gk.read_feed(swu_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)

        # Should succeed even with wrong agency_name since SWU is single-agency
        success, result = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=swu_feed,
            start_date=start_date.isoformat(),
            duration="DAY",
            agency_name="NonExistent Agency",
        )
        assert success
        assert isinstance(result, UUID)

    # ====================
    # Multi-Agency Selector Tests (list/iterable of names or ids)
    # ====================

    def test_multi_agency_list_of_names(self, ingester, multi_agency_feed_zip) -> None:
        feed = gk.read_feed(multi_agency_feed_zip, dist_units="m")
        result = ingester.filter_feed_by_agency(feed, agency_name=["Agency One", "Agency Two"])
        assert isinstance(result, gk.Feed)
        assert set(result.agency["agency_name"]) == {"Agency One", "Agency Two"}
        assert set(result.routes["agency_id"]) <= {"A1", "A2"}

    def test_multi_agency_list_of_ids(self, ingester, multi_agency_feed_zip) -> None:
        feed = gk.read_feed(multi_agency_feed_zip, dist_units="m")
        result = ingester.filter_feed_by_agency(feed, agency_id=["A1", "A3"])
        assert isinstance(result, gk.Feed)
        assert set(result.agency["agency_id"].astype(str)) == {"A1", "A3"}

    def test_multi_agency_mixed_names_and_ids(self, ingester, multi_agency_feed_zip) -> None:
        feed = gk.read_feed(multi_agency_feed_zip, dist_units="m")
        result = ingester.filter_feed_by_agency(feed, agency_name=["Agency One"], agency_id=["A2"])
        assert isinstance(result, gk.Feed)
        assert set(result.agency["agency_id"].astype(str)) == {"A1", "A2"}

    def test_multi_agency_single_string_still_works(self, ingester, multi_agency_feed_zip) -> None:
        feed = gk.read_feed(multi_agency_feed_zip, dist_units="m")
        result = ingester.filter_feed_by_agency(feed, agency_name="Agency One")
        assert isinstance(result, gk.Feed)
        assert list(result.agency["agency_name"]) == ["Agency One"]

    def test_multi_agency_partial_miss_reports_error(self, ingester, multi_agency_feed_zip) -> None:
        feed = gk.read_feed(multi_agency_feed_zip, dist_units="m")
        result = ingester.filter_feed_by_agency(feed, agency_name=["Agency One", "Nope"])
        assert isinstance(result, tuple)
        success, error_dict = result
        assert success is False
        assert "agency_name" in error_dict
        assert "Nope" in error_dict["agency_name"]

    def test_prepare_multi_agency_list_scenario_name(self, ingester, multi_agency_feed_zip) -> None:
        feed = gk.read_feed(multi_agency_feed_zip, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()

        success, result = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=multi_agency_feed_zip,
            start_date=start_date.isoformat(),
            duration="DAY",
            agency_name=["Agency One", "Agency Two"],
            bus_only=False,
        )
        assert success, result
        assert isinstance(result, UUID)

        import pickle

        save_path = ingester.path_for_uuid(result)
        with open(save_path / "gtfs_data.dill", "rb") as f:
            data = pickle.load(f)
        assert data["agency_name"] == "Agency One / Agency Two"

    # ====================
    # Route ID Selector Tests (filter_feed_by_route_ids)
    # ====================

    ALL_SAMPLE_FEED_1_ROUTES = {"AB", "BFC", "STBA", "CITY", "AAMV"}

    @staticmethod
    def _read_sample_feed_1_with_parent_station(sample_feed_1_path):
        """Load sample-feed-1 and add an empty parent_station column.

        Mirrors the pre-processing that ``GtfsIngester.prepare`` performs
        (gtfs.py:160-161). gtfs_kit's ``restrict_to_routes`` raises KeyError on
        'parent_station' when the column is absent, so direct-method tests
        must add it the same way ``prepare`` does.
        """
        import pandas as pd

        feed = gk.read_feed(sample_feed_1_path, dist_units="m")
        if feed.stops is not None and "parent_station" not in feed.stops.columns:
            feed.stops["parent_station"] = pd.NA
        return feed

    def test_route_ids_single_string(self, ingester, sample_feed_1) -> None:
        feed = self._read_sample_feed_1_with_parent_station(sample_feed_1)
        result = ingester.filter_feed_by_route_ids(feed, "AB")
        assert isinstance(result, gk.Feed)
        assert set(result.routes["route_id"].astype(str)) == {"AB"}
        # Cascading: trips should only reference the retained route.
        assert set(result.trips["route_id"].astype(str)) == {"AB"}

    def test_route_ids_list(self, ingester, sample_feed_1) -> None:
        feed = self._read_sample_feed_1_with_parent_station(sample_feed_1)
        result = ingester.filter_feed_by_route_ids(feed, ["AB", "CITY"])
        assert isinstance(result, gk.Feed)
        assert set(result.routes["route_id"].astype(str)) == {"AB", "CITY"}

    def test_route_ids_none_is_noop(self, ingester, sample_feed_1) -> None:
        feed = gk.read_feed(sample_feed_1, dist_units="m")
        result = ingester.filter_feed_by_route_ids(feed, None)
        assert isinstance(result, gk.Feed)
        assert set(result.routes["route_id"].astype(str)) == self.ALL_SAMPLE_FEED_1_ROUTES

    def test_route_ids_empty_string_is_noop(self, ingester, sample_feed_1) -> None:
        feed = gk.read_feed(sample_feed_1, dist_units="m")
        result = ingester.filter_feed_by_route_ids(feed, "")
        assert isinstance(result, gk.Feed)
        assert set(result.routes["route_id"].astype(str)) == self.ALL_SAMPLE_FEED_1_ROUTES

    def test_route_ids_empty_list_is_noop(self, ingester, sample_feed_1) -> None:
        feed = gk.read_feed(sample_feed_1, dist_units="m")
        result = ingester.filter_feed_by_route_ids(feed, [])
        assert isinstance(result, gk.Feed)
        assert set(result.routes["route_id"].astype(str)) == self.ALL_SAMPLE_FEED_1_ROUTES

    def test_route_ids_missing_reports_error(self, ingester, sample_feed_1) -> None:
        feed = gk.read_feed(sample_feed_1, dist_units="m")
        result = ingester.filter_feed_by_route_ids(feed, ["AB", "NOPE"])
        assert isinstance(result, tuple)
        success, error_dict = result
        assert success is False
        assert "route_ids" in error_dict
        message = error_dict["route_ids"]
        assert "NOPE" in message
        # Only "NOPE" is missing; "AB" must not appear in the missing-ids list.
        missing_line = message.splitlines()[0]
        assert "AB" not in missing_line

    def test_prepare_with_route_ids(self, ingester, sample_feed_1) -> None:
        feed = gk.read_feed(sample_feed_1, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()

        success, result = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=sample_feed_1,
            start_date=start_date.isoformat(),
            duration="DAY",
            route_ids=["AB", "STBA"],
        )
        assert success, result
        assert isinstance(result, UUID)

        import pickle

        save_path = ingester.path_for_uuid(result)
        with open(save_path / "gtfs_data.dill", "rb") as f:
            data = pickle.load(f)
        saved_feed = data["feed"]
        assert set(saved_feed.routes["route_id"].astype(str)) == {"AB", "STBA"}

    # ====================
    # _coerce_str_list Unit Test
    # ====================

    def test_coerce_str_list(self) -> None:
        coerce = GtfsIngester._coerce_str_list
        assert coerce(None) == []
        assert coerce("") == []
        assert coerce("x") == ["x"]
        assert coerce(["a", "", "b"]) == ["a", "b"]
        assert coerce(("a", None, "b")) == ["a", "b"]

    # ====================
    # AssocRouteStation.elapsed_distance Tests
    # ====================

    def test_elapsed_distance_is_not_uniform_swu(self, ingester, swu_feed) -> None:
        """Regression test for the uniform-elapsed-distance bug.

        Before the fix, every route in the SWU feed produced perfectly equal
        inter-stop spacing because (a) SWU's stops use ``parent_station``,
        which broke the per-stop ``shape_dist_traveled`` lookup, and (b) the
        fallback distributed distance evenly with no warning. This test
        ingests SWU and asserts that at least one multi-stop route has
        meaningful variance in its inter-stop distances.
        """
        from sqlalchemy.orm import Session
        from eflips.model import AssocRouteStation, Route, create_engine

        feed = gk.read_feed(swu_feed, dist_units="m")
        validity = ingester.get_feed_validity_period(feed)
        start_date = datetime.strptime(validity[0], "%Y%m%d").date()
        start_date += timedelta(days=7)

        success, uuid = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=swu_feed,
            start_date=start_date.isoformat(),
            duration="DAY",
        )
        assert success
        assert isinstance(uuid, UUID)
        ingester.ingest(uuid)

        engine = create_engine(self.database_url)
        non_uniform_routes = 0
        with Session(engine) as session:
            routes = session.query(Route).all()
            assert len(routes) > 0, "expected at least one route"
            for route in routes:
                assocs = (
                    session.query(AssocRouteStation)
                    .filter(AssocRouteStation.route == route)
                    .order_by(AssocRouteStation.elapsed_distance)
                    .all()
                )
                if len(assocs) < 3:
                    continue
                segments = [assocs[i].elapsed_distance - assocs[i - 1].elapsed_distance for i in range(1, len(assocs))]
                # Uniform distribution → all segments identical (variance == 0).
                spread = max(segments) - min(segments)
                if spread > 1.0:
                    non_uniform_routes += 1

        assert non_uniform_routes > 0, (
            "every SWU route had uniformly-spaced elapsed_distance values; the "
            "shape_dist_traveled lookup is silently falling back to uniform "
            "distribution again."
        )


class TestParseGtfsTime:
    """Unit tests for ``GtfsIngester.parse_gtfs_time``.

    These tests pin the spec-literal "noon minus 12h" interpretation of GTFS
    times, which is what protects DST transition days from producing
    non-monotonic UTC instants. The function is a ``@staticmethod`` so the
    tests do not need any database or ingester fixtures.

    Each assertion compares ``result.astimezone(timezone.utc)`` rather than
    the wall-clock representation. Comparing two aware datetimes that share a
    ``tzinfo`` instance falls into Python's same-tzinfo fast path, which
    operates on naive wall-clock components and ignores per-instance offsets
    -- exactly the trap that hid the original bug from the in-memory
    ``sorted(...)`` ordering check.
    """

    BERLIN = ZoneInfo("Europe/Berlin")

    def test_normal_day(self) -> None:
        """On a non-DST day, GTFS HH:MM:SS should match local wall clock."""
        base_date = datetime(2026, 3, 28, tzinfo=self.BERLIN)
        result = GtfsIngester.parse_gtfs_time("14:30:00", base_date)

        assert result.astimezone(timezone.utc) == datetime(2026, 3, 28, 13, 30, tzinfo=timezone.utc)

    def test_spring_forward_day_brackets_dst_gap(self) -> None:
        """Regression test for the original bug.

        On 2026-03-29 in Europe/Berlin the local clock jumps from 02:00 CET
        directly to 03:00 CEST. A trip with stops at "2:55:00" and "3:19:00"
        previously produced an arrival before the departure in UTC, tripping
        the Trip CHECK constraint. After the fix, both times must be
        monotonic in UTC and equal the spec's "noon minus 12h" anchor +
        offset.
        """
        base_date = datetime(2026, 3, 29, tzinfo=self.BERLIN)

        early = GtfsIngester.parse_gtfs_time("2:55:00", base_date)
        late = GtfsIngester.parse_gtfs_time("3:19:00", base_date)

        # Spec: noon = 12:00 CEST = 10:00 UTC, anchor = 22:00 UTC the day
        # before. early = anchor + 2h55m = 00:55 UTC, late = anchor + 3h19m
        # = 01:19 UTC.
        assert early.astimezone(timezone.utc) == datetime(2026, 3, 29, 0, 55, tzinfo=timezone.utc)
        assert late.astimezone(timezone.utc) == datetime(2026, 3, 29, 1, 19, tzinfo=timezone.utc)

        # The actual constraint that the SQL CHECK enforces.
        assert early.astimezone(timezone.utc) < late.astimezone(timezone.utc)

    def test_fall_back_day(self) -> None:
        """On 2025-10-26 in Europe/Berlin the clock falls back at 03:00 CEST.

        Noon is 12:00 CET = 11:00 UTC, so the anchor is 23:00 UTC on
        2025-10-25. "12:00:00" must still land at noon local; "01:00:00"
        must land at the *first* occurrence of 02:00 local (CEST), which is
        00:00 UTC on the service day.
        """
        base_date = datetime(2025, 10, 26, tzinfo=self.BERLIN)

        noon = GtfsIngester.parse_gtfs_time("12:00:00", base_date)
        one_am = GtfsIngester.parse_gtfs_time("01:00:00", base_date)

        assert noon.astimezone(timezone.utc) == datetime(2025, 10, 26, 11, 0, tzinfo=timezone.utc)
        assert one_am.astimezone(timezone.utc) == datetime(2025, 10, 26, 0, 0, tzinfo=timezone.utc)

    def test_overflow_past_midnight(self) -> None:
        """GTFS times can exceed 24:00:00 for trips that span midnight."""
        base_date = datetime(2026, 3, 28, tzinfo=self.BERLIN)

        result = GtfsIngester.parse_gtfs_time("25:35:00", base_date)

        # 2026-03-28 is a normal day, anchor = local midnight = 23:00 UTC
        # the previous day. + 25h35m = 00:35 UTC on 2026-03-29.
        assert result.astimezone(timezone.utc) == datetime(2026, 3, 29, 0, 35, tzinfo=timezone.utc)

    def test_naive_base_date_falls_back_and_warns(self, caplog) -> None:
        """A naive ``base_date`` is a caller bug; we still produce a result
        via direct timedelta addition, but we log a WARNING so it gets
        noticed."""
        naive_base = datetime(2026, 3, 28)

        with caplog.at_level(logging.WARNING, logger="eflips.ingest.gtfs"):
            result = GtfsIngester.parse_gtfs_time("14:30:00", naive_base)

        assert result == datetime(2026, 3, 28, 14, 30)
        assert result.tzinfo is None

        warnings = [r for r in caplog.records if r.levelno == logging.WARNING]
        assert warnings, "expected a WARNING for naive base_date"
        assert any("naive base_date" in r.getMessage() for r in warnings)
