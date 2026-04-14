import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo
import os.path
from pathlib import Path
from uuid import UUID

import pytest

from eflips.ingest.gtfs import GtfsIngester
from tests.base import BaseIngester
import gtfs_kit as gk

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
