from datetime import datetime, timedelta
import os.path
from pathlib import Path
from uuid import UUID

import eflips.model
import pytest
from eflips.model import create_engine
from sqlalchemy.orm import Session

from eflips.ingest.gtfs import GtfsIngester
from tests.base import BaseIngester
import gtfs_kit as gk


class TestGtfsIngester(BaseIngester):
    """Tests for the GTFS ingester.

    These tests focus on:
    - SWU.zip: Medium-sized single-agency feed for comprehensive testing
    - sample-feed-1.zip: Minimal GTFS feed for basic functionality
    - VBB.zip: Multi-agency feed used ONLY for agency filtering tests (DAY mode only)
    """

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

        # Note: sample-feed-1 has no parent_station column, so we use bus_only=False
        # to avoid gtfs_kit filtering issues (all routes are buses anyway)
        success, result = ingester.prepare(
            progress_callback=None,
            gtfs_zip_file=sample_feed_1,
            start_date=start_date.isoformat(),
            duration="DAY",
            bus_only=False,
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

        # Note: sample-feed-1 has no parent_station column, so we use bus_only=False
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
