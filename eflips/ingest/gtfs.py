import bisect
import logging
import math
import os
import pathlib as pl
import pickle
import sys
import time
import uuid
import warnings
from dataclasses import dataclass
from datetime import date as date_type
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Dict, Callable, Iterable, Sequence, Tuple, List, Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

import gtfs_kit as gk  # type: ignore [import-untyped]
import pandas as pd
from eflips.model import (
    Station,
    Line,
    Route,
    Trip,
    StopTime,
    AssocRouteStation,
    Rotation,
    Scenario,
    VehicleType,
    TripType,
    Base,
)
from eflips.model import create_engine
from geoalchemy2.shape import from_shape, to_shape
from gtfs_kit import Feed
from pyproj import Geod
from shapely import LineString  # type: ignore [import-untyped]
from shapely.geometry import Point  # type: ignore [import-untyped]
from sqlalchemy.orm import Session

from eflips.ingest.base import AbstractIngester
from eflips.ingest.util import get_altitude, geometry_has_z

# Single shared geodesic helper. WGS84 matches the GTFS coordinate system.
_GEOD = Geod(ellps="WGS84")


class AmbiguousProjectionError(ValueError):
    """Internal signal raised by Source B when ≥ 2 stops cap at the same end.

    Raised inside the shape-projection source when two or more stops
    project to either the start or end vertex of the shape. The rescale
    that historically followed projection produced duplicate
    ``elapsed_distance`` values 1 ULP above ``route.distance``, which
    the eflips-model validator rejects. Caught by the orchestrator,
    which falls through to Source C (stops-as-line haversine) with a
    WARNING — the projection is unreliable for this route, but the
    haversine-of-stops fallback is still meaningful.
    """


# Unit-detection bands for ``stop_times.shape_dist_traveled`` values that
# are compared against an authoritative geodetic length (the shape's
# ``Route.calculate_length`` result). All bands are ±5% around the nominal
# scale factor; values inside a band are rescaled to meters and the unit
# mismatch is logged. Values outside every band are treated as
# untrustworthy and Source A falls through.
_UNIT_BAND_TOLERANCE = 0.05  # ±5%
_UNIT_BANDS: Tuple[Tuple[str, float], ...] = (
    ("meters", 1.0),
    ("kilometers", 1000.0),
    ("miles", 1609.344),
)


@dataclass(frozen=True)
class RouteDistances:
    """Self-consistent distance triple for one Route.

    Produced by :meth:`GtfsIngester._compute_route_distances` from exactly
    one source (``shape_dist_traveled``, shape projection, or
    stops-as-line haversine) so ``distance`` and ``elapsed_distances``
    cannot disagree.

    Invariants (the eflips-model validator depends on these):

    - ``distance > 0``.
    - ``len(elapsed_distances) == n_stops``.
    - ``elapsed_distances[0] == 0.0`` exactly.
    - ``elapsed_distances[-1] == distance`` exactly.
    - ``elapsed_distances`` is monotonic non-decreasing.
    """

    distance: float
    elapsed_distances: List[float]
    geom: LineString | None
    source: str  # "shape_dist_traveled" | "shape_projection" | "stops_haversine"


class GtfsIngester(AbstractIngester):
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(self.__class__.__name__)

    @staticmethod
    def get_feed_validity_period(feed: gk.Feed) -> Tuple[str, str] | None:
        """
        Get the validity period of a GTFS feed.

        Returns a tuple of (start_date, end_date) in YYYYMMDD format, or None if no calendar information exists.

        This handles both cases from the GTFS specification:
        1. calendar.txt with regular schedules (with optional calendar_dates.txt for exceptions)
        2. calendar_dates.txt only with explicit dates

        :param feed: A gtfs_kit Feed object
        :return: Tuple of (start_date_str, end_date_str) in YYYYMMDD format, or None
        """
        dates: List[Any] = []

        # Case 1: Check calendar.txt for regular schedules
        if feed.calendar is not None and not feed.calendar.empty:
            if "start_date" in feed.calendar.columns and "end_date" in feed.calendar.columns:
                calendar_start = feed.calendar["start_date"].min()
                calendar_end = feed.calendar["end_date"].max()
                if pd.notna(calendar_start):
                    dates.append(calendar_start)
                if pd.notna(calendar_end):
                    dates.append(calendar_end)

        # Case 2: Check calendar_dates.txt for explicit dates
        if feed.calendar_dates is not None and not feed.calendar_dates.empty:
            if "date" in feed.calendar_dates.columns:
                cal_dates_start = feed.calendar_dates["date"].min()
                cal_dates_end = feed.calendar_dates["date"].max()
                if pd.notna(cal_dates_start):
                    dates.append(cal_dates_start)
                if pd.notna(cal_dates_end):
                    dates.append(cal_dates_end)

        # If we found any dates, return the overall min and max
        if dates:
            return (min(dates), max(dates))
        else:
            return None

    def prepare(  # type: ignore[override]
        self,
        gtfs_zip_file: pl.Path,
        start_date: str,
        progress_callback: None | Callable[[float], None] = None,
        duration: str = "WEEK",
        agency_name: str | Iterable[str] = "",
        agency_id: str | Iterable[str] = "",
        bus_only: bool = True,
        route_ids: str | Iterable[str] | None = None,
    ) -> Tuple[bool, UUID | Dict[str, str]]:
        """
        Prepare and validate the input data for ingestion.

        The keyword arguments should be set to specific arguments when subclassing this method. Additionally, the
        :meth:`prepare_param_names` and :meth:`prepare_param_description` methods should be implemented to provide hints
        for the parameters of this method.

        This method of subclassing vilotaes the Liskov Substitution Principle, as the subclassed method has a different
        signature than the superclass method. As such, the subclass should be marked with # type: ignore.

        The types for keyword arguments should be limited to the following:
        - str: For text data.
        - int: For integer data.
        - float: For floating point data.
        - bool: For boolean data.
        - subclass of Enum: For enumerated data.
        - pathlib.Path: For file paths. *This is what should be done to express the need for uploaded files.*

        Additionally the method should accept a progress_callback parameter, which is a function that accepts a float
        value between 0 and 1. This function should be called periodically to update the progress of the ingestion
        process.

        When developing a (web) interface, it is suggested to use introspection on the parameters of this method to
        generate a form for the user to fill in. This can be done by using the :meth:`inspect.signature` method from the
        `inspect` module.

        This method should validate the input data and save its working data to a temporary location (suggested to be
        the path returned by the :meth:`path_for_uuid` method). If the data is valid, it should return a UUID
        representing the data and a boolean indicating that the data is valid. If the data is invalid, it should return
        a dictionary containing the error names and messages.

        :param gtfs_zip_file: Path to the GTFS zip file
        :param start_date: Start date for import in ISO 8601 format (YYYY-MM-DD)
        :param duration: Duration of import period ('DAY' or 'WEEK')
        :param agency_name: Name of the agency to import, or an iterable of names to combine
            (required if feed contains multiple agencies, unless ``agency_id`` is given)
        :param agency_id: Id of the agency to import, or an iterable of ids to combine
            (may be used instead of, or together with, ``agency_name``)
        :param bus_only: If True (default), only import bus routes (route_type 3 or 700-799)
        :param progress_callback: Optional callback function for progress updates
        :return: A tuple containing a boolean indicating whether the input data is valid and either a UUID or a dictionary
                 containing the error message.
        """
        if not (
            (isinstance(gtfs_zip_file, str) and os.path.isfile(gtfs_zip_file))
            or isinstance(gtfs_zip_file, Path)
            and gtfs_zip_file.is_file()
        ):
            return (False, {"gtfs_zip_file": "gtfs_zip_file parameter must be a valid path to a GTFS zip file"})

        # Validate bus_only parameter
        if not isinstance(bus_only, bool):
            return (False, {"bus_only": "bus_only parameter must be a boolean (True or False)"})

        # Read the GTFS feed
        feed = gk.read_feed(gtfs_zip_file, dist_units="m")

        # Ensure stops has a parent_station column (optional in GTFS spec, but required by gtfs_kit filtering)
        if feed.stops is not None and "parent_station" not in feed.stops.columns:
            feed.stops["parent_station"] = pd.NA

        # Filter by route_ids if provided
        if route_ids:
            route_id_filter_result = self.filter_feed_by_route_ids(feed, route_ids)
            if isinstance(route_id_filter_result, tuple):
                # Error occurred
                return route_id_filter_result
            feed = route_id_filter_result

        # Handle multi-agency feeds
        agency_filter_result = self.filter_feed_by_agency(feed, agency_name, agency_id)
        if isinstance(agency_filter_result, tuple):
            # Error occurred
            return agency_filter_result
        feed = agency_filter_result

        # Filter by route type (bus only)
        route_type_filter_result = self.filter_feed_by_route_type(feed, bus_only)
        if isinstance(route_type_filter_result, tuple):
            # Error occurred
            return route_type_filter_result
        feed = route_type_filter_result

        # Validate and parse start_date
        if start_date is None:
            return (False, {"start_date": "start_date parameter is required"})

        try:
            # Parse ISO 8601 date (YYYY-MM-DD)
            start_date_obj = date_type.fromisoformat(start_date)
            # Convert to GTFS format (YYYYMMDD)
            start_date_gtfs = start_date_obj.strftime("%Y%m%d")
        except ValueError:
            return (False, {"start_date": f"Invalid date format: {start_date}. Expected ISO 8601 format (YYYY-MM-DD)"})

        # Validate duration
        if duration not in ["DAY", "WEEK"]:
            return (False, {"duration": f"Invalid duration: {duration}. Must be 'DAY' or 'WEEK'"})

        # Calculate date range
        if duration == "DAY":
            dates_to_import = [start_date_gtfs]
            end_date_gtfs = start_date_gtfs
        else:  # WEEK
            dates_to_import = []
            for i in range(7):
                date_obj = start_date_obj + timedelta(days=i)
                dates_to_import.append(date_obj.strftime("%Y%m%d"))
            end_date_gtfs = dates_to_import[-1]

        # Validate that the requested import period falls within the feed's validity period
        feed_validity = self.get_feed_validity_period(feed)
        if feed_validity is None:
            return (
                False,
                {
                    "calendar": "No calendar information found in GTFS feed. "
                    "The feed must contain either calendar.txt or calendar_dates.txt."
                },
            )

        feed_start, feed_end = feed_validity

        # Convert dates to date objects for comparison
        def parse_gtfs_date(date_str: str) -> date_type:
            """Parse YYYYMMDD string to date object"""
            return datetime.strptime(date_str, "%Y%m%d").date()

        feed_start_date = parse_gtfs_date(feed_start)
        feed_end_date = parse_gtfs_date(feed_end)
        import_start_date = parse_gtfs_date(start_date_gtfs)
        import_end_date = parse_gtfs_date(end_date_gtfs)

        # Check if the import period is outside the feed's validity period
        if import_start_date < feed_start_date or import_end_date > feed_end_date:
            # Format dates nicely for the error message (ISO 8601)
            def format_date(d: date_type) -> str:
                return d.strftime("%Y-%m-%d")

            return (
                False,
                {
                    "date_range": f"Your planned import period is from {format_date(import_start_date)} to "
                    f"{format_date(import_end_date)}, but the GTFS feed is only valid from "
                    f"{format_date(feed_start_date)} to {format_date(feed_end_date)}. "
                    f"Please choose dates within the feed's validity period."
                },
            )

        # Get timezone from agency.txt
        if feed.agency is None or len(feed.agency) == 0:
            return (False, {"agency": "No agency information found in GTFS feed"})

        # Check if all agencies have the same timezone
        timezones = feed.agency["agency_timezone"].unique()
        if len(timezones) == 0:
            return (False, {"timezone": "No agency_timezone found in GTFS feed"})
        if len(timezones) > 1:
            return (False, {"timezone": f"Multiple different timezones found in GTFS feed: {', '.join(timezones)}"})

        timezone_str = timezones[0]

        # Parse the timezone
        try:
            tz = ZoneInfo(timezone_str)
        except Exception:
            return (False, {"timezone": f"Invalid timezone in GTFS feed: {timezone_str}"})

        ingestion_uuid = uuid.uuid4()

        # Get the path for this UUID
        save_path = self.path_for_uuid(ingestion_uuid)
        save_path.mkdir(parents=True, exist_ok=True)

        data_to_save = {
            "data_to_import_dates": dates_to_import,
            "feed": feed,
            "gtfs_zip_file": gtfs_zip_file,
            "tz": tz,
            "agency_name": " / ".join(feed.agency["agency_name"].tolist())
            if "agency_name" in feed.agency.columns and len(feed.agency) > 0
            else "Unknown Agency",
            "start_date": start_date,
            "duration": duration,
        }

        with open(save_path / "gtfs_data.dill", "wb") as f:
            pickle.dump(data_to_save, f, protocol=pickle.HIGHEST_PROTOCOL)

        return (True, ingestion_uuid)

    @staticmethod
    def parse_gtfs_time(time_str: str, base_date: datetime) -> datetime:
        """
        Parse GTFS time format (HH:MM:SS) which can exceed 24 hours.

        Per the GTFS Schedule reference (Field Types - Time):

            The time is measured from "noon minus 12h" of the service day
            (effectively midnight except for days on which daylight savings
            time changes occur).

        https://gtfs.org/documentation/schedule/reference/#field-types

        We follow this definition literally rather than the more common
        shortcut of adding the parsed HH:MM:SS to local midnight: on DST
        transition days, "noon minus 12h" differs from local midnight by
        exactly the DST offset, and using local midnight produces non-
        monotonic UTC instants for stops bracketing the DST gap (which
        then trip the Trip.arrival_time > Trip.departure_time CHECK
        constraint at insert time).

        :param time_str: Time string in HH:MM:SS format. May exceed 24:00:00
                         for trips that continue past the end of the service
                         day.
        :param base_date: A datetime whose date components identify the
                          service day. If timezone-aware, the result is
                          returned in the same timezone; if naive, the GTFS
                          delta is added directly (no DST to consider) and
                          a warning is logged.
        :return: Datetime object representing the parsed time.
        """
        parts = time_str.split(":")
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
        delta = timedelta(hours=hours, minutes=minutes, seconds=seconds)

        if base_date.tzinfo is None:
            # No timezone information - fall back to wall-clock addition. The
            # callers in this module always pass a tz-aware base_date, so a
            # naive value here is a bug upstream that would produce silently
            # wrong results on DST transition days. Warn loudly so it gets
            # noticed.
            logging.getLogger(__name__).warning(
                "parse_gtfs_time called with naive base_date %r; DST handling "
                "is disabled and results will be incorrect on DST transition "
                "days. Pass a timezone-aware base_date.",
                base_date,
            )
            return base_date + delta

        # Step 1: locate noon (local) on the service day. Noon always exists
        # exactly once: DST transitions happen in the early hours, never at
        # midday.
        noon_local = base_date.replace(hour=12, minute=0, second=0, microsecond=0)

        # Step 2: subtract 12 hours of REAL elapsed time to obtain the
        # service-day anchor. We move through UTC for the subtraction -
        # subtracting a timedelta directly from an aware datetime would
        # re-derive the offset on the new wall clock, which is exactly the
        # class of mistake this function is being rewritten to avoid.
        noon_utc = noon_local.astimezone(timezone.utc)
        anchor_utc = noon_utc - timedelta(hours=12)

        # Step 3: add the GTFS offset as real elapsed time, then project the
        # result back into the original timezone.
        return (anchor_utc + delta).astimezone(base_date.tzinfo)

    @staticmethod
    def _parse_gtfs_time_with_anchor(time_str: str, anchor_utc: datetime, tz: Any) -> datetime:
        """Fast path for parse_gtfs_time when the per-service-day anchor is known.

        The anchor is ``noon_local.astimezone(UTC) - 12h`` for the service day; see
        :meth:`parse_gtfs_time` for the rationale. Callers that parse many times on
        the same service day should precompute one anchor per day and pass it here
        instead of reconstructing the tz conversion per call.
        """
        parts = time_str.split(":")
        delta = timedelta(hours=int(parts[0]), minutes=int(parts[1]), seconds=int(parts[2]))
        return (anchor_utc + delta).astimezone(tz)

    def parent_station_id_exists(self, parent_station_id: Any) -> bool:
        """Check is a given parent_station_id value is "filled" (not null/NA/empty)"""
        if isinstance(parent_station_id, str):
            if parent_station_id.strip() == "" or parent_station_id.strip().upper() == "<NA>":
                return False
            return True
        if pd.isna(parent_station_id):
            return False
        if parent_station_id is None:
            return False
        raise ValueError(f"Unknown parent_station_id value: {parent_station_id}")

    def ingest(  # type: ignore[override]
        self, uuid: UUID, always_flush: bool = False, progress_callback: None | Callable[[float], None] = None
    ) -> None:
        """
        Ingest the data into the database. In order for this method to be called, the :meth:`prepare` method must have
        returned a UUID, indicating that the preparation was successful.

        This method must call the progress_callback function periodically to update the progress of the ingestion
        process. The progress_callback function should accept a float value between 0 and 1.

        The method should check if a scenario with the same UUID already exists in the database. If it does, it should
        add the data to the existing scenario. If it does not, it should create a new scenario.

        :param uuid: A UUID representing the data to ingest.
        :param always_flush: If True, flush after each object is added to the session. Default is False.
        :return: Nothing. If unexpected errors occur, they should be raised as exceptions.
        """
        self.logger.info(f"Starting ingestion for UUID {uuid}")

        # Load the prepared data
        save_path = self.path_for_uuid(uuid)
        with open(save_path / "gtfs_data.dill", "rb") as f:
            prepared_data = pickle.load(f)

        dates_to_import = prepared_data["data_to_import_dates"]
        feed = prepared_data["feed"]
        gtfs_zip_file = prepared_data["gtfs_zip_file"]
        tz = prepared_data["tz"]

        # Create database engine and session
        engine = create_engine(self.database_url)
        session = Session(engine)

        try:
            self.logger.info("Starting GTFS to eflips model conversion and database insertion")

            # Define progress sections
            PROGRESS_STATIONS = 0.10
            PROGRESS_LINES = 0.05
            PROGRESS_ROUTES = 0.10
            PROGRESS_VEHICLE_TYPE = 0.01
            PROGRESS_ROTATIONS = 0.05
            PROGRESS_TRIP_ACTIVITY = 0.04
            PROGRESS_TRIPS = 0.25
            PROGRESS_STOP_TIMES = 0.25
            PROGRESS_ASSOC_ROUTE_STATIONS = 0.10
            PROGRESS_COMMIT = 0.05

            current_progress = 0.0
            if progress_callback:
                progress_callback(current_progress)

            # Create scenario and add to database
            scenario = Scenario(
                name=f"{prepared_data['agency_name']} GTFS Import ({prepared_data['start_date']} "
                f"for {prepared_data['duration']}) (file: {gtfs_zip_file.name})",
            )
            session.add(scenario)
            if always_flush:
                session.flush()

            # Dictionaries to store objects keyed by GTFS ID
            stations_dict: Dict[str, Station] = {}
            lines_dict: Dict[str, Line] = {}
            routes_dict: Dict[str, Route] = {}
            trips_dict: Dict[str, Trip] = {}
            rotations_dict: Dict[str, Rotation] = {}

            # Step 1: Create Station objects from stops
            # We are only creating the top-level "parent" stations in the database. However, these
            # parent statiosn are added to the dict under all stop_ids, so that we can link them later.
            self.logger.info("Creating Station objects from GTFS stops")
            stops_df = feed.stops
            stops_by_id = {row["stop_id"]: row for row in stops_df.to_dict("records")}
            for stop_row in stops_by_id.values():
                stop_id = stop_row["stop_id"]
                stop_name = stop_row["stop_name"]
                stop_lat = stop_row.get("stop_lat")
                stop_lon = stop_row.get("stop_lon")

                # Try go go up to parent_station if available
                root_parent_stop_id = None
                parent_station_id = stop_row.get("parent_station")
                if self.parent_station_id_exists(parent_station_id):
                    parent_stop_row = stops_by_id.get(parent_station_id)
                    if parent_stop_row is None:
                        raise ValueError(
                            f"parent_station {parent_station_id!r} referenced by stop_id {stop_id!r} "
                            f"not found in stops.txt"
                        )
                    # we may have to go up one more level
                    grandparent_station_id = parent_stop_row.get("parent_station")
                    if self.parent_station_id_exists(grandparent_station_id):
                        parent_stop_row = stops_by_id.get(grandparent_station_id)
                        if parent_stop_row is None:
                            raise ValueError(f"parent_station {grandparent_station_id!r} not found in stops.txt")
                        # Make sure we are at the top-level parent
                        great_grandparent_station_id = parent_stop_row.get("parent_station")
                        if self.parent_station_id_exists(great_grandparent_station_id):
                            raise ValueError(
                                f"More than two levels of parent_station found for stop_id {stop_id}. "
                                f"GTFS feed must not have more than two levels of station hierarchy."
                            )

                    # We now have the root parent. Check if it already exists in stations_dict
                    root_parent_stop_id = parent_stop_row["stop_id"]
                    if root_parent_stop_id in stations_dict:
                        station_from_gtfs = stations_dict[root_parent_stop_id]
                        # if the station already exists, we can continue here and not create a new one
                        stations_dict[stop_id] = station_from_gtfs
                        continue
                    else:
                        # Create new Station for the parent
                        stop_name = parent_stop_row["stop_name"]
                        stop_lat = parent_stop_row.get("stop_lat")
                        stop_lon = parent_stop_row.get("stop_lon")
                        stop_short_name = root_parent_stop_id
                else:
                    stop_short_name = stop_id

                # Create Point geometry if coordinates are available
                geom = None
                if pd.notna(stop_lat) and pd.notna(stop_lon):
                    point = Point(stop_lon, stop_lat)  # Note: lon, lat order for Point

                    # If geometry type has Z, we need to get altitude
                    if geometry_has_z():
                        z = get_altitude((stop_lat, stop_lon))
                        point = Point(stop_lon, stop_lat, z)

                    geom = from_shape(point, srid=4326)

                station_from_gtfs = Station(
                    name=stop_name,
                    name_short=stop_short_name,
                    geom=geom,
                    is_electrified=False,
                    is_electrifiable=True,
                    scenario=scenario,
                )
                stations_dict[stop_id] = station_from_gtfs
                if root_parent_stop_id is not None:
                    stations_dict[root_parent_stop_id] = station_from_gtfs
                session.add(station_from_gtfs)
                if always_flush:
                    session.flush()

            self.logger.info(f"Created {len(stations_dict)} stations")
            current_progress += PROGRESS_STATIONS
            if progress_callback:
                progress_callback(current_progress)

            # Step 2: Create Line objects from routes (one line per route_short_name)
            # For the "one line per route_short_name" logic, we need an additional dict
            lines_by_short_name: Dict[str, Line] = {}
            self.logger.info("Creating Line objects from GTFS routes")
            routes_df = feed.routes
            for idx, route_row in enumerate(routes_df.to_dict("records")):
                route_id = route_row["route_id"]
                if route_id is None or pd.isna(route_id):
                    raise ValueError(
                        f"route_id is required for all routes in GTFS feed, but route at index {idx} has missing route_id"
                    )
                gtfs_short_name = route_row.get("route_short_name")
                if gtfs_short_name is None or pd.isna(gtfs_short_name) or str(gtfs_short_name).strip() == "":
                    self.logger.warning(
                        f"route_short_name is missing or empty for route_id {route_id} at index {idx}. "
                        f"Using route_id as route_short_name."
                    )
                    route_short_name_or_id = route_id
                else:
                    route_short_name_or_id = gtfs_short_name

                # The "route_short_name" is similar to a line number, while "route_long_name" is more descriptive.
                # For the line, we use the route_short_name as both short name and merging key.
                # The long name is set later.

                if route_short_name_or_id not in lines_by_short_name:
                    line = Line(
                        name="TODO – if you're seeing this, the import went wrong!",
                        name_short=route_short_name_or_id,
                        scenario=scenario,
                    )
                    lines_by_short_name[route_short_name_or_id] = line
                    lines_dict[route_id] = line
                    session.add(line)
                    if always_flush:
                        session.flush()
                    self.logger.debug(f"Created line with route_short_name {route_short_name_or_id}")
                else:
                    lines_dict[route_id] = lines_by_short_name[route_short_name_or_id]
                    self.logger.debug(
                        f"Line with route_short_name {route_short_name_or_id} already exists, skipping creation"
                    )

            self.logger.info(f"Created {len(lines_by_short_name)} lines")
            current_progress += PROGRESS_LINES
            if progress_callback:
                progress_callback(current_progress)

            # Step 3: Create Route objects
            # For each unique combination of (route_id, direction_id, first_stop, last_stop), create a Route
            self.logger.info("Creating Route objects from GTFS trips")
            trips_df = feed.trips
            stop_times_df = feed.stop_times

            # Pre-process stop_times into a dictionary for fast lookup
            stop_times_by_trip = self._preprocess_stop_times_by_trip(stop_times_df)

            # Build route geometries from GTFS shapes if available
            route_geometries = self.build_route_geometries(feed)

            # Per-route_key shapely LineString, used later when computing
            # AssocRouteStation.elapsed_distance via point-on-line projection
            # for feeds that lack stop_times.shape_dist_traveled.
            shape_geom_by_route_key: Dict[str, LineString] = {}

            # Build trip endpoints using pre-sorted stop_times
            trip_endpoints = []
            for trip_row in trips_df.to_dict("records"):
                trip_id = trip_row["trip_id"]
                trip_stops_sorted = stop_times_by_trip.get(trip_id)

                if trip_stops_sorted is None or len(trip_stops_sorted) == 0:
                    continue

                first_stop = trip_stops_sorted.iloc[0]["stop_id"]
                last_stop = trip_stops_sorted.iloc[-1]["stop_id"]
                route_id = trip_row["route_id"]
                direction_id = trip_row.get("direction_id", 0)
                trip_headsign = trip_row.get("trip_headsign", "")
                shape_id = trip_row.get("shape_id", None)

                trip_endpoints.append(
                    {
                        "trip_id": trip_id,
                        "route_id": route_id,
                        "direction_id": str(direction_id),  # Otherwise it may get auto-converted to int
                        "first_stop": first_stop,
                        "last_stop": last_stop,
                        "headsign": trip_headsign,
                        "shape_id": shape_id,
                        "stop_sequence": tuple(trip_stops_sorted["stop_id"].tolist()),
                    }
                )

            trip_endpoints_df = pd.DataFrame(trip_endpoints)

            # Create a dictionary for fast trip endpoint lookups
            trip_endpoints_dict = {row["trip_id"]: row for row in trip_endpoints}

            # Create unique routes based on (route_id, direction_id, first_stop, last_stop)
            unique_routes = (
                trip_endpoints_df.groupby(["route_id", "direction_id", "first_stop", "last_stop", "stop_sequence"])
                .first()
                .reset_index()
            )

            for route_row in unique_routes.to_dict("records"):
                gtfs_route_id = route_row["route_id"]
                direction_id = route_row["direction_id"]
                first_stop_id = route_row["first_stop"]
                last_stop_id = route_row["last_stop"]
                headsign = route_row.get("headsign", "")
                shape_id = route_row.get("shape_id", None)
                stop_sequence = route_row["stop_sequence"]

                # Get the line for this route
                if gtfs_route_id not in lines_dict.keys():
                    raise ValueError(f"Line not found for route_id {gtfs_route_id}")
                line = lines_dict[gtfs_route_id]

                # Get departure and arrival stations
                departure_station = stations_dict[first_stop_id]
                arrival_station = stations_dict[last_stop_id]

                # Create a unique key for this route
                route_key = f"{gtfs_route_id}_{direction_id}_{first_stop_id}_{last_stop_id}_{'_'.join(stop_sequence)}"

                # Stash the shape geometry (if any) for Step 8, where the
                # final ``Route.distance`` / ``Route.geom`` / per-stop
                # elapsed_distances are derived together by
                # ``_compute_route_distances``. Until then the Route holds
                # a placeholder ``distance=1.0`` and ``geom=None`` (which
                # satisfies both the ``distance > 0`` and the
                # ``geom IS NULL OR ...`` constraints if the row is
                # flushed before Step 8).
                shapely_geom: LineString | None = None
                if route_geometries is not None and pd.notna(shape_id) and shape_id in route_geometries:
                    try:
                        shapely_geom = route_geometries[shape_id]
                    except Exception as e:
                        self.logger.warning(f"Failed to read shape {shape_id} for route {route_key}: {e}")
                        shapely_geom = None

                route = Route(
                    name=f"{line.name_short} → {arrival_station.name}" if line else f"Route to {arrival_station.name}",
                    name_short=f"{gtfs_route_id}_{direction_id}",
                    headsign=headsign if headsign else None,
                    distance=1.0,  # placeholder; finalised in Step 8
                    geom=None,  # finalised in Step 8 alongside distance
                    departure_station=departure_station,
                    arrival_station=arrival_station,
                    line=line,
                    scenario=scenario,
                )
                routes_dict[route_key] = route
                if shapely_geom is not None:
                    shape_geom_by_route_key[route_key] = shapely_geom
                session.add(route)
                if always_flush:
                    session.flush()

            self.logger.info(f"Created {len(routes_dict)} routes")
            current_progress += PROGRESS_ROUTES
            if progress_callback:
                progress_callback(current_progress)

            # Step 3.5: Update Line long names based on the most common stations for each line
            self.logger.info("Updating Line long names based on most common route_long_name")
            for line in lines_dict.values():
                # Get all routes for this line
                line_routes = [route for route in routes_dict.values() if route.line == line]

                # Get the most common arrival and departure stations for these routes
                arrival_stations = [route.arrival_station.name for route in line_routes]
                departure_stations = [route.departure_station.name for route in line_routes]
                if arrival_stations and departure_stations:
                    all_stations = set(arrival_stations + departure_stations)
                    sorted_stations = sorted(
                        all_stations,
                        key=lambda s: (arrival_stations.count(s) + departure_stations.count(s)),
                        reverse=True,
                    )
                    if len(sorted_stations) >= 2:
                        line.name = f"{line.name_short}: {sorted_stations[0]} <-> {sorted_stations[1]}"
                    elif len(sorted_stations) == 1:
                        line.name = f"{line.name_short}: {sorted_stations[0]}"
                    else:
                        line.name = line.name_short
                else:
                    line.name = line.name_short

            # Step 4: Create a default VehicleType (required for Rotation)
            # This is a placeholder - in a real scenario, vehicle types should be configured
            self.logger.info("Creating default VehicleType")
            default_vehicle_type = VehicleType(
                name="Default Bus",
                name_short="default_bus",
                battery_capacity=350.0,  # kWh
                charging_curve=[[0, 150], [1, 150]],  # Simple charging curve
                opportunity_charging_capable=False,
                consumption=1.0,  # kWh/km
                scenario=scenario,
            )
            session.add(default_vehicle_type)
            if always_flush:
                session.flush()

            current_progress += PROGRESS_VEHICLE_TYPE
            if progress_callback:
                progress_callback(current_progress)

            # Step 5: Create Rotation objects.
            # If the feed has real block_id values, group trips by (service_id, block_id)
            # to preserve block semantics (one rotation per vehicle-day). Otherwise
            # block information is absent or meaningless and we fall back to per-trip
            # dummy rotations created in Step 6 — which is also much cheaper, since it
            # avoids piling thousands of trips onto the same rotation collection.
            self.logger.info("Creating Rotation objects")
            has_real_block_id = "block_id" in trips_df.columns and trips_df["block_id"].notna().any()

            # Template rotations hold no trips themselves; per-day copies are created
            # in Step 6 and the empty templates are deleted at the end.
            template_rotations_delete_later: List[Rotation] = []

            if has_real_block_id:
                block_trips_df = trips_df.dropna(subset=["block_id"])
                for (service_id, block_id), _group in block_trips_df.groupby(["service_id", "block_id"]):
                    rotation_key = f"{service_id}_{block_id}"
                    rotation_from_gtfs = Rotation(
                        name=rotation_key,
                        vehicle_type=default_vehicle_type,
                        allow_opportunity_charging=False,
                        scenario=scenario,
                    )
                    rotations_dict[rotation_key] = rotation_from_gtfs
                    template_rotations_delete_later.append(rotation_from_gtfs)
                    session.add(rotation_from_gtfs)
                    if always_flush:
                        session.flush()

            self.logger.info(f"Created {len(rotations_dict)} rotation templates")
            current_progress += PROGRESS_ROTATIONS
            if progress_callback:
                progress_callback(current_progress)

            # Step 5.5: Compute trip activity for the selected dates
            # This uses gtfs_kit's built-in functionality to handle calendar and calendar_dates
            self.logger.info(f"Computing trip activity for {len(dates_to_import)} dates")
            trip_activity_df = feed.compute_trip_activity(dates_to_import)

            # Create a dict mapping trip_id to list of dates it runs on
            trip_dates_dict: Dict[str, List[str]] = {}
            for row in trip_activity_df.to_dict("records"):
                trip_id = row["trip_id"]
                active_dates = [date for date in dates_to_import if row[date] == 1]
                if active_dates:  # Only include trips that run on at least one date
                    trip_dates_dict[trip_id] = active_dates

            self.logger.info(f"Found {len(trip_dates_dict)} active trips")
            current_progress += PROGRESS_TRIP_ACTIVITY
            if progress_callback:
                progress_callback(current_progress)

            # Pre-compute the per-service-day "noon UTC minus 12h" anchor used by
            # parse_gtfs_time. The anchor depends only on (date, tz), so a ~40-entry
            # dict replaces ~1.7 M astimezone() + timedelta() recomputations on the
            # stop-times hot path for the Izmir-scale feed.
            anchors_by_date: Dict[str, datetime] = {}
            for d in dates_to_import:
                d_obj = datetime.strptime(d, "%Y%m%d").date()
                noon_local = datetime(d_obj.year, d_obj.month, d_obj.day, 12, tzinfo=tz)
                anchors_by_date[d] = noon_local.astimezone(timezone.utc) - timedelta(hours=12)

            # Step 6: Create Trip objects (one instance per trip per date it runs)
            self.logger.info("Creating Trip objects")
            trips_start_progress = current_progress
            total_trips = len(trips_df)
            for trip_idx, trip_row in enumerate(trips_df.to_dict("records")):
                trip_id = trip_row["trip_id"]

                # Skip trips that don't run on any of our selected dates
                if trip_id not in trip_dates_dict:
                    continue

                gtfs_route_id = trip_row["route_id"]
                service_id = trip_row["service_id"]
                block_id = trip_row.get("block_id")

                # Find the matching route
                trip_endpoint = trip_endpoints_dict.get(trip_id)
                if trip_endpoint is None:
                    raise ValueError(f"No endpoint information found for trip {trip_id}")

                direction_id = trip_endpoint["direction_id"]
                first_stop_id = trip_endpoint["first_stop"]
                last_stop_id = trip_endpoint["last_stop"]
                stop_sequence = trip_endpoint["stop_sequence"]

                route_key = f"{gtfs_route_id}_{direction_id}_{first_stop_id}_{last_stop_id}_{'_'.join(stop_sequence)}"
                if route_key not in routes_dict.keys():
                    raise ValueError(f"Route key {route_key} not found for trip {trip_id}")
                route = routes_dict[route_key]

                if route is None:
                    raise ValueError(f"Route not found for trip {trip_id} with key {route_key}")

                # Get trip times from stop_times (pre-sorted dictionary lookup)
                trip_stops = stop_times_by_trip.get(trip_id)

                if trip_stops is None or len(trip_stops) == 0:
                    raise ValueError(f"No stop times found for trip {trip_id}")

                # Parse departure and arrival times
                # GTFS times can be > 24:00:00, so we need to handle them specially
                first_arrival_str = trip_stops.iloc[0]["arrival_time"]
                last_departure_str = trip_stops.iloc[-1]["departure_time"]

                # Create one trip instance for each date this trip runs
                for date_str in trip_dates_dict[trip_id]:
                    # Two cases:
                    #   1. Feed has real block_ids and this trip has one: look up (or
                    #      create) the day-specific copy of the (service_id, block_id)
                    #      template, shared by all trips in the same block on this day.
                    #   2. Otherwise: give this trip instance its own one-trip dummy
                    #      rotation, never shared. Avoids O(N²) cascades from piling
                    #      thousands of trips onto one collection.
                    if has_real_block_id and pd.notna(block_id):
                        rotation_key_without_day = f"{service_id}_{block_id}"
                        rotation_key_with_day = f"{rotation_key_without_day}_{date_str}"
                        rotation = rotations_dict.get(rotation_key_with_day)
                        if rotation is None:
                            template = rotations_dict[rotation_key_without_day]
                            rotation = Rotation(
                                name=f"{template.name} for {date_str}",
                                vehicle_type=template.vehicle_type,
                                allow_opportunity_charging=template.allow_opportunity_charging,
                                scenario=scenario,
                            )
                            rotations_dict[rotation_key_with_day] = rotation
                            session.add(rotation)
                            if always_flush:
                                session.flush()
                    else:
                        rotation = Rotation(
                            name=f"GTFS dummy rotation for trip {trip_id} on {date_str}",
                            vehicle_type=default_vehicle_type,
                            allow_opportunity_charging=False,
                            scenario=scenario,
                        )
                        session.add(rotation)
                        if always_flush:
                            session.flush()

                    anchor_utc = anchors_by_date[date_str]

                    departure_time = self._parse_gtfs_time_with_anchor(first_arrival_str, anchor_utc, tz)

                    # For arrival time, use the departure time from the last stop if available
                    if pd.notna(last_departure_str) and last_departure_str:
                        arrival_time = self._parse_gtfs_time_with_anchor(last_departure_str, anchor_utc, tz)
                    else:
                        # Use arrival time of last stop
                        last_arrival_str = trip_stops.iloc[-1]["arrival_time"]
                        arrival_time = self._parse_gtfs_time_with_anchor(last_arrival_str, anchor_utc, tz)

                    # Create a unique trip key that includes the date
                    trip_instance_key = f"{trip_id}_{date_str}"
                    trip_from_gtfs = Trip(
                        route=route,
                        rotation=rotation,
                        departure_time=departure_time,
                        arrival_time=arrival_time,
                        trip_type=TripType.PASSENGER,
                        scenario=scenario,
                    )
                    trips_dict[trip_instance_key] = trip_from_gtfs
                    session.add(trip_from_gtfs)

                    if always_flush:
                        session.flush()

                # Update progress for trips section
                if progress_callback and total_trips > 0:
                    trip_progress = trips_start_progress + (trip_idx + 1) / total_trips * PROGRESS_TRIPS
                    progress_callback(trip_progress)

            self.logger.info(f"Created {len(trips_dict)} trip instances")
            current_progress += PROGRESS_TRIPS
            if progress_callback:
                progress_callback(current_progress)

            # Step 7: Create StopTime objects (for each trip instance on each date)
            self.logger.info("Creating StopTime objects")
            stop_times_start_progress = current_progress
            for trip_idx, trip_row in enumerate(trips_df.to_dict("records")):
                trip_id = trip_row["trip_id"]

                # Skip trips that don't run on any of our selected dates
                if trip_id not in trip_dates_dict:
                    continue

                trip_stop_times_df = stop_times_by_trip.get(trip_id)
                if trip_stop_times_df is None:
                    raise ValueError(f"No stop times found for trip {trip_id}")
                # Convert to list-of-dicts once per trip; the inner loop iterates this
                # list once per (trip, date) and avoids pandas-Series construction that
                # dominates DataFrame.iterrows.
                trip_stop_times_records = trip_stop_times_df.to_dict("records")

                for date_str in trip_dates_dict[trip_id]:
                    trip_instance_key = f"{trip_id}_{date_str}"
                    trip = trips_dict.get(trip_instance_key)

                    if trip is None:
                        raise ValueError(f"Trip instance not found for key {trip_instance_key}")

                    anchor_utc = anchors_by_date[date_str]

                    stop_times_to_add = []
                    for stop_time_row in trip_stop_times_records:
                        stop_id = stop_time_row["stop_id"]
                        arrival_time_str = stop_time_row["arrival_time"]
                        departure_time_str = stop_time_row.get("departure_time", arrival_time_str)

                        station = stations_dict.get(stop_id)
                        if station is None:
                            raise ValueError(f"Station not found for stop_id {stop_id} in trip {trip_id}")

                        arrival_time = self._parse_gtfs_time_with_anchor(arrival_time_str, anchor_utc, tz)

                        # Calculate dwell duration
                        if pd.notna(departure_time_str) and departure_time_str:
                            departure_time = self._parse_gtfs_time_with_anchor(departure_time_str, anchor_utc, tz)
                            dwell_duration = departure_time - arrival_time
                        else:
                            dwell_duration = timedelta(seconds=0)

                        stop_time = StopTime(
                            station=station,
                            trip=trip,
                            arrival_time=arrival_time,
                            dwell_duration=dwell_duration,
                            scenario=scenario,
                        )

                        # Add to(temporary) stop_times list
                        stop_times_to_add.append(stop_time)
                        # We cannot flush here, as we are (temporarily) shaving incomplete sets of stop times
                    # We may need to modify the stop_times, as there may be simultaneous arrivals/departures at different stations
                    # A bus cannot be in two places at once, so we need to stagger them slightly
                    stop_times_to_add = self.stagger_simultaneous_stop_times(stop_times_to_add)

                    # But after each trip, we can flush
                    session.add_all(stop_times_to_add)
                    if always_flush:
                        session.flush()

                # Update progress for stop times section
                if progress_callback and total_trips > 0:
                    stop_time_progress = stop_times_start_progress + (trip_idx + 1) / total_trips * PROGRESS_STOP_TIMES
                    progress_callback(stop_time_progress)

            # Count total stop times for logging
            total_stop_times = sum(len(trip.stop_times) for trip in trips_dict.values())
            self.logger.info(f"Created {total_stop_times} stop times")
            current_progress += PROGRESS_STOP_TIMES
            if progress_callback:
                progress_callback(current_progress)

            # Step 8: Create AssocRouteStation objects
            self.logger.info("Creating AssocRouteStation objects")
            for route_key, route in routes_dict.items():
                # Find trips for this route
                relevant_trips = [trip for trip in trips_dict.values() if trip.route == route]

                if len(relevant_trips) == 0:
                    warnings.warn(f"No trips found for route {route.name}, skipping AssocRouteStation creation")
                    continue

                # Use the trip with the longest stop_times as the sample trip
                sample_trip = max(relevant_trips, key=lambda t: len(t.stop_times))

                # Sort stop times by arrival time
                sorted_stop_times = sorted(sample_trip.stop_times, key=lambda st: st.arrival_time)

                # Recover the GTFS trip_id from the per-day instance key
                # ({trip_id}_{YYYYMMDD}); needed to look up the raw stop_times
                # rows that may carry shape_dist_traveled.
                trip_id_gtfs = [k for k, v in trips_dict.items() if v == sample_trip][0]
                trip_id_gtfs = trip_id_gtfs[:-9]

                stop_times_for_trip = stop_times_by_trip.get(trip_id_gtfs)
                shape_geom_for_route = shape_geom_by_route_key.get(route_key)

                # Single source of truth: pick one strategy (A → B → C),
                # produce a self-consistent (distance, elapsed_distances,
                # geom) triple, then drop it onto the Route + create the
                # matching AssocRouteStations. No endpoint anchor needed —
                # each Source pins endpoints by construction.
                rd = self._compute_route_distances(
                    sorted_stop_times=sorted_stop_times,
                    stop_times_for_trip=stop_times_for_trip,
                    shape_geom=shape_geom_for_route,
                    session=session,
                    route_name=route.name,
                )

                route.distance = rd.distance
                route_geom_pg = from_shape(rd.geom, srid=4326) if rd.geom is not None else None
                route.geom = route_geom_pg  # type: ignore[assignment]

                # Diagnostic dump: any non-last elapsed_distance strictly
                # greater than ``route.distance`` would trip the validator.
                # With the new orchestrator this should never happen, but
                # keep the safety net for debugging unexpected geometry.
                # Activated by ``EFLIPS_INGEST_GEOJSON_DUMP_DIR``.
                dump_dir = os.environ.get("EFLIPS_INGEST_GEOJSON_DUMP_DIR")
                if dump_dir and any(d > rd.distance for d in rd.elapsed_distances[:-1]):
                    self._dump_route_geojson(
                        dump_dir=dump_dir,
                        route_name=route.name,
                        shape_geom=shape_geom_for_route,
                        sorted_stop_times=sorted_stop_times,
                        distances=rd.elapsed_distances,
                        target_total_distance=rd.distance,
                    )

                # Build all assocs first, then attach in one go: SQLAlchemy
                # can otherwise see a temporarily out-of-order collection.
                assocs_to_add = [
                    AssocRouteStation(
                        route=route,
                        station=stop_time.station,
                        elapsed_distance=ed,
                        scenario=scenario,
                    )
                    for stop_time, ed in zip(sorted_stop_times, rd.elapsed_distances)
                ]
                route.assoc_route_stations = assocs_to_add
                session.add_all(assocs_to_add)
                if always_flush:
                    session.flush()

            # Remove unused template rotations
            removed_count = 0
            for rotation in template_rotations_delete_later:
                if len(rotation.trips) == 0:
                    # Only delete if the rotation has been persisted
                    from sqlalchemy.inspection import inspect

                    if inspect(rotation).persistent:
                        session.delete(rotation)
                        removed_count += 1
                        if always_flush:
                            session.flush()
            self.logger.info(f"Removed {removed_count} unused template rotations")

            # Count total AssocRouteStation objects for logging
            total_assoc_route_stations = sum(len(route.assoc_route_stations) for route in routes_dict.values())
            self.logger.info(f"Created {total_assoc_route_stations} route-station associations")
            current_progress += PROGRESS_ASSOC_ROUTE_STATIONS
            if progress_callback:
                progress_callback(current_progress)

            self.logger.info("GTFS to eflips model conversion completed")

            # In the previous steps, we stored the stop_id in name_short. This was done to make some lookups
            # easier during the conversion process. Now that we are done with all the conversions, we can clear
            # the name_short field for stations to avoid confusion.
            session.query(Station).filter(Station.scenario == scenario).update({Station.name_short: None})
            if always_flush:
                session.flush()

            # Commit the transaction
            self.logger.info("Committing transaction to database")
            session.commit()

            current_progress += PROGRESS_COMMIT
            if progress_callback:
                progress_callback(1.0)

            self.logger.info(f"Ingestion completed for UUID {uuid}")

        except Exception as e:
            self.logger.error(f"Database insertion failed: {e}")
            session.rollback()
            raise e
        finally:
            session.close()

    @classmethod
    def prepare_param_names(cls) -> Dict[str, str | Dict[Enum, str]]:
        """
        A dictionary containing the parameter names for :meth:`prepare`.

        These should be short, descriptive names for the parameters of the :meth:`prepare` method. The keys must be the
        names of the parameters, and the values should be strings describing the parameter. If the keyword argument is
        an enumerated type, the value should be a dictionary with the keys being the members.

        This method can then be used to generate a help text for the user.

        :return: A dictionary containing the parameter hints for the prepare method.
        """
        return {
            "gtfs_zip_file": "The GTFS zip file to ingest.",
            "start_date": "Start date for the import (ISO 8601 format, e.g., '2024-01-15')",
            "duration": "Duration to import ('DAY' or 'WEEK')",
            "agency_name": "Agency name (required if feed contains multiple agencies)",
            "agency_id": "Agency ID (alternative to agency_name)",
            "bus_only": "Filter to only import bus routes (default: True)",
            "route_ids": "Route id(s) to restrict the import to (optional)",
        }

    @classmethod
    def prepare_param_description(cls) -> Dict[str, str | Dict[Enum, str]]:
        """
        A dictionary containing the parameter descriptions for :meth:`prepare`.

        These should be longer, more detailed descriptions of the parameters of the :meth:`prepare`
        method. The keys must be the names of the parameters, and the values should be strings describing the parameter.

        This method can then be used to generate a help text for the user.

        :return: A dictionary containing the parameter hints for the prepare method.
        """
        return {
            "gtfs_zip_file": "A zip file containing the GTFS data to ingest. This file should contain all the necessary "
            "GTFS files, such as stops.txt, routes.txt, trips.txt, stop_times.txt, etc. The timezone will be "
            "automatically extracted from the agency_timezone field in agency.txt.",
            "start_date": "The start date for the import in ISO 8601 format (YYYY-MM-DD). Trips will be imported "
            "starting from this date. The calendar.txt and calendar_dates.txt files will be used to determine which "
            "trips run on which dates, including handling exceptions for added or removed services.",
            "duration": "The duration of the import period. Must be either 'DAY' (import one day) or 'WEEK' (import "
            "one week starting from the start_date). For 'WEEK', trips will be imported for 7 consecutive days.",
            "agency_name": "The name of the agency to import from the GTFS feed. This parameter is required if the "
            "feed contains multiple agencies. If the feed contains only one agency, this parameter is optional and "
            "will be ignored. The agency name must match the 'agency_name' field in agency.txt exactly. If not "
            "specified for a multi-agency feed, an error will be returned listing all available agencies.",
            "agency_id": "The ID of the agency to import from the GTFS feed. Can be used as an alternative to "
            "agency_name. The agency ID must match the 'agency_id' field in agency.txt exactly. Accepts a single "
            "string or an iterable of strings to select multiple agencies.",
            "bus_only": "If True (default), only bus routes will be imported from the GTFS feed. This includes routes "
            "with route_type of 3 (standard GTFS bus) or 700-799 (extended GTFS bus types). If False, all route types "
            "will be imported. If set to True and the feed contains no bus routes, an error will be returned.",
            "route_ids": "Optional route id(s) to restrict the import to. Accepts a single string or an iterable of "
            "strings matching the 'route_id' field in routes.txt. When provided, the feed is filtered to those "
            "routes (and their dependent trips, stops, shapes, etc.) before any agency or bus-only filtering. "
            "If any supplied route id is not present in the feed, an error listing the available route ids is "
            "returned. If omitted or empty, no route-id filtering is applied.",
        }

    @staticmethod
    def _coerce_str_list(value: str | Iterable[str] | None) -> List[str]:
        """Normalise a scalar/iterable/None into a list of non-empty strings.

        ``None`` / ``""`` → ``[]``; a single string → ``[s]``; any iterable of
        strings → a list with empty entries dropped.
        """
        if value is None or value == "":
            return []
        if isinstance(value, str):
            return [value]
        return [str(v) for v in value if v not in (None, "")]

    def filter_feed_by_agency(
        self,
        feed: Feed,
        agency_name: str | Iterable[str] | None = None,
        agency_id: str | Iterable[str] | None = None,
    ) -> Feed | Tuple[bool, Dict[str, str]]:
        """
        Filter the GTFS feed to include only data from one or more agencies.

        Agencies can be selected by name, by id, or by a combination of both.
        Each selector accepts either a single string or an iterable of strings,
        allowing callers that operate a shared depot across multiple "paper"
        agencies to pass all of them in a single call.

        If the feed contains only one agency, both selectors are ignored.
        If the feed contains multiple agencies and neither selector is given,
        returns an error tuple with the list of available agencies.

        :param feed: A gtfs_kit Feed object
        :param agency_name: Name(s) of agencies to keep (str or iterable of str)
        :param agency_id: Id(s) of agencies to keep (str or iterable of str)
        :return: Filtered Feed object, or error tuple (False, error_dict)
        """
        if feed.agency is None or len(feed.agency) == 0:
            return (False, {"agency": "No agency information found in GTFS feed"})

        num_agencies = len(feed.agency)

        wanted_names = self._coerce_str_list(agency_name)
        wanted_ids = self._coerce_str_list(agency_id)

        # Single agency - no filtering needed
        if num_agencies == 1:
            if wanted_names or wanted_ids:
                self.logger.info(
                    f"Feed contains only one agency, ignoring agency_name={wanted_names!r} " f"agency_id={wanted_ids!r}"
                )
            return feed

        # Multiple agencies - at least one selector is required
        if not wanted_names and not wanted_ids:
            agency_names = feed.agency["agency_name"].tolist()
            agency_list = "\n".join(f"  - {name}" for name in agency_names)
            error_msg = (
                f"The GTFS feed contains {num_agencies} agencies. "
                f"Please specify which agency or agencies to import using the "
                f"'agency_name' or 'agency_id' parameter (a single string or a "
                f"list of strings is accepted).\n\n"
                f"Available agencies:\n{agency_list}"
            )
            return (False, {"agency_name": error_msg})

        matched_by_name = feed.agency[feed.agency["agency_name"].isin(wanted_names)] if wanted_names else None
        matched_by_id = feed.agency[feed.agency["agency_id"].astype(str).isin(wanted_ids)] if wanted_ids else None

        errors: Dict[str, str] = {}
        if matched_by_name is not None:
            missing_names = sorted(set(wanted_names) - set(matched_by_name["agency_name"]))
            if missing_names:
                agency_names_list = feed.agency["agency_name"].tolist()
                agency_list = "\n".join(f"  - {name}" for name in agency_names_list)
                errors["agency_name"] = (
                    f"Agency name(s) {missing_names} not found in GTFS feed.\n\n" f"Available agencies:\n{agency_list}"
                )
        if matched_by_id is not None:
            missing_ids = sorted(set(wanted_ids) - set(matched_by_id["agency_id"].astype(str)))
            if missing_ids:
                agency_ids_list = feed.agency["agency_id"].astype(str).tolist()
                agency_list = "\n".join(f"  - {aid}" for aid in agency_ids_list)
                errors["agency_id"] = (
                    f"Agency id(s) {missing_ids} not found in GTFS feed.\n\n" f"Available agency ids:\n{agency_list}"
                )
        if errors:
            return (False, errors)

        frames = [df for df in (matched_by_name, matched_by_id) if df is not None]
        combined = pd.concat(frames).drop_duplicates(subset=["agency_id"])
        agency_ids_to_keep = list(combined["agency_id"])
        self.logger.info(
            f"Filtering feed to {len(agency_ids_to_keep)} agency/agencies "
            f"(ids: {agency_ids_to_keep}, names: {list(combined['agency_name'])})"
        )

        try:
            filtered_feed = feed.restrict_to_agencies(agency_ids_to_keep)
            assert isinstance(filtered_feed, Feed)
            self.logger.info(f"Feed filtered: {len(feed.routes)} routes → {len(filtered_feed.routes)} routes")
            return filtered_feed
        except Exception as e:
            return (False, {"agency_filter": f"Failed to filter feed by agency: {str(e)}"})

    def filter_feed_by_route_type(self, feed: Feed, bus_only: bool) -> Feed | Tuple[bool, Dict[str, str]]:
        """
        Filter the GTFS feed to include only specific route types.

        If bus_only is True, only bus routes will be included (route_type 3 or 700-799).
        If bus_only is False, all route types are included (no filtering).

        :param feed: A gtfs_kit Feed object
        :param bus_only: If True, filter to only bus routes; if False, include all route types
        :return: Filtered Feed object, or error tuple (False, error_dict)
        """
        # If bus_only is False, return the feed unmodified
        if not bus_only:
            self.logger.info("bus_only=False: Including all route types")
            return feed

        # Check if routes exist
        if feed.routes is None or len(feed.routes) == 0:
            return (False, {"routes": "No routes found in GTFS feed"})

        # Filter for bus routes (route_type 3 or 700-799)
        # route_type 3 = Bus (standard GTFS)
        # route_type 700-799 = Bus service (extended GTFS)
        bus_routes = feed.routes[
            (feed.routes["route_type"] == 3) | ((feed.routes["route_type"] >= 700) & (feed.routes["route_type"] <= 799))
        ]

        if len(bus_routes) == 0:
            return (
                False,
                {"bus_only": "No bus routes found in GTFS feed. " "Set bus_only=False to import all route types."},
            )

        # Get list of bus route IDs
        bus_route_ids = bus_routes["route_id"].tolist()
        self.logger.info(f"Filtering feed to {len(bus_route_ids)} bus routes")

        # Use gtfs_kit's restrict_to_routes to filter the feed
        try:
            filtered_feed = feed.restrict_to_routes(bus_route_ids)
            assert isinstance(filtered_feed, Feed)
            self.logger.info(f"Feed filtered: {len(feed.routes)} routes → {len(filtered_feed.routes)} routes")
            return filtered_feed
        except Exception as e:
            return (False, {"route_type_filter": f"Failed to filter feed by route type: {str(e)}"})

    def filter_feed_by_route_ids(
        self,
        feed: Feed,
        route_ids: str | Iterable[str] | None,
    ) -> Feed | Tuple[bool, Dict[str, str]]:
        """
        Filter the GTFS feed to only the given routes and their dependent data.

        Uses ``Feed.restrict_to_routes`` from gtfs_kit, which cascades the
        restriction through trips, stop_times, stops (including parent stations),
        calendar, calendar_dates, shapes, frequencies, and transfers — so the
        returned feed contains no orphaned ancillary data.

        Empty / None ``route_ids`` is treated as a no-op and returns the feed
        unchanged, symmetric with ``bus_only=False`` in ``filter_feed_by_route_type``.

        :param feed: A gtfs_kit Feed object
        :param route_ids: Route id(s) to keep. Accepts a single string, an iterable
            of strings, or None/"" / [] for "no filter".
        :return: Filtered Feed object, or error tuple (False, error_dict)
        """
        wanted_ids = self._coerce_str_list(route_ids)
        if not wanted_ids:
            self.logger.info("No route_ids provided: skipping route-id filter")
            return feed

        if feed.routes is None or len(feed.routes) == 0:
            return (False, {"routes": "No routes found in GTFS feed"})

        feed_route_ids = set(feed.routes["route_id"].astype(str))
        missing = sorted(set(wanted_ids) - feed_route_ids)
        if missing:
            available = "\n".join(f"  - {rid}" for rid in sorted(feed_route_ids))
            return (
                False,
                {
                    "route_ids": (
                        f"Route id(s) {missing} not found in GTFS feed.\n\n" f"Available route ids:\n{available}"
                    )
                },
            )

        self.logger.info(f"Filtering feed to {len(wanted_ids)} route id(s)")
        try:
            filtered_feed = feed.restrict_to_routes(wanted_ids)
            assert isinstance(filtered_feed, Feed)
            self.logger.info(f"Feed filtered: {len(feed.routes)} routes → {len(filtered_feed.routes)} routes")
            return filtered_feed
        except Exception as e:
            return (False, {"route_ids_filter": f"Failed to filter feed by route ids: {str(e)}"})

    def build_route_geometries(self, feed: Feed) -> Dict[str, Any] | None:
        """
        Build a dictionary mapping GTFS shape_id to Shapely LineString geometries.

        Returns None if the feed has no shapes, otherwise returns a dictionary where:
        - Keys are shape_id strings
        - Values are Shapely LineString objects in WGS84 coordinates (SRID 4326)

        :param feed: A gtfs_kit Feed object
        :return: Dictionary of shape_id -> LineString, or None if no shapes available
        """
        if feed.shapes is None:
            self.logger.info("Feed has no shapes.txt file, skipping route geometry creation")
            return None

        self.logger.info(f"Building route geometries from {len(feed.shapes['shape_id'].unique())} GTFS shapes")

        try:
            # Use gtfs_kit to build geometries in WGS84 coordinates
            geometry_by_shape = feed.build_geometry_by_shape(use_utm=False)
            assert isinstance(geometry_by_shape, dict)
            self.logger.info(f"Successfully built {len(geometry_by_shape)} route geometries")
            if geometry_has_z():
                # We will need to turn each LINTESTRING into a LINESTRING Z by looking up the Z values for all points
                self.logger.info("Converting route geometries to LINESTRING Z format")
                geometry_by_shape_z: Dict[str, LineString] = {}
                for shape_id, geom in geometry_by_shape.items():
                    assert isinstance(geom, LineString)
                    coords_with_z = []
                    for lon, lat in geom.coords:
                        z = get_altitude((lat, lon))
                        coords_with_z.append((lon, lat, z))
                    geom_z = LineString(coords_with_z)
                    geometry_by_shape_z[shape_id] = geom_z
                return geometry_by_shape_z
            else:
                return geometry_by_shape
        except Exception as e:
            self.logger.error(f"Failed to build route geometries: {e}")
            raise e  # Potentially, there are some special types of exceptions we may want to swallow

    # ------------------------------------------------------------------
    # Distance computation: one chosen source per route, returning a
    # self-consistent ``RouteDistances`` so ``Route.distance`` and
    # ``AssocRouteStation.elapsed_distance`` cannot disagree.
    #
    # Source priority (orchestrator: ``_compute_route_distances``):
    #   A  ``stop_times.shape_dist_traveled`` (per-stop producer values,
    #      with km/miles unit detection vs. shape geometry).
    #   B  Project stops onto the GTFS shape; geodetic meters along the
    #      polyline. Closed-loop terminus/start stops are snapped.
    #   C  Build a synthetic LineString from the stop coordinates and
    #      take its geodetic length. No detour factor; logged loud.
    # If none of A/B/C is applicable, ingestion fails for that route.
    # ------------------------------------------------------------------

    @staticmethod
    def _stop_lonlats(sorted_stop_times: List[StopTime]) -> List[Tuple[float, float]] | None:
        """Extract (lon, lat) for each stop, or ``None`` if any stop lacks geom."""
        out: List[Tuple[float, float]] = []
        for st in sorted_stop_times:
            if st.station.geom is None:
                return None
            pt = to_shape(st.station.geom)  # type: ignore[arg-type]
            out.append((float(pt.x), float(pt.y)))
        return out

    def _detect_shape_dist_unit(
        self,
        max_raw_value: float,
        shape_geom: LineString | None,
        session: Session,
        route_name: str,
    ) -> float | None:
        """Return the scale factor that converts raw ``shape_dist_traveled``
        values to meters, or ``None`` if the unit cannot be confidently
        identified.

        With no shape geometry to compare against we trust the GTFS spec
        and assume meters. Otherwise we compare ``max_raw_value`` against
        the shape's geodetic length and accept the first ±5% band that
        matches:

        - meters       (factor 1.0)
        - kilometers   (factor 1000.0,    WARNING)
        - miles        (factor 1609.344,  WARNING)

        A ratio outside every band returns ``None`` so the orchestrator
        can fall through to Source B.
        """
        if shape_geom is None:
            return 1.0
        try:
            geodetic = Route.calculate_length(session, shape_geom.wkt)
        except Exception as e:  # noqa: BLE001  -- be lenient; B/C still possible
            self.logger.warning(
                f"Route {route_name!r}: could not compute shape geodetic length "
                f"for unit detection ({e}); assuming shape_dist_traveled is in meters."
            )
            return 1.0
        if geodetic <= 0.0 or max_raw_value <= 0.0:
            return None
        ratio = max_raw_value / geodetic
        for unit_name, factor in _UNIT_BANDS:
            if (1.0 - _UNIT_BAND_TOLERANCE) * factor <= ratio <= (1.0 + _UNIT_BAND_TOLERANCE) * factor:
                if factor != 1.0:
                    self.logger.warning(
                        f"Route {route_name!r}: stop_times.shape_dist_traveled appears "
                        f"to be in {unit_name} (ratio {ratio:.3f} vs shape geodetic "
                        f"length {geodetic:.0f} m). Rescaling by {factor}; the GTFS "
                        f"spec mandates meters, please fix the feed upstream."
                    )
                return factor
        self.logger.warning(
            f"Route {route_name!r}: max(stop_times.shape_dist_traveled)/geodetic "
            f"= {ratio:.3f} matches no known unit (meters/km/miles, ±5%); "
            f"falling back to shape projection."
        )
        return None

    def _source_a_shape_dist_traveled(
        self,
        sorted_stop_times: List[StopTime],
        stop_times_for_trip: pd.DataFrame | None,
        shape_geom: LineString | None,
        session: Session,
        route_name: str,
    ) -> RouteDistances | None:
        """Source A: per-stop ``shape_dist_traveled`` from ``stop_times.txt``.

        Positional match: the StopTime objects were created in stop_sequence
        order from the same DataFrame, so index ``i`` of ``sorted_stop_times``
        corresponds to row ``i`` of ``stop_times_for_trip``. (Matching by
        station identity fails whenever GTFS stops use ``parent_station``.)

        When a shape geometry is available, ``Route.distance`` is anchored
        to the geodetic length of that shape (so the database's
        ``geom IS NULL OR ABS(ST_Length(geom, True) - distance) < 50``
        constraint is satisfied by construction) and the per-stop values
        are rescaled to match. When no shape is present, the producer's
        values are used as-is.

        Returns ``None`` if any precondition fails (missing column, length
        mismatch, NaN, non-monotonic, max <= 0, or unit detection inconclusive).
        """
        n = len(sorted_stop_times)
        if stop_times_for_trip is None or "shape_dist_traveled" not in stop_times_for_trip.columns:
            return None
        if len(stop_times_for_trip) != n:
            return None
        sd_col = stop_times_for_trip["shape_dist_traveled"]
        if not sd_col.notna().all():
            return None
        raw = [float(sd_col.iloc[i]) for i in range(n)]
        if any(raw[i] < raw[i - 1] for i in range(1, n)):
            return None
        if raw[-1] <= 0.0:
            return None

        unit_factor = self._detect_shape_dist_unit(raw[-1], shape_geom, session, route_name)
        if unit_factor is None:
            return None

        elapsed = [v * unit_factor for v in raw]

        if shape_geom is not None:
            # Anchor distance to the database-side geodetic length so the
            # geom-vs-distance CHECK constraint is satisfied by construction.
            # Rescale the shape_dist_traveled values to match: the producer's
            # *relative* per-stop spacing is preserved, the absolute scale
            # comes from the same geodesy the constraint uses.
            distance = Route.calculate_length(session, shape_geom.wkt)
            if distance <= 0.0:
                return None
            if elapsed[-1] > 0.0 and elapsed[-1] != distance:
                scale = distance / elapsed[-1]
                elapsed = [v * scale for v in elapsed]
        else:
            distance = elapsed[-1]

        # Endpoint contract: by construction, ``elapsed[-1]`` is the
        # rescaled producer max; ``elapsed[0]`` should be 0 in well-formed
        # feeds. Pin both exactly to honour the validator's strict
        # bit-equality checks.
        elapsed[0] = 0.0
        elapsed[-1] = distance
        return RouteDistances(
            distance=distance,
            elapsed_distances=elapsed,
            geom=shape_geom,
            source="shape_dist_traveled",
        )

    def _source_b_shape_projection(
        self,
        sorted_stop_times: List[StopTime],
        shape_geom: LineString,
        session: Session,
        route_name: str,
    ) -> RouteDistances | None:
        """Source B: project stops onto the GTFS shape, return geodetic meters.

        Distance and per-stop values come from the same shape geometry so
        endpoints land at 0 and ``distance`` by construction (modulo a
        sub-metre FP delta from the PostGIS-vs-pyproj geodesy difference,
        which we clamp explicitly at the end).

        Raises :class:`AmbiguousProjectionError` when ≥ 2 stops cap at the
        same end of the shape. The orchestrator catches that and falls
        through to Source C with a WARNING.

        Returns ``None`` if the shape is degenerate (< 2 distinct vertices)
        or any stop lacks coordinates.
        """
        n = len(sorted_stop_times)
        coords_2d: List[Tuple[float, float]] = [(float(c[0]), float(c[1])) for c in shape_geom.coords]
        if len(coords_2d) < 2:
            return None
        stops_lonlat = self._stop_lonlats(sorted_stop_times)
        if stops_lonlat is None:
            return None

        shape_2d = LineString(coords_2d)

        # Cumulative degree-arc along the polyline; matches LineString.project units.
        deg_cum: List[float] = [0.0]
        for i in range(1, len(coords_2d)):
            x1, y1 = coords_2d[i - 1]
            x2, y2 = coords_2d[i]
            deg_cum.append(deg_cum[-1] + math.hypot(x2 - x1, y2 - y1))

        # Cumulative ellipsoidal meters along the polyline. ``Geod.line_lengths``
        # returns per-segment lengths; we fold them into a cumulative array.
        shape_lons = [c[0] for c in coords_2d]
        shape_lats = [c[1] for c in coords_2d]
        seg_meters = list(_GEOD.line_lengths(shape_lons, shape_lats))
        geo_cum: List[float] = [0.0]
        for seg in seg_meters:
            geo_cum.append(geo_cum[-1] + float(seg))

        if deg_cum[-1] <= 0.0 or geo_cum[-1] <= 0.0:
            return None

        # Authoritative route distance: the geodetic length the database
        # itself computes from this same shape (PostGIS / SpatiaLite).
        distance = Route.calculate_length(session, shape_geom.wkt)
        if distance <= 0.0:
            return None

        # Project each stop onto the shape (degrees), then map to geodetic
        # meters via (deg_cum, geo_cum). Track which stops landed at each
        # end cap so we can detect ambiguous projections below.
        at_start = [False] * n
        at_end = [False] * n
        elapsed: List[float] = []
        for i, (lon, lat) in enumerate(stops_lonlat):
            proj_deg = float(shape_2d.project(Point(lon, lat)))
            if proj_deg <= 0.0:
                at_start[i] = True
                elapsed.append(0.0)
                continue
            if proj_deg >= deg_cum[-1]:
                at_end[i] = True
                elapsed.append(geo_cum[-1])
                continue
            idx = bisect.bisect_right(deg_cum, proj_deg)
            if idx <= 0:
                idx = 1
            elif idx >= len(deg_cum):
                idx = len(deg_cum) - 1
            seg_deg = deg_cum[idx] - deg_cum[idx - 1]
            if seg_deg <= 0.0:
                elapsed.append(geo_cum[idx - 1])
                continue
            frac = (proj_deg - deg_cum[idx - 1]) / seg_deg
            seg_geo = geo_cum[idx] - geo_cum[idx - 1]
            elapsed.append(geo_cum[idx - 1] + frac * seg_geo)

        # Closed-loop terminus snap: the last stop projects to ~0 because
        # the shape's start and end vertices coincide. Snap forward.
        if n >= 2 and elapsed[-1] < elapsed[-2]:
            self.logger.warning(
                f"Closed-loop shape detected for route {route_name!r}: terminus stop "
                f"projected backward (raw {elapsed[-1]:.1f} m < previous stop's "
                f"{elapsed[-2]:.1f} m). Snapping to end of shape ({geo_cum[-1]:.1f} m)."
            )
            elapsed[-1] = geo_cum[-1]
            at_start[-1] = False
            at_end[-1] = True

        # Symmetric closed-loop start snap.
        if n >= 2 and elapsed[0] > elapsed[1]:
            self.logger.warning(
                f"Closed-loop shape detected for route {route_name!r}: first stop "
                f"projected forward (raw {elapsed[0]:.1f} m > next stop's "
                f"{elapsed[1]:.1f} m). Snapping to 0."
            )
            elapsed[0] = 0.0
            at_end[0] = False
            at_start[0] = True

        # Strict-count check: ≥ 2 stops at either cap means the projection
        # cannot disambiguate them. Signal up to the orchestrator.
        if sum(at_start) > 1:
            raise AmbiguousProjectionError(
                f"Route {route_name!r}: {sum(at_start)} stops project at or before "
                f"the shape's start vertex; cannot disambiguate."
            )
        if sum(at_end) > 1:
            raise AmbiguousProjectionError(
                f"Route {route_name!r}: {sum(at_end)} stops project at or after "
                f"the shape's end vertex; cannot disambiguate."
            )

        # Monotonicity sweep for back-tracking shapes.
        for i in range(1, n):
            if elapsed[i] < elapsed[i - 1]:
                elapsed[i] = elapsed[i - 1]

        # Pin endpoints exactly. Both should already match within ~1e-6 m
        # because ``distance`` and ``elapsed`` derive from the same shape;
        # the explicit assignment honours the validator's bit-equality
        # check and absorbs the residual PostGIS-vs-pyproj FP delta.
        elapsed[0] = 0.0
        delta = elapsed[-1] - distance
        if abs(delta) > 1.0:
            self.logger.warning(
                f"Route {route_name!r}: last projected distance {elapsed[-1]:.1f} m "
                f"differs from shape geodetic length {distance:.1f} m by {delta:+.1f} m; "
                f"clamping to the geodetic length."
            )
        elif abs(delta) > 0.0:
            self.logger.debug(f"Route {route_name!r}: clamping last elapsed distance by {delta:+.3f} m.")
        elapsed[-1] = distance

        # Monotonicity sweep again in case the [-1] clamp introduced a dip.
        for i in range(n - 2, 0, -1):
            if elapsed[i] > elapsed[-1]:
                elapsed[i] = elapsed[-1]

        return RouteDistances(
            distance=distance,
            elapsed_distances=elapsed,
            geom=shape_geom,
            source="shape_projection",
        )

    def _source_c_stops_haversine(
        self,
        sorted_stop_times: List[StopTime],
        route_name: str,
    ) -> RouteDistances | None:
        """Source C: build a synthetic LineString from the stop coordinates
        and use its geodetic length as both ``route.distance`` and the
        cumulative ``elapsed_distance`` per stop.

        This is much weaker than projecting onto a real shape — there is
        no detour factor, so the result systematically *under*-estimates
        the real route length. Logged as WARNING per route.

        Adjacent stops with identical coordinates produce zero-length
        segments. We dedup the leading/trailing runs by nudging the
        duplicates outward by 1 mm so the validator's strict-equality
        endpoint checks still see clean values.

        Returns ``None`` if any stop lacks coordinates (the orchestrator
        treats this as a hard failure for the route).
        """
        stops_lonlat = self._stop_lonlats(sorted_stop_times)
        if stops_lonlat is None:
            return None
        n = len(stops_lonlat)
        if n < 2:
            return None

        lons = [c[0] for c in stops_lonlat]
        lats = [c[1] for c in stops_lonlat]
        seg_lengths = [float(s) for s in _GEOD.line_lengths(lons, lats)]

        elapsed: List[float] = [0.0]
        for seg in seg_lengths:
            elapsed.append(elapsed[-1] + seg)
        distance = elapsed[-1]
        if distance <= 0.0:
            return None

        # Dedup adjacent identical-coord stops at the leading / trailing
        # runs by nudging the duplicates outward. Without this, the
        # validator would see ≥ 2 elapsed_distance entries equal to 0
        # (or to ``distance``), one of which trips the bit-equality check.
        n_leading = 0
        for v in elapsed:
            if v == 0.0:
                n_leading += 1
            else:
                break
        n_trailing = 0
        for v in reversed(elapsed):
            if v == distance:
                n_trailing += 1
            else:
                break
        if n_leading > 1 or n_trailing > 1:
            self.logger.warning(
                f"Route {route_name!r}: {n_leading} leading and {n_trailing} trailing "
                f"stops share identical coordinates (Source C, no shape geometry). "
                f"Nudging duplicates by 1 mm so the validator's endpoint checks pass; "
                f"please verify the GTFS stop coordinates upstream."
            )
            # Push duplicate-leading stops to small positive offsets so the
            # first stop alone holds 0.0.
            for k in range(1, n_leading):
                elapsed[k] = 0.001 * k
            # Pull duplicate-trailing stops back from ``distance`` so the
            # last stop alone holds ``distance``. Walk inward from the
            # second-to-last; each gets a slightly smaller value than its
            # successor.
            for k in range(1, n_trailing):
                elapsed[n - 1 - k] = distance - 0.001 * k
            # Restore monotonicity in case the nudge collided with body
            # values (unlikely for short stop lists but cheap to enforce).
            for i in range(1, n):
                if elapsed[i] < elapsed[i - 1]:
                    elapsed[i] = elapsed[i - 1]

        # Build the synthetic LineString. Preserve Z when the schema uses
        # POINT Z stations so the resulting LINESTRING Z matches the
        # stations' dimensionality.
        if geometry_has_z():
            coords_xyz: List[Tuple[float, float, float]] = []
            for st in sorted_stop_times:
                pt = to_shape(st.station.geom)  # type: ignore[arg-type]
                z = float(pt.z) if pt.has_z else 0.0
                coords_xyz.append((float(pt.x), float(pt.y), z))
            geom = LineString(coords_xyz)
        else:
            geom = LineString(stops_lonlat)

        self.logger.warning(
            f"Route {route_name!r}: no shape_dist_traveled and no usable shape "
            f"geometry; falling back to Source C (stops-as-line haversine). "
            f"Distance {distance:.0f} m is the geodetic sum of stop-to-stop legs "
            f"with NO detour factor — the real route is longer."
        )

        return RouteDistances(
            distance=distance,
            elapsed_distances=elapsed,
            geom=geom,
            source="stops_haversine",
        )

    def _compute_route_distances(
        self,
        sorted_stop_times: List[StopTime],
        stop_times_for_trip: pd.DataFrame | None,
        shape_geom: LineString | None,
        session: Session,
        route_name: str,
    ) -> RouteDistances:
        """Pick one source per route and return a self-consistent triple.

        Order: A → B → C → hard failure. ``AmbiguousProjectionError`` from
        Source B is caught here and converted into a fall-through to C
        with a WARNING (the projection is unreliable for this route, but
        Source C still gives a meaningful estimate).
        """
        n = len(sorted_stop_times)
        if n == 0:
            raise ValueError(f"Route {route_name!r} has no stop times.")
        if n == 1:
            raise ValueError(f"Route {route_name!r} has only one stop; cannot compute distances.")

        # Source A
        result = self._source_a_shape_dist_traveled(
            sorted_stop_times, stop_times_for_trip, shape_geom, session, route_name
        )
        if result is not None:
            return result

        # Source B
        if shape_geom is not None:
            try:
                result = self._source_b_shape_projection(sorted_stop_times, shape_geom, session, route_name)
            except AmbiguousProjectionError as e:
                self.logger.warning(
                    f"Route {route_name!r}: shape projection ambiguous ({e}); "
                    f"falling through to Source C (stops-as-line haversine)."
                )
                result = None
            if result is not None:
                return result

        # Source C
        result = self._source_c_stops_haversine(sorted_stop_times, route_name)
        if result is not None:
            return result

        # Hard failure: nothing worked. Be specific about why.
        missing = []
        if stop_times_for_trip is None or "shape_dist_traveled" not in stop_times_for_trip.columns:
            missing.append("no stop_times.shape_dist_traveled")
        if shape_geom is None:
            missing.append("no shapes.txt geometry")
        no_geom_idx = [i for i, st in enumerate(sorted_stop_times) if st.station.geom is None]
        if no_geom_idx:
            missing.append(f"stops missing coordinates at indices {no_geom_idx}")
        raise ValueError(
            f"Route {route_name!r}: cannot derive route.distance / elapsed_distance "
            f"from any source ({'; '.join(missing) or 'unknown reason'}). "
            f"Fix the GTFS feed upstream."
        )

    def _dump_route_geojson(
        self,
        dump_dir: str,
        route_name: str,
        shape_geom: "LineString | None",
        sorted_stop_times: List[StopTime],
        distances: List[float],
        target_total_distance: float,
    ) -> None:
        """Dump the route shape and stop coordinates to a GeoJSON file.

        The output is a FeatureCollection containing one LineString feature
        for the GTFS shape and one Point feature per stop. Each feature
        carries diagnostic properties (stop_sequence, projected
        elapsed_distance, target_total_distance, etc.) so the offending
        geometry can be inspected in QGIS / geojson.io.
        """
        import json
        import re
        from pathlib import Path

        out_dir = Path(dump_dir)
        out_dir.mkdir(parents=True, exist_ok=True)
        safe_name = re.sub(r"[^A-Za-z0-9_-]+", "_", route_name)[:80] or "unnamed"
        out_path = out_dir / f"{safe_name}.geojson"

        features: list[dict[str, Any]] = []
        # Shape: 2D coords for clean rendering (Z is irrelevant for visual diagnosis).
        # When shape_geom is None (haversine / uniform fallback), emit a
        # placeholder Feature so the GeoJSON still renders the stops alone.
        if shape_geom is not None:
            shape_coords = [[float(c[0]), float(c[1])] for c in shape_geom.coords]
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "kind": "shape",
                        "route_name": route_name,
                        "n_vertices": len(shape_coords),
                        "target_total_distance": target_total_distance,
                        "closed_loop": shape_coords[0] == shape_coords[-1],
                    },
                    "geometry": {"type": "LineString", "coordinates": shape_coords},
                }
            )
        # Stations: each as a Point with stop_sequence + projected
        # elapsed_distance so it's clear where each stop landed.
        for i, st in enumerate(sorted_stop_times):
            geom = st.station.geom
            if geom is None:
                continue
            try:
                pt = to_shape(geom)  # type: ignore[arg-type]
            except Exception:  # noqa: BLE001
                continue
            features.append(
                {
                    "type": "Feature",
                    "properties": {
                        "kind": "stop",
                        "stop_sequence": i,
                        "stop_id": getattr(st.station, "name_short", None),
                        "station_name": getattr(st.station, "name", None),
                        "projected_elapsed_distance": distances[i],
                        "delta_to_target": distances[i] - target_total_distance,
                    },
                    "geometry": {
                        "type": "Point",
                        "coordinates": [float(pt.x), float(pt.y)],
                    },
                }
            )
        out_path.write_text(json.dumps({"type": "FeatureCollection", "features": features}))
        self.logger.warning(
            f"Dumped offender GeoJSON for route {route_name!r} -> {out_path} "
            f"(max distance {max(distances):.6f}, target {target_total_distance:.6f})"
        )

    def _preprocess_stop_times_by_trip(self, stop_times_df: pd.DataFrame) -> Dict[str, pd.DataFrame]:
        """
        Pre-process stop_times DataFrame by grouping by trip_id and sorting by stop_sequence.
        This avoids repeated filtering and sorting operations during trip creation.

        :param stop_times_df: DataFrame containing stop_times data
        :return: Dictionary mapping trip_id to sorted DataFrame of stop_times for that trip
        """
        self.logger.info("Pre-processing stop_times by trip_id")
        stop_times_dict = {}

        for trip_id, group in stop_times_df.groupby("trip_id"):
            assert isinstance(trip_id, str)
            stop_times_dict[trip_id] = group.sort_values("stop_sequence").reset_index(drop=True)

        self.logger.info(f"Pre-processed stop_times for {len(stop_times_dict)} trips")
        return stop_times_dict

    def stagger_simultaneous_stop_times(self, stop_times_to_add: List[StopTime]) -> List[StopTime]:
        """
        Takes in a list of StopTime objects and adjusts their arrival times slightly if there are multiple
        arrivals at the same time to ensure that no two StopTime objects have the exact same arrival time.

        The first stop and the last stop are never adjusted, as they represent the start and end of the trip.

        :param stop_times_to_add: A list of StopTime objects
        :return: A list of StopTime objects with adjusted arrival times.
        """
        if len(stop_times_to_add) <= 2:
            return stop_times_to_add

        # Sort by arrival time to maintain temporal order
        sorted_stop_times = sorted(stop_times_to_add, key=lambda st: st.arrival_time)

        # Group stop times by arrival time
        from collections import defaultdict

        time_groups = defaultdict(list)
        for i, st in enumerate(sorted_stop_times):
            time_groups[st.arrival_time].append(i)

        # Process each group of duplicates
        for arrival_time, indices in time_groups.items():
            if len(indices) <= 1:
                continue  # No duplicates

            # Check if first or last stop is in this group
            has_first = 0 in indices
            has_last = (len(sorted_stop_times) - 1) in indices

            if has_last and not has_first:
                # Last stop is in the group, adjust earlier duplicates backward
                # Keep the last occurrence (which is the last stop), adjust earlier ones
                for i, idx in enumerate(indices[:-1]):  # All except the last
                    sorted_stop_times[idx].arrival_time = arrival_time - timedelta(seconds=len(indices) - 1 - i)
            elif has_first and not has_last:
                # First stop is in the group, adjust later duplicates forward
                # Keep the first occurrence (which is the first stop), adjust later ones
                for i, idx in enumerate(indices[1:], start=1):  # All except the first
                    sorted_stop_times[idx].arrival_time = arrival_time + timedelta(seconds=i)
            elif has_first and has_last:
                # Both first and last are in the group - adjust middle duplicates
                middle_indices = [idx for idx in indices if idx != 0 and idx != len(sorted_stop_times) - 1]
                for i, idx in enumerate(middle_indices, start=1):
                    sorted_stop_times[idx].arrival_time = arrival_time + timedelta(seconds=i)
            else:
                # Neither first nor last in the group, adjust later occurrences forward
                # Keep the first occurrence in the group, adjust others
                for i, idx in enumerate(indices[1:], start=1):
                    sorted_stop_times[idx].arrival_time = arrival_time + timedelta(seconds=i)

        return sorted_stop_times


if __name__ == "__main__":
    # Echo the pid and sleep for 10 seconds
    pid = os.getpid()
    print(f"GTFS Ingester Test PID: {pid}")
    remain = 10
    while remain > 0:
        print(f"Sleeping... {remain} seconds remaining")
        time.sleep(1)
        remain -= 1

    # Simple test code to verify functionality
    ingester = GtfsIngester(database_url="sqlite:///./test_gtfs_ingester.db")
    engine = create_engine(ingester.database_url)
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)

    # Path to a sample GTFS zip file (update this path as needed)
    sample_gtfs_zip = pl.Path("../../samples/GTFS/VBB.zip").resolve().absolute()

    sys.setrecursionlimit(100)
    logger = logging.getLogger("eflips.ingest.gtfs")
    logging.basicConfig(level=logging.WARNING)

    success, result = ingester.prepare(
        progress_callback=lambda p: logger.debug(f"Prepare progress: {p*100:.1f}%"),
        gtfs_zip_file=sample_gtfs_zip,
        start_date="2025-12-08",
        # start_date="2008-01-01",
        duration="WEEK",
        agency_name="Berliner Verkehrsbetriebe",
    )

    if success:
        ingestion_uuid = result
        assert isinstance(ingestion_uuid, UUID)
        print(f"Preparation successful. UUID: {ingestion_uuid}")
        ingester.ingest(
            ingestion_uuid,
            progress_callback=lambda p: logger.debug(f"Ingest progress: {p*100:.1f}%"),
            always_flush=False,
        )
        print("Ingestion completed successfully.")
    else:
        error_dict = result
        assert isinstance(error_dict, dict)
        print("Preparation failed with errors:")
        for key, message in error_dict.items():
            print(f" - {key}: {message}")
