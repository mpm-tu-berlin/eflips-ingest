import gtfs_kit as gk  # type: ignore [import-untyped]
import logging
import os
import pandas as pd
import pathlib as pl
import pickle
import sys
import time
import uuid
import warnings
from datetime import date as date_type
from datetime import datetime, timedelta
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
from enum import Enum
from geoalchemy2.shape import from_shape
from gtfs_kit import Feed
from pathlib import Path
from shapely.geometry import Point  # type: ignore [import-untyped]
from sqlalchemy.orm import Session
from typing import Dict, Callable, Tuple, List, Any
from uuid import UUID, uuid4
from zoneinfo import ZoneInfo

from eflips.ingest.base import AbstractIngester


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
        agency_name: str = "",
        bus_only: bool = True,
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
        :param agency_name: Name of the agency to import (required if feed contains multiple agencies)
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

        # Handle multi-agency feeds
        agency_filter_result = self.filter_feed_by_agency(feed, agency_name)
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
        }

        with open(save_path / "gtfs_data.dill", "wb") as f:
            pickle.dump(data_to_save, f, protocol=pickle.HIGHEST_PROTOCOL)

        return (True, ingestion_uuid)

    @staticmethod
    def parse_gtfs_time(time_str: str, base_date: datetime) -> datetime:
        """
        Parse GTFS time format (HH:MM:SS) which can exceed 24 hours.

        :param time_str: Time string in HH:MM:SS format
        :param base_date: Base datetime to add the time to (should have timezone info)
        :return: Datetime object representing the parsed time
        """
        parts = time_str.split(":")
        hours = int(parts[0])
        minutes = int(parts[1])
        seconds = int(parts[2])
        return base_date + timedelta(hours=hours, minutes=minutes, seconds=seconds)

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
                name=f"GTFS Import from {str(gtfs_zip_file)} @ {datetime.now().isoformat()}",
                description="Imported from GTFS feed",
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
            for idx, stop_row in stops_df.iterrows():
                stop_id = stop_row["stop_id"]
                stop_name = stop_row["stop_name"]
                stop_lat = stop_row.get("stop_lat")
                stop_lon = stop_row.get("stop_lon")

                # Try go go up to parent_station if available
                root_parent_stop_id = None
                parent_station_id = stop_row.get("parent_station")
                if self.parent_station_id_exists(parent_station_id):
                    parent_stop_row = stops_df.loc[stops_df["stop_id"] == parent_station_id]
                    # we may have to go up one more level
                    grandparent_station_id = parent_stop_row.iloc[0].get("parent_station")
                    if self.parent_station_id_exists(grandparent_station_id):
                        parent_stop_row = stops_df.loc[stops_df["stop_id"] == grandparent_station_id]
                        # Make sure we are at the top-level parent
                        great_grandparent_station_id = parent_stop_row.iloc[0].get("parent_station")
                        if self.parent_station_id_exists(great_grandparent_station_id):
                            raise ValueError(
                                f"More than two levels of parent_station found for stop_id {stop_id}. "
                                f"GTFS feed must not have more than two levels of station hierarchy."
                            )

                    # We now have the root parent. Check if it already exists in stations_dict
                    root_parent_stop_id = parent_stop_row.iloc[0]["stop_id"]
                    if root_parent_stop_id in stations_dict:
                        station_from_gtfs = stations_dict[root_parent_stop_id]
                        # if the station already exists, we can continue here and not create a new one
                        stations_dict[stop_id] = station_from_gtfs
                        continue
                    else:
                        # Create new Station for the parent
                        stop_name = parent_stop_row.iloc[0]["stop_name"]
                        stop_lat = parent_stop_row.iloc[0].get("stop_lat")
                        stop_lon = parent_stop_row.iloc[0].get("stop_lon")
                        stop_short_name = root_parent_stop_id
                else:
                    stop_short_name = stop_id

                # Create Point geometry if coordinates are available
                geom = None
                if pd.notna(stop_lat) and pd.notna(stop_lon):
                    point = Point(stop_lon, stop_lat)  # Note: lon, lat order for Point
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

            # Step 2: Create Line objects from routes (one line per route_id)
            self.logger.info("Creating Line objects from GTFS routes")
            routes_df = feed.routes
            for idx, route_row in routes_df.iterrows():
                route_id = route_row["route_id"]
                route_short_name = route_row.get("route_short_name", route_id)
                route_long_name = route_row.get("route_long_name", "")

                # Handle pandas NA values properly
                if pd.isna(route_long_name) or route_long_name == "":
                    line_name = route_short_name if not pd.isna(route_short_name) else route_id
                else:
                    line_name = route_long_name

                line = Line(
                    name=line_name,
                    name_short=route_short_name if not pd.isna(route_short_name) else route_id,
                    scenario=scenario,
                )
                lines_dict[route_id] = line
                session.add(line)
                if always_flush:
                    session.flush()

            self.logger.info(f"Created {len(lines_dict)} lines")
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

            # Build trip endpoints using pre-sorted stop_times
            trip_endpoints = []
            for idx, trip_row in trips_df.iterrows():
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
            trip_endpoints_dict = {row["trip_id"]: row for _, row in trip_endpoints_df.iterrows()}

            # Create unique routes based on (route_id, direction_id, first_stop, last_stop)
            unique_routes = (
                trip_endpoints_df.groupby(["route_id", "direction_id", "first_stop", "last_stop", "stop_sequence"])
                .first()
                .reset_index()
            )

            for idx, route_row in unique_routes.iterrows():
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

                # Calculate distance (we'll compute this from shape geometry or stop_times shape_dist_traveled)
                # For now, use a placeholder and update later
                distance = 1000.0  # Placeholder

                # Get route geometry from shapes if available
                route_geom = None
                shapely_geom = None
                if route_geometries is not None and pd.notna(shape_id) and shape_id in route_geometries:
                    try:
                        shapely_geom = route_geometries[shape_id]
                        route_geom = from_shape(shapely_geom, srid=4326)
                    except Exception as e:
                        self.logger.warning(f"Failed to convert shape {shape_id} to geometry: {e}")

                # If we have geometry, calculate distance from it
                # The Route model requires that geom length equals distance field (geodetic meters)
                if route_geom is not None and shapely_geom is not None:
                    # Calculate geodetic length directly using pyproj (more performant than GeoDataFrame conversion)
                    from pyproj import Geod

                    geod = Geod(ellps="WGS84")
                    distance = abs(geod.geometry_length(shapely_geom))
                else:
                    # No geometry available, try to get distance from shape_dist_traveled
                    route_trips = trip_endpoints_df[
                        (trip_endpoints_df["route_id"] == gtfs_route_id)
                        & (trip_endpoints_df["direction_id"] == direction_id)
                        & (trip_endpoints_df["first_stop"] == first_stop_id)
                        & (trip_endpoints_df["last_stop"] == last_stop_id)
                    ]

                    if len(route_trips) > 0:
                        sample_trip_id = route_trips.iloc[0]["trip_id"]
                        sample_trip_stops = stop_times_by_trip.get(sample_trip_id)

                        if sample_trip_stops is not None and "shape_dist_traveled" in sample_trip_stops.columns:
                            max_dist = sample_trip_stops["shape_dist_traveled"].max()
                            if pd.notna(max_dist) and max_dist > 0:
                                distance = float(max_dist)

                route = Route(
                    name=f"{line.name_short} → {arrival_station.name}" if line else f"Route to {arrival_station.name}",
                    name_short=f"{gtfs_route_id}_{direction_id}",
                    headsign=headsign if headsign else None,
                    distance=distance,
                    geom=route_geom,
                    departure_station=departure_station,
                    arrival_station=arrival_station,
                    line=line,
                    scenario=scenario,
                )
                routes_dict[route_key] = route
                session.add(route)
                if always_flush:
                    session.flush()

            self.logger.info(f"Created {len(routes_dict)} routes")
            current_progress += PROGRESS_ROUTES
            if progress_callback:
                progress_callback(current_progress)

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

            # Step 5: Create Rotation objects (group trips by block_id or service_id)
            # If block_id exists, use it; otherwise use service_id
            self.logger.info("Creating Rotation objects")
            rotation_groups = (
                trips_df.groupby(["service_id", "block_id"])
                if "block_id" in trips_df.columns
                else trips_df.groupby("service_id")
            )

            # These are "template" rotations that will hold trips; specific rotations per day may be created later
            # So we will need to remember to delete these if they end up unused
            template_rotations_delete_later: List[Rotation] = []

            for group_key, group_trips in rotation_groups:
                if isinstance(group_key, tuple):
                    service_id, block_id = group_key
                    rotation_key: str = f"{service_id}_{block_id}" if pd.notna(block_id) else f"{service_id}_default"
                else:
                    service_id = group_key
                    rotation_key = f"{service_id}_default"

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

            self.logger.info(f"Created {len(rotations_dict)} rotations")
            current_progress += PROGRESS_ROTATIONS
            if progress_callback:
                progress_callback(current_progress)

            # Step 5.5: Compute trip activity for the selected dates
            # This uses gtfs_kit's built-in functionality to handle calendar and calendar_dates
            self.logger.info(f"Computing trip activity for {len(dates_to_import)} dates")
            trip_activity_df = feed.compute_trip_activity(dates_to_import)

            # Create a dict mapping trip_id to list of dates it runs on
            trip_dates_dict: Dict[str, List[str]] = {}
            for _, row in trip_activity_df.iterrows():
                trip_id = row["trip_id"]
                active_dates = [date for date in dates_to_import if row[date] == 1]
                if active_dates:  # Only include trips that run on at least one date
                    trip_dates_dict[trip_id] = active_dates

            self.logger.info(f"Found {len(trip_dates_dict)} active trips")
            current_progress += PROGRESS_TRIP_ACTIVITY
            if progress_callback:
                progress_callback(current_progress)

            # Step 6: Create Trip objects (one instance per trip per date it runs)
            self.logger.info("Creating Trip objects")
            trips_start_progress = current_progress
            total_trips = len(trips_df)
            for trip_idx, (idx, trip_row) in enumerate(trips_df.iterrows()):
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
                    rotation_key_without_day: str = (
                        f"{service_id}_{block_id}" if pd.notna(block_id) else f"{service_id}_default"
                    )
                    rotation_key_with_day = rotation_key_without_day + f"_{date_str}"
                    # See if the specific rotation for this day exists
                    rotation = rotations_dict.get(rotation_key_with_day)

                    if rotation is None:
                        # Check if the general rotation exists
                        rotation = rotations_dict.get(rotation_key_without_day)

                        if rotation is None:
                            # We will need to create a new template rotation containing just this one trip
                            rotation = Rotation(
                                name=f"TEMPLATE DUMMY Single-Trip Rotation for {trip_id}",
                                vehicle_type=default_vehicle_type,
                                allow_opportunity_charging=False,
                                scenario=scenario,
                            )
                            rotations_dict[uuid4().hex] = rotation  # Store with a unique key
                        else:
                            # We will create a copy of the existing rotation for this specific day
                            rotation = Rotation(
                                name=f"{rotation.name} for {date_str}",
                                vehicle_type=rotation.vehicle_type,
                                allow_opportunity_charging=rotation.allow_opportunity_charging,
                                scenario=scenario,
                            )
                            rotations_dict[rotation_key_with_day] = rotation
                        session.add(rotation)
                        if always_flush:
                            session.flush()

                    # Parse the GTFS date string (YYYYMMDD) to get the actual date
                    date_obj = datetime.strptime(date_str, "%Y%m%d").date()
                    # Create base datetime for this specific date
                    base_date = datetime(date_obj.year, date_obj.month, date_obj.day, tzinfo=tz)

                    departure_time = self.parse_gtfs_time(first_arrival_str, base_date)

                    # For arrival time, use the departure time from the last stop if available
                    if pd.notna(last_departure_str) and last_departure_str:
                        arrival_time = self.parse_gtfs_time(last_departure_str, base_date)
                    else:
                        # Use arrival time of last stop
                        last_arrival_str = trip_stops.iloc[-1]["arrival_time"]
                        arrival_time = self.parse_gtfs_time(last_arrival_str, base_date)

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

                    # Make sure the rotation's trips are sorted by departure time
                    trip_from_gtfs.rotation.trips = sorted(
                        trip_from_gtfs.rotation.trips, key=lambda t: t.departure_time
                    )

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
            for trip_idx, (idx, trip_row) in enumerate(trips_df.iterrows()):
                trip_id = trip_row["trip_id"]

                # Skip trips that don't run on any of our selected dates
                if trip_id not in trip_dates_dict:
                    continue

                for date_str in trip_dates_dict[trip_id]:
                    trip_instance_key = f"{trip_id}_{date_str}"
                    trip = trips_dict.get(trip_instance_key)

                    if trip is None:
                        raise ValueError(f"Trip instance not found for key {trip_instance_key}")

                    # Parse the GTFS date string (YYYYMMDD) to get the actual date
                    date_obj = datetime.strptime(date_str, "%Y%m%d").date()
                    # Create base datetime for this specific date
                    base_date = datetime(date_obj.year, date_obj.month, date_obj.day, tzinfo=tz)

                    stop_times_to_add = []
                    trip_stop_times = stop_times_by_trip.get(trip_id)
                    if trip_stop_times is None:
                        raise ValueError(f"No stop times found for trip {trip_id}")

                    for idx, stop_time_row in trip_stop_times.iterrows():
                        trip_id = stop_time_row["trip_id"]

                        # Skip trips that don't run on any of our selected dates
                        if trip_id not in trip_dates_dict:
                            raise ValueError(f"Trip {trip_id} not found in active trips for stop times")

                        stop_id = stop_time_row["stop_id"]
                        arrival_time_str = stop_time_row["arrival_time"]
                        departure_time_str = stop_time_row.get("departure_time", arrival_time_str)

                        station = stations_dict.get(stop_id)
                        if station is None:
                            raise ValueError(f"Station not found for stop_id {stop_id} in trip {trip_id}")

                        arrival_time = self.parse_gtfs_time(arrival_time_str, base_date)

                        # Calculate dwell duration
                        if pd.notna(departure_time_str) and departure_time_str:
                            departure_time = self.parse_gtfs_time(departure_time_str, base_date)
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

                # Calculate elapsed distances
                # If we have shape_dist_traveled, use that; otherwise, distribute evenly
                # Get the GTFS trip_id for the sample trip
                trip_id_gtfs = [k for k, v in trips_dict.items() if v == sample_trip][0]

                # Remove the date part to get the original trip_id
                trip_id_gtfs = trip_id_gtfs[:-9]

                stop_times_for_trip = stop_times_by_trip.get(trip_id_gtfs)

                # We cannot add them one by one because we would be (temporarily) violating distance ordering
                assocs_to_add = []
                for i, stop_time in enumerate(sorted_stop_times):
                    elapsed_distance = None

                    # Try to get shape_dist_traveled from the original data
                    if stop_times_for_trip is not None and "shape_dist_traveled" in stop_times_for_trip.columns:
                        # Find the matching stop in the dataframe by stop_id
                        stop_id_to_find = stop_time.station.name_short  # We stored the stop_id in name_short
                        matching_stops = stop_times_for_trip[stop_times_for_trip["stop_id"] == stop_id_to_find]

                        if len(matching_stops) > 0:
                            stop_row = matching_stops.iloc[0]
                            if pd.notna(stop_row["shape_dist_traveled"]):
                                elapsed_distance = float(stop_row["shape_dist_traveled"])

                    # If we didn't get a distance from shape_dist_traveled, distribute evenly
                    if elapsed_distance is None:
                        elapsed_distance = (i / max(len(sorted_stop_times) - 1, 1)) * route.distance

                    assoc_route_station = AssocRouteStation(
                        route=route, station=stop_time.station, elapsed_distance=elapsed_distance, scenario=scenario
                    )
                    assocs_to_add.append(assoc_route_station)

                # Add to route's assoc_route_stations list

                # Make sure the first and last stations are exactly at 0 and route.distance, warning is it's changed
                if assocs_to_add[0].elapsed_distance != 0.0:
                    # Only warn if the difference is significant (> 50 meters)
                    difference = abs(assocs_to_add[0].elapsed_distance - 0.0)
                    warn_string = (
                        f"First station distance for route {route.name} adjusted from "
                        f"{assocs_to_add[0].elapsed_distance:.0f} to 0.0"
                    )
                    if difference > 50.0:
                        self.logger.warning(warn_string)
                    else:
                        self.logger.info(warn_string)
                    assocs_to_add[0].elapsed_distance = 0.0
                if assocs_to_add[-1].elapsed_distance != route.distance:
                    # Only warn if the difference is significant (> 50 meters)
                    difference = abs(assocs_to_add[-1].elapsed_distance - route.distance)
                    warn_string = (
                        f"Last station distance for route {route.name} adjusted from "
                        f"{assocs_to_add[-1].elapsed_distance:.0f} to {route.distance:.0f}"
                    )
                    if difference > 50.0:
                        self.logger.warning(warn_string)
                    else:
                        self.logger.info(warn_string)
                    assocs_to_add[-1].elapsed_distance = route.distance

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
            "bus_only": "Filter to only import bus routes (default: True)",
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
            "bus_only": "If True (default), only bus routes will be imported from the GTFS feed. This includes routes "
            "with route_type of 3 (standard GTFS bus) or 700-799 (extended GTFS bus types). If False, all route types "
            "will be imported. If set to True and the feed contains no bus routes, an error will be returned.",
        }

    def filter_feed_by_agency(self, feed: Feed, agency_name: str | None) -> Feed | Tuple[bool, Dict[str, str]]:
        """
        Filter the GTFS feed to include only data from a specific agency.

        If the feed contains multiple agencies and no agency_name is specified,
        returns an error tuple with available agency names.

        If the feed contains only one agency, the agency_name parameter is ignored.

        :param feed: A gtfs_kit Feed object
        :param agency_name: Name of the agency to filter by (optional if single-agency feed)
        :return: Filtered Feed object, or error tuple (False, error_dict)
        """
        if feed.agency is None or len(feed.agency) == 0:
            return (False, {"agency": "No agency information found in GTFS feed"})

        num_agencies = len(feed.agency)

        # Single agency - no filtering needed
        if num_agencies == 1:
            if agency_name and agency_name != "":
                self.logger.info(f"Feed contains only one agency, ignoring agency_name parameter '{agency_name}'")
            return feed

        # Multiple agencies - agency_name is required
        if not agency_name or agency_name == "":
            # Build helpful error message with list of available agencies
            agency_names = feed.agency["agency_name"].tolist()
            agency_list = "\n".join(f"  - {name}" for name in agency_names)
            error_msg = (
                f"The GTFS feed contains {num_agencies} agencies. "
                f"Please specify which agency to import using the 'agency_name' parameter.\n\n"
                f"Available agencies:\n{agency_list}"
            )
            return (False, {"agency_name": error_msg})

        # Find the agency by name
        matching_agencies = feed.agency[feed.agency["agency_name"] == agency_name]

        if len(matching_agencies) == 0:
            # Agency name not found
            agency_names = feed.agency["agency_name"].tolist()
            agency_list = "\n".join(f"  - {name}" for name in agency_names)
            error_msg = f"Agency '{agency_name}' not found in GTFS feed.\n\n" f"Available agencies:\n{agency_list}"
            return (False, {"agency_name": error_msg})

        # Get the agency_id for filtering
        agency_id = matching_agencies.iloc[0]["agency_id"]
        self.logger.info(f"Filtering feed to agency '{agency_name}' (ID: {agency_id})")

        # Use gtfs_kit's restrict_to_agencies to filter the feed
        try:
            filtered_feed = feed.restrict_to_agencies([agency_id])
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
            return geometry_by_shape
        except Exception as e:
            self.logger.warning(f"Failed to build route geometries: {e}")
            return None

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
