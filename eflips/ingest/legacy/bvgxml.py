#!/usr/bin/env python3
import glob
import logging
import os
import socket
import statistics
import warnings
import zoneinfo
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from multiprocessing import Pool
from pathlib import Path
from typing import Dict, List, Tuple, Union

import eflips.model
import fire  # type: ignore
import psycopg2
from eflips.model import ConsistencyWarning, Station, Route, AssocRouteStation, StopTime
from geoalchemy2 import WKBElement
from geoalchemy2.functions import ST_Distance
from geoalchemy2.shape import to_shape
from lxml import etree
from sqlalchemy import create_engine, func
from sqlalchemy.orm import Session
from tqdm.auto import tqdm
from xsdata.formats.dataclass.parsers import XmlParser

import eflips.ingest.util
from eflips.ingest.legacy.xmldata import (
    Linienfahrplan,
    NetzpunktNetzpunkttyp,
)
from eflips.ingest.util import soldner_to_pointz


def load_and_validate_xml(filename: Path) -> Linienfahrplan:
    """
    Loads the xml string into a Linienfahrplan object and validates it against the schema
    - also removes the 'ns2' strings from the first and last lines

    :param filename: the filename to load

    :return: a Linienfahrplan object of there is data, None otherwise. raises a ValueError if the data is there but
             not valid
    """
    xml_string = filename.read_text()
    logger = logging.getLogger(__name__)

    if "Keine gültige Linie." in xml_string:
        raise ValueError(f"File {filename} is not a valid line.")
    elif "Keine Umläufe vorhanden." in xml_string:
        raise ValueError(f"File {filename} does not contain any rotations.")

    # We want to remove the occurrences of 'ns2: and ':ns2' in the first and last line. Only then does our schema
    # and the 'xmldata' package work. For some reason, with the ns2 it generates two differnet python files, and
    # then it doesn't work.
    xml_string = xml_string.replace("ns2:", "")
    xml_string = xml_string.replace(":ns2", "")

    xsd_path = Path(__file__).parent.parent.parent.parent / "data" / "bvg_xml.xsd"
    xmlschema_doc = etree.parse(xsd_path)
    xmlschema = etree.XMLSchema(xmlschema_doc)

    xml_doc = etree.fromstring(xml_string)

    if not xmlschema.validate(xml_doc):
        raise ValueError(f"XML file {filename} is not valid.")

    parser = XmlParser()

    data: Linienfahrplan = parser.from_string(xml_string, Linienfahrplan)

    return data


def add_or_ret_station(scenario_id: int, id: int, name: str, name_short: str, session: Session) -> eflips.model.Station:
    """
    For a given station ID, adds a station to the database if it does not exist yet, and returns the station object.

    :param scenario_id: The scenario ID to use
    :param id: The station ID to use
    :param name: The long name of the station
    :param name_short: The short name of the station
    :param session: An open database session
    :return: A station object, connected to the database it will not have a geometry yet
    """

    station = (
        session.query(eflips.model.Station)
        .filter(eflips.model.Station.scenario_id == scenario_id)
        .filter(eflips.model.Station.id == id)
        .one_or_none()
    )
    if station is None:
        station = eflips.model.Station(
            scenario_id=scenario_id,
            id=id,
            name=name,
            name_short=name_short,
            is_electrified=False,
            geom="SRID=4326;POINTZ(0 0 0)",  # Will be set later
        )
        session.add(station)
    return station


def create_stations(linienfahrplan: Linienfahrplan, scenario_id: int, session: Session) -> None:
    """
    First method to be used when importing a set of xml files. It takes the parsed xml data and creates the stations
    from the 'Linienfahrplan/StreckennetzDaten/Haltestellenbereiche/Haltestellenbereich' entries.

    :param linienfahrplan: A parsed Linienfahrplan object
    :param scenario_id: The scenario ID to use
    :param session: An open database session
    :return: Nothing - the stations are added to the database
    """
    logger = logging.getLogger(__name__)

    for haltestellenbereich in linienfahrplan.streckennetz_daten.haltestellenbereiche.haltestellenbereich:
        id_no = haltestellenbereich.nummer
        short_name = haltestellenbereich.kurzname
        long_name = haltestellenbereich.fahrplanbuchname

        add_or_ret_station(scenario_id, id_no, long_name, short_name, session)


def add_or_ret_line(scenario_id: int, name: str, session: Session) -> eflips.model.Line:
    line = (
        session.query(eflips.model.Line)
        .filter(eflips.model.Line.scenario_id == scenario_id)
        .filter(eflips.model.Line.name == name)
        .one_or_none()
    )
    if line is None:
        line = eflips.model.Line(scenario_id=scenario_id, name=name)
        session.add(line)
    return line


def add_or_ret_station_for_grid_point(
    scenario_id: int,
    gridpoint_id: int,
    grid_points: Dict[int, Linienfahrplan.StreckennetzDaten.Netzpunkte.Netzpunkt],
    session: Session,
) -> eflips.model.Station:
    """
    Sets up the station object for a given grid point. If it is if the `Netzpunkttyp` "Hst", returns the station.
    Otherwise, check if a station already exists for the grid point (by short name). If not, create a new station
    """

    logger = logging.getLogger(__name__)

    grid_point = grid_points[gridpoint_id]
    if grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.HST:
        station = (
            session.query(eflips.model.Station)
            .filter(eflips.model.Station.scenario_id == scenario_id)
            .filter(eflips.model.Station.id == grid_point.haltestellenbereich)
            .one_or_none()
        )
        if station is None:
            # The station should have already been created in the previous step
            raise ValueError(f"Station for grid point {gridpoint_id} not found, even though it is of type 'Hst'")
    elif grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.BPUNKT:
        # Assumption: A "Betriebspunkt" basically belongs to the station for our purposes
        # We query the station by the four-character short name
        short_name = grid_point.kurzname[0:4]
        if short_name.endswith("0") or short_name.endswith("1"):
            # The 0 or 1 seems to be added to the short name for bpunkts corresponding to stations which actually have a
            # three-character short name. if we shorten the short name to three characters, we can find the station
            short_name = short_name[0:3]
        station = (
            session.query(eflips.model.Station)
            .filter(eflips.model.Station.scenario_id == scenario_id)
            .filter(eflips.model.Station.name_short == short_name)
            .one_or_none()
        )
        if station is None:
            logger.info(
                f"Station for grid point {gridpoint_id} not found, even though it is of type 'BPUNKT' and should have a station"
            )
            # Here, we can actually calculate the coordinates already
            geom = soldner_to_pointz(grid_point.xkoordinate, grid_point.ykoordinate)

            station = eflips.model.Station(
                scenario_id=scenario_id,
                name=f"BPUNKT {grid_point.langname}",
                name_short=short_name,
                is_electrified=False,
                geom=geom,
            )

            # raise ValueError(
            #    f"Station for grid point {gridpoint_id} not found, even though it is of type 'BPUNKT' and should have a station"
            # )
    elif grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.EPKT or grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.APKT:
        # We want to merge the Einsetzpunkt and Aussetzpunkt stations into one station
        # Make sure the short name ends in "E" or "A", then remove that character
        short_name = grid_point.kurzname
        if short_name[-1] != "E" and short_name[-1] != "A":
            raise ValueError(
                f"Station for grid point {gridpoint_id} not found, even though it is of type 'EPKT' or 'APKT' and should have a station"
            )
        short_name = short_name[:-2]

        # Make sure the long name ends in "Einsetzen" or "Aussetzen", then remove that word
        long_name = grid_point.langname
        if long_name[-9:] != "Einsetzen" and long_name[-9:] != "Aussetzen":
            raise ValueError(
                f"Station for grid point {gridpoint_id} not found, even though it is of type 'EPKT' or 'APKT' and should have a station"
            )
        long_name = long_name[:-10]

        station = (
            session.query(eflips.model.Station)
            .filter(eflips.model.Station.scenario_id == scenario_id)
            .filter(eflips.model.Station.name_short == short_name)
            .filter(eflips.model.Station.name == long_name)
            .one_or_none()
        )
        if station is None:
            # Here, we can actually calculate the coordinates already
            geom = soldner_to_pointz(grid_point.xkoordinate, grid_point.ykoordinate)

            station = eflips.model.Station(
                scenario_id=scenario_id,
                id=gridpoint_id,
                name=long_name,
                name_short=short_name,
                is_electrified=False,
                geom=geom,
            )
            session.add(station)
    else:
        raise ValueError(f"Grid point {gridpoint_id} is of type {grid_point.netzpunkttyp}, which is not supported")

    return station


def add_or_ret_vehicle_type(scenario_id: int, fahrzeugtyp: str, session: Session) -> eflips.model.VehicleType:
    vehicle_type = (
        session.query(eflips.model.VehicleType)
        .filter(eflips.model.VehicleType.scenario_id == scenario_id)
        .filter(eflips.model.VehicleType.name_short == fahrzeugtyp)
        .one_or_none()
    )
    if vehicle_type is None:
        vehicle_type = eflips.model.VehicleType(
            scenario_id=scenario_id,
            name=f"Auto-Generated by XML Importer for {fahrzeugtyp}",
            name_short=fahrzeugtyp,
            battery_capacity=1000,
            charging_curve=[[0, 150], [1, 150]],
            opportunity_charging_capable=True,
        )
        session.add(vehicle_type)
    return vehicle_type


def setup_working_dictionaries(
    schedule: Linienfahrplan,
) -> Tuple[
    Dict[int, Linienfahrplan.StreckennetzDaten.Netzpunkte.Netzpunkt],
    Dict[int, Linienfahrplan.StreckennetzDaten.Strecken.Strecke],
    Dict[int, Linienfahrplan.LinienDaten.Linie.RoutenDaten.Route],
    Dict[int, int],
]:
    """
    This creates a set of dictionaries that are used to assemble the routes and trips.

    - The first entry is a dict of the "Netzpunkte", by their ID.
    - The second entry is a dict of the "Strecken", by their ID.
    - The third entry is a dict of the "RoutenDaten", by their "lfd_nr"
    - The fourth dictionary is the "Route.lfd_nr" for each "Routenvariante.lfd_nr"

    :param schedule: The parsed Linienfahrplan object
    :return: A tuple of the four dictionaries
    """
    line_name = schedule.linien_daten.linie.kurzname

    # Create a dict from the "Netzpunkzte", to be used in reassembling the routes later on.
    grid_points: Dict[int, Linienfahrplan.StreckennetzDaten.Netzpunkte.Netzpunkt] = {}
    for netzpunkt in schedule.streckennetz_daten.netzpunkte.netzpunkt:
        grid_points[netzpunkt.nummer] = netzpunkt

    # Create a similar dict for the "Strecken", whoch will be the segments we use to assemble the route shape
    segments: Dict[int, Linienfahrplan.StreckennetzDaten.Strecken.Strecke] = {}
    for strecke in schedule.streckennetz_daten.strecken.strecke:
        segments[strecke.id] = strecke

    # Create a similar dict for the route data, which we will use to assemble the trips
    route_datas: Dict[int, Linienfahrplan.LinienDaten.Linie.RoutenDaten.Route] = {}
    for route in schedule.linien_daten.linie.routen_daten.route:
        route_datas[route.lfd_nr] = route

    # Create a list of the route lfd nr for all the variants
    route_lfd_nrs: Dict[int, int] = {}
    for route_variant in schedule.linien_daten.linie.routenvarianten.routenvariante:
        route_lfd_nrs[route_variant.lfd_nr] = route_variant.lfd_nr_route

    return grid_points, segments, route_datas, route_lfd_nrs


@dataclass
class TimeProfile:
    @dataclass
    class TimeProfilePoint:
        station: eflips.model.Station
        arrival_offset_from_start: timedelta
        dwell_duration: timedelta

        def __eq__(self: "TimeProfile.TimeProfilePoint", other: object) -> bool:
            if not isinstance(other, TimeProfile.TimeProfilePoint):
                return False
            if self.station != other.station:
                return False
            if self.arrival_offset_from_start != other.arrival_offset_from_start:
                return False
            if self.dwell_duration != other.dwell_duration:
                return False
            return True

    route: eflips.model.Route
    start_offset_from_midnight: timedelta
    time_profile_points: List[TimeProfilePoint]

    def __eq__(self: "TimeProfile", other: object) -> bool:
        if not isinstance(other, TimeProfile):
            return False
        if self.route != other.route:
            return False
        if self.start_offset_from_midnight != other.start_offset_from_midnight:
            return False
        if len(self.time_profile_points) != len(other.time_profile_points):
            return False
        for i in range(len(self.time_profile_points)):
            if self.time_profile_points[i] != other.time_profile_points[i]:
                return False
        return True

    def to_trip(
        self,
        rotation: eflips.model.Rotation,
        the_date: date,
        timezone: zoneinfo.ZoneInfo = zoneinfo.ZoneInfo("Europe/Berlin"),
    ) -> eflips.model.Trip:
        """
        Create a trip on a given date from this time profile

        :param rotation: The rotation to create the trip for
        :param the_date: The date to use
        :param timezone: The timezone in which the offsets from midnight are given. Defaults to Europe/Berlin
        :return: a trip object, which is not yet added to the database
        """
        logger = logging.getLogger(__name__)

        local_midnight = datetime.combine(the_date, time(0, 0, 0, 0), tzinfo=timezone)

        # Find the trip type fromt the route name:
        # - Einsetzfahrt
        # - Aussetzfahrt
        if "Einsetzfahrt" in self.route.name or "Aussetzfahrt" in self.route.name:
            trip_type = eflips.model.TripType.EMPTY
        else:
            trip_type = eflips.model.TripType.PASSENGER

        # Create the trip object
        trip = eflips.model.Trip(
            scenario_id=self.route.scenario_id,
            route=self.route,
            rotation=rotation,
            departure_time=local_midnight
            + self.start_offset_from_midnight
            + self.time_profile_points[0].arrival_offset_from_start,
            arrival_time=local_midnight
            + self.start_offset_from_midnight
            + self.time_profile_points[-1].arrival_offset_from_start
            + self.time_profile_points[-1].dwell_duration,
            trip_type=trip_type,
        )

        # Sort the time profile points by arrival offset
        self.time_profile_points.sort(key=lambda x: x.arrival_offset_from_start)
        # Sort the assoc_route_stations by elapsed distance
        self.route.assoc_route_stations.sort(key=lambda x: x.elapsed_distance)

        # Create the stoptimes
        for i in range(len(self.time_profile_points)):
            if self.time_profile_points[i].station != self.route.assoc_route_stations[i].station:
                raise ValueError(
                    f"Station {self.time_profile_points[i].station.name} at position {i} does not match the station {self.route.assoc_route_stations[i].station.name} in the route"
                )

            stoptime = eflips.model.StopTime(
                scenario_id=self.route.scenario_id,
                trip=None,  # Will be done manually through append
                station=self.time_profile_points[i].station,
                arrival_time=local_midnight
                + self.start_offset_from_midnight
                + self.time_profile_points[i].arrival_offset_from_start,
                dwell_duration=self.time_profile_points[i].dwell_duration,
            )
            trip.stop_times.append(stoptime)

        return trip


def create_route(
    scenario_id: int,
    departure_station: eflips.model.Station,
    arrival_station: eflips.model.Station,
    line: eflips.model.Line,
    distance: float,
    first_grid_point: Linienfahrplan.StreckennetzDaten.Netzpunkte.Netzpunkt,
    last_grid_point: Linienfahrplan.StreckennetzDaten.Netzpunkte.Netzpunkt,
) -> eflips.model.Route:
    """
    Create a route object, setting the name up from the station

    :param scenario_id: A scenario ID
    :param departure_station: The departure station
    :param arrival_station: The arrival station
    :param line: The line object
    :param distance: The distance of the route
    :param first_grid_point: The first grid point of the route. Used to check if it is an Einsetzpunkt
    :param last_grid_point: The last grid point of the route. Used to check if it is an Aussetzpunkt
    :return: A Route object, which is not yet added to the database
    """

    name_short = line.name + " " + departure_station.name_short + " → " + arrival_station.name_short

    name = line.name + " "
    if first_grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.EPKT:
        name += "Einsetzfahrt "
    elif last_grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.APKT:
        name += "Aussetzfahrt "
    elif (
        first_grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.BPUNKT
        or last_grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.BPUNKT
    ):
        pass
    else:
        assert (
            first_grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.HST
            and last_grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.HST
        ), "Check what the other grid point types might mean"
    name += departure_station.name + " → " + arrival_station.name

    return eflips.model.Route(
        scenario_id=scenario_id,
        departure_station=departure_station,
        arrival_station=arrival_station,
        line=line,
        name=name,
        name_short=name_short,
        headsign=None,
        distance=distance,
        geom=None,
    )


def fix_times(points: List[TimeProfile.TimeProfilePoint]) -> List[TimeProfile.TimeProfilePoint]:
    """
    Take a list of time profile points that might have a identical arrival offsets and fix them by
    taking each set of identical arrival offsets and adding a second to each of them
    """
    # Check if the points are sorted by arrival offset
    for i in range(len(points) - 1):
        if points[i].arrival_offset_from_start > points[i + 1].arrival_offset_from_start:
            raise ValueError("Points are not sorted by arrival offset")

    # Check if there are any identical arrival offsets and fix them
    # Taking into account that there might be multiple identical arrival offsets in a row
    last_arrival_offset = None
    current_increment = 0
    for i in range(len(points)):
        current_arrival_offset = points[i].arrival_offset_from_start
        if last_arrival_offset is not None and current_arrival_offset == last_arrival_offset:
            current_increment += 1
            points[i].arrival_offset_from_start += timedelta(seconds=current_increment)

            # If we are shifting the last point back, instead we need to shift all the points forward which we
            # have shifted back before
            if i == len(points) - 1:
                for j in range(i - current_increment, i + 1):
                    points[j].arrival_offset_from_start -= timedelta(seconds=current_increment)

        else:
            current_increment = 0
        last_arrival_offset = current_arrival_offset

    # Debugging: make sure they are now strictly sorted
    # Check if the points are sorted by arrival offset
    for i in range(len(points) - 1):
        if points[i].arrival_offset_from_start >= points[i + 1].arrival_offset_from_start:
            raise ValueError("Points are not sorted by arrival offset")
    if points[-1].arrival_offset_from_start.total_seconds() % 60 != 0:
        raise ValueError("The last point does not have an arrival offset in full minutes, which it should have")

    return points


def create_routes_and_time_profiles(
    schedule: Linienfahrplan, scenario_id: int, session: Session
) -> Tuple[Dict[int, Dict[int, List[TimeProfile.TimeProfilePoint]]], Dict[int, None | eflips.model.Route]]:
    """
    First method to be used when importing a set of xml files. It takes the parsed xml data and creates the stations
    from the 'Linienfahrplan/LinienDaten/Linie/RoutenDaten/Route' entries.

    This method
    1. Adds the routes (with their corresponding AssocRouteStation objects) to the database
    2. goes through the 'Linienfahrplan/LinienDaten/Linie/RoutenDaten/Route/Fahrzeitprofil' entries and turns it into
       a list of time profiles to be used when creating the trips.
    3. Goes through the 'Linienfahrplan/Liniendaten/Fahrt' entries and assigns one of our time profiles to each trip


    :param schedule: A parsed Linienfahrplan object
    :param scenario_id: The scenario ID to use
    :param session: An open database session
    :return: A tuple of two dictionaries:
             - one contains the points of the time profiles, by their route.lfd_nr in the schedule and then by the
               fahrzeitprofil_nummer
             - the other contains the eflip route objects, by their route.lfd_nr in the schedule
    """
    logger = logging.getLogger(__name__)

    grid_points, segments, route_datas, route_lfd_nrs = setup_working_dictionaries(schedule)

    # Create the line object, if it does not exist yet
    db_line = add_or_ret_line(scenario_id, schedule.linien_daten.linie.kurzname, session)

    # We need to make sure there is only one "Linie" object, otherwise the route numbers are non-unique
    # If this turns into a list, we need to change the code below
    assert isinstance(schedule.linien_daten.linie, Linienfahrplan.LinienDaten.Linie)

    trip_time_profiles: Dict[
        int, Dict[int, List[TimeProfile.TimeProfilePoint]]
    ] = {}  # Will be keyed by route.lfd_nr, then by fahrzeitprofil_nummer
    db_routes_by_lfd_nr: Dict[int, None | eflips.model.Route] = {}  # Will be keyed by route.lfd_nr
    for route in schedule.linien_daten.linie.routen_daten.route:
        # Contrary to the naive approach, we first create the AssocRouteStation objects, and then the route object
        # This way, we are sure the departure and arrival stations match as well as the total distance
        assocs: List[eflips.model.AssocRouteStation] = []
        elapsed_distance = 0.0

        time_profile_points: Dict[
            int, List[TimeProfile.TimeProfilePoint]
        ] = {}  # Order: time profile number, time profile
        elapsed_time: Dict[int, timedelta] = {}
        for fahrzeitprofil in route.fahrzeitprofile.fahrzeitprofil:
            time_profile_points[fahrzeitprofil.fahrzeitprofil_nummer] = []
            elapsed_time[fahrzeitprofil.fahrzeitprofil_nummer] = timedelta(seconds=0)

        for i in range(len(route.punktfolge.punkt)):
            point = route.punktfolge.punkt[i]
            # Load data to be used later
            station = add_or_ret_station_for_grid_point(scenario_id, point.netzpunkt, grid_points, session)
            grid_point = grid_points[point.netzpunkt]
            geom = soldner_to_pointz(grid_point.xkoordinate, grid_point.ykoordinate)

            # Temporal: Update driving times
            driving_times: Dict[int, Tuple[timedelta, timedelta]] = {}  # Order: driving, waiting
            for fahrzeitprofil in route.fahrzeitprofile.fahrzeitprofil:
                driving_time_point = fahrzeitprofil.fahrzeitprofilpunkte.punkt[i]
                driving_times[fahrzeitprofil.fahrzeitprofil_nummer] = (
                    timedelta(seconds=driving_time_point.streckenfahrzeit),
                    timedelta(seconds=driving_time_point.wartezeit),
                )
                elapsed_time[fahrzeitprofil.fahrzeitprofil_nummer] += timedelta(
                    seconds=driving_time_point.streckenfahrzeit
                )

            # Geographic: Update elapsed distance
            if i > 0:
                segment_id = route.streckenfolge.strecke[i - 1].strecken_id
                segment = segments[segment_id]
                elapsed_distance += segment.streckenlaenge

            # SPECIAL FIXES
            # Some routes have a distance of zero even once the last point is reached
            if i == len(route.punktfolge.punkt) - 1 and elapsed_distance == 0:
                # We mark these by putting an obscenely large number in the distance
                logger.warning(f"Route {route.lfd_nr} of line {db_line.name} has a zero distance at the end.")
                elapsed_distance += 1e6 * 1000  # One million kilometers

            # Some time profiles have a zero time at the end
            if i == len(route.punktfolge.punkt) - 1:
                for fahrzeitprofil in route.fahrzeitprofile.fahrzeitprofil:
                    if elapsed_time[fahrzeitprofil.fahrzeitprofil_nummer] == timedelta(seconds=0):
                        logger.info(
                            f"Route {route.lfd_nr} of line {db_line.name} has a zero time at the end. Calculating duration with a fixed speed of 30 km/h."
                        )

                        SPEED = 30 / 3.6  # 30 km/h in m/s
                        if elapsed_distance != 0:
                            duration = elapsed_distance / SPEED
                        else:
                            logger.warning(
                                f"Route {route.lfd_nr} of line {db_line.name} has a zero distance at the end. Calculating duration with a fixed speed of 30 km/h and a distance of 1 km."
                            )
                            duration = 1000 / SPEED

                        # Now, depending on whether it is an EInsetzfahrt or Aussetzfahrt, we shoft the beginning forward
                        # or the end backward
                        first_grid_point = grid_points[route.punktfolge.punkt[0].netzpunkt]
                        last_grid_point = grid_points[route.punktfolge.punkt[-1].netzpunkt]
                        if first_grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.EPKT:
                            # Shift the first entry back by the duration
                            time_profile_points[fahrzeitprofil.fahrzeitprofil_nummer][
                                0
                            ].arrival_offset_from_start = timedelta(seconds=-duration)
                        elif last_grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.APKT:
                            elapsed_time[fahrzeitprofil.fahrzeitprofil_nummer] += timedelta(seconds=duration)
                        else:
                            raise ValueError(
                                f"Route {route.lfd_nr} of line {db_line.name} has a zero time at the end, but is neither an Einsetzfahrt nor an Aussetzfahrt"
                            )

            # We always add the first point
            if i == 0:
                # Geographic
                assoc = eflips.model.AssocRouteStation(
                    scenario_id=scenario_id,
                    route=None,  # Will be set later
                    station=station,
                    location=geom,
                    elapsed_distance=elapsed_distance,
                )
                assocs.append(assoc)

                # Temporal
                # Here, the time driven until this point must be 0
                for fahrzeitprofil in route.fahrzeitprofile.fahrzeitprofil:
                    assert driving_times[fahrzeitprofil.fahrzeitprofil_nummer][0] == timedelta(seconds=0)
                    waiting_time = driving_times[fahrzeitprofil.fahrzeitprofil_nummer][1]
                    time_profile_points[fahrzeitprofil.fahrzeitprofil_nummer].append(
                        TimeProfile.TimeProfilePoint(
                            station=station,
                            arrival_offset_from_start=timedelta(seconds=0),
                            dwell_duration=waiting_time,
                        )
                    )
            # Otherwise, we only add a point if the station and elapsed distance are different from the previous point
            elif station != assocs[-1].station and elapsed_distance != assocs[-1].elapsed_distance:
                # Geographic
                assoc = eflips.model.AssocRouteStation(
                    scenario_id=scenario_id,
                    route=None,  # Will be set later
                    station=station,
                    location=geom,
                    elapsed_distance=elapsed_distance,
                )
                assocs.append(assoc)

                # Temporal
                for fahrzeitprofil in route.fahrzeitprofile.fahrzeitprofil:
                    time_profile_points[fahrzeitprofil.fahrzeitprofil_nummer].append(
                        TimeProfile.TimeProfilePoint(
                            station=station,
                            arrival_offset_from_start=elapsed_time[fahrzeitprofil.fahrzeitprofil_nummer],
                            dwell_duration=timedelta(seconds=0),
                        )
                    )
            # If we are at the last point, we will need to update the elapsed distance and time
            # even if we would not normally add the point
            elif i == len(route.punktfolge.punkt) - 1:
                # Geographic
                assocs[-1].elapsed_distance = elapsed_distance

                # Temporal
                for fahrzeitprofil in route.fahrzeitprofile.fahrzeitprofil:
                    time_profile_points[fahrzeitprofil.fahrzeitprofil_nummer][
                        -1
                    ].arrival_offset_from_start = elapsed_time[fahrzeitprofil.fahrzeitprofil_nummer]

            # In the end, add the waiting time at this point (if any)
            # to the elapsed_time and the dwell_durations of the last time profile point
            for fahrzeitprofil in route.fahrzeitprofile.fahrzeitprofil:
                waiting_time = driving_times[fahrzeitprofil.fahrzeitprofil_nummer][1]
                elapsed_time[fahrzeitprofil.fahrzeitprofil_nummer] += waiting_time
                time_profile_points[fahrzeitprofil.fahrzeitprofil_nummer][-1].dwell_duration += waiting_time

        del (
            point,
            station,
            grid_point,
            geom,
            driving_time_point,
            segment_id,
            segment,
            waiting_time,
            assoc,
        )  # Clean up the debugger

        # Now, do some sanity checks
        last_distance = None
        if len(assocs) < 2:
            # There are some routes which we have manually checked out and figured to be pointless
            if [p.netzpunkt for p in route.punktfolge.punkt] == [102001974, 101001974, 101029999, 101001974, 102001974]:
                # This is a bus which just stands aroung in Alt-Gatow for a while ?!?!?!?!?
                logger.info(f"Route {route.lfd_nr} of line {db_line.name} is a pointless route. Skipping")
                # Write none to the dict of routes, to show we're aware of this route, but it's pointless
                db_routes_by_lfd_nr[route.lfd_nr] = None
                continue
            elif [p.netzpunkt for p in route.punktfolge.punkt] == [102021010, 101021010, 101002083, 102002083]:
                # This might be a turning-around at Osloer Straße. We don't need it
                logger.info(f"Route {route.lfd_nr} of line {db_line.name} is a pointless route. Skipping")
                # Write none to the dict of routes, to show we're aware of this route, but it's pointless
                db_routes_by_lfd_nr[route.lfd_nr] = None
                continue
            elif [p.netzpunkt for p in route.punktfolge.punkt] == [102004107, 101004107, 101004108, 102004108]:
                # Turning around at Hermannstraße
                logger.info(f"Route {route.lfd_nr} of line {db_line.name} is a pointless route. Skipping")
                # Write none to the dict of routes, to show we're aware of this route, but it's pointless
                db_routes_by_lfd_nr[route.lfd_nr] = None
                continue
            raise ValueError("There should be at least one assoc")
        for assoc in assocs:
            if last_distance is not None:
                if not assoc.elapsed_distance > last_distance:
                    raise ValueError("The elapsed distance should be increasing for each assoc")
            last_distance = assoc.elapsed_distance
        del last_distance

        for fahrzeitprofil in route.fahrzeitprofile.fahrzeitprofil:
            last_time = None
            this_vehicles_time_profile_points = time_profile_points[fahrzeitprofil.fahrzeitprofil_nummer]
            if len(this_vehicles_time_profile_points) < 2:
                raise ValueError("There should be at least one time profile point")
            for time_profile_point in this_vehicles_time_profile_points:
                if last_time is not None:
                    if not time_profile_point.arrival_offset_from_start > last_time:
                        logger.info(
                            f"Route {route.lfd_nr} of line {db_line.name} has a time profile with a non-increasing arrival offset. Fixing them by adding one second to each identical arrival offset"
                        )
                        this_vehicles_time_profile_points = fix_times(this_vehicles_time_profile_points)
                last_time = time_profile_point.arrival_offset_from_start + time_profile_point.dwell_duration
        del this_vehicles_time_profile_points, time_profile_point, last_time

        # Save the time profile points - we do this even if the route is pointless, as there might be trips on it
        trip_time_profiles[route.lfd_nr] = time_profile_points

        # We can now create the route object and see if it already exists
        db_route = create_route(
            scenario_id=scenario_id,
            departure_station=assocs[0].station,
            arrival_station=assocs[-1].station,
            line=db_line,
            distance=elapsed_distance,
            first_grid_point=grid_points[route.punktfolge.punkt[0].netzpunkt],
            last_grid_point=grid_points[route.punktfolge.punkt[-1].netzpunkt],
        )

        # Now we can check if there already is a route with the same assocs
        route_already_exists = False
        route_q = (
            session.query(eflips.model.Route)
            .filter(eflips.model.Route.scenario_id == scenario_id)
            .filter(eflips.model.Route.name == db_route.name)
            .filter(eflips.model.Route.name_short == db_route.name_short)
        )
        if route_q.count() > 0:
            for existing_route in route_q:
                if len(existing_route.assoc_route_stations) == len(assocs):
                    equal = True
                    for i in range(len(existing_route.assoc_route_stations)):
                        if (
                            existing_route.assoc_route_stations[i].station != assocs[i].station
                            or existing_route.assoc_route_stations[i].location != assocs[i].location
                            or existing_route.assoc_route_stations[i].elapsed_distance != assocs[i].elapsed_distance
                        ):
                            equal = False
                            break  # We can stop comparing the assocs
                    if equal:
                        # If the route already exist, we will return it as the one to create the trips for
                        db_routes_by_lfd_nr[route.lfd_nr] = existing_route
                        route_already_exists = True
                        break  # We can stop comparing the other routes
        if not route_already_exists:
            session.add(db_route)
            for assoc in assocs:
                assoc.route = db_route
                session.add(assoc)
            # Also add it to the dict of routes
            db_routes_by_lfd_nr[route.lfd_nr] = db_route

        del (
            assocs,
            elapsed_distance,
            elapsed_time,
            time_profile_points,
            route_already_exists,
        )

    return trip_time_profiles, db_routes_by_lfd_nr


def create_trip_prototypes(
    schedule: Linienfahrplan,
    route_time_profileps: Dict[int, Dict[int, List[TimeProfile.TimeProfilePoint]]],
    db_routes_by_lfd_nr: Dict[int, None | eflips.model.Route],
) -> Dict[int, None | TimeProfile]:
    """
    From a list of time profiles, create a list of trip prototypes, one for each trip in the schedule.

    :param schedule: A parsed Linienfahrplan object
    :param route_time_profileps: A Dict of all the time profiles for each route, ordered by the route.lfd_nr (not to be
           confused with routenvatiante.lfd_nr)
    :return: A Dict with time profiles, ordered by the trip.ID (Fahrt.ID)
    """
    logger = logging.getLogger(__name__)

    grid_points, segments, route_datas, route_lfd_nrs = setup_working_dictionaries(schedule)

    # We need to make sure there is only one "Linie" object, otherwise the route numbers are non-unique
    # If this turns into a list, we need to change the code below
    assert isinstance(schedule.linien_daten.linie, Linienfahrplan.LinienDaten.Linie)

    time_profiles_by_trip_id: Dict[int, None | TimeProfile] = {}
    for fahrt in schedule.fahrt_daten.fahrt:
        route_lfd_nr = route_lfd_nrs[
            fahrt.lfd_nr_routenvariante
        ]  # Convert the routenvariante.lfd_nr to the route.lfd_nr
        db_route = db_routes_by_lfd_nr[route_lfd_nr]
        if db_route is None:
            # That means that thios route was seen by the route creation function, but was deemed pointless
            logger.info(f"Refusing to create trip prototype for trip {fahrt.id}, as the route is pointless")

            # Put an explicit None in the dict, so we know we've seen this trip
            time_profiles_by_trip_id[fahrt.id] = None

            continue
        time_profile = route_time_profileps[route_lfd_nr][fahrt.fahrzeitprofil]

        for i in range(len(time_profile)):
            if time_profile[i].station != db_route.assoc_route_stations[i].station:
                raise ValueError(
                    f"Station {time_profile[i].station.name} at position {i} does not match the station {db_route.assoc_route_stations[i].station.name} in the route"
                )

        profile = TimeProfile(
            route=db_route,
            start_offset_from_midnight=timedelta(seconds=fahrt.startzeit),
            time_profile_points=time_profile,
        )
        time_profiles_by_trip_id[fahrt.id] = profile
    return time_profiles_by_trip_id


def create_trips_and_vehicle_schedules(
    schedule: Linienfahrplan, trip_prototypes: Dict[int, None | TimeProfile], scenario_id: int, session: Session
) -> None:
    """
    Creates the trips and vehicle schedules from the parsed Linienfahrplan object
    :param schedule: A parsed Linienfahrplan object
    :param trip_prototypes: A dictionary containing *all* the available time profiles, by their trip.ID. This (rather
           than the *schedule's* time profiles) is used, because the schedule might contain trips that are not in the
           schedule (if a vehicle from this line goes to another line)
    :param scenario_id: The scenario ID to use
    :param session: An open database session
    :return: Nothing. The trips are added to the database
    """
    logger = logging.getLogger(__name__)

    for fahrzeugumlauf in schedule.fahrzeugumlauf_daten.fahrzeugumlauf:
        # The Fharzeugumlauf has an Umlaeufe object, which implies there could be multiple.
        # We do not support this
        vehicle_type = add_or_ret_vehicle_type(scenario_id, fahrzeugumlauf.fahrzeugtyp, session)
        rotation = eflips.model.Rotation(
            scenario_id=scenario_id,
            id=None,
            name="",
            vehicle_type=vehicle_type,
            allow_opportunity_charging=True,
        )
        session.add(rotation)
        rotation_trips = []
        for xml_rotation in fahrzeugumlauf.umlaeufe.umlauf:
            date_german = xml_rotation.kalenderdatum.split(".")
            the_date = date(day=int(date_german[0]), month=int(date_german[1]), year=int(date_german[2]))
            rotation.name += f"{xml_rotation.umlaufbezeichnung} "
            # Assemble the list of trips
            for umlaufteilgruppe in xml_rotation.umlaufteilgruppen.umlaufteilgruppe:
                if umlaufteilgruppe.fahrtreihenfolge is None:
                    logger.info(
                        f"Umlauf {xml_rotation.umlauf_id} has a Umlaufteilgruppe without a Fahrtreihenfolge. Skipping"
                    )
                    continue
                # Make sure the vehicle type stays the same
                part_trips = []
                for vehicle_type_str in umlaufteilgruppe.fahrzeugtyp:
                    if vehicle_type_str != vehicle_type.name_short:
                        raise ValueError(f"Vehicle type changed while within Umlauf {xml_rotation.umlauf_id}!")
                    for fahrt in umlaufteilgruppe.fahrtreihenfolge.fahrt:
                        if fahrt.fahrt_id not in trip_prototypes.keys():
                            raise ValueError(
                                f"Trip {fahrt.fahrt_id} from Umlauf {xml_rotation.umlauf_id} is not known! "
                                f"Is it from another Linie, which XML file we are missing?"
                            )
                        tp = trip_prototypes[fahrt.fahrt_id]
                        if tp is None:
                            logger.info(
                                f"Trip {fahrt.fahrt_id} from Umlauf {xml_rotation.umlauf_id} is from a pointless route. Skipping"
                            )
                            continue
                        with session.no_autoflush:  # Get rid of spurious SAWarnings
                            part_trips.append(tp.to_trip(rotation, the_date))
                    rotation_trips.extend(part_trips)

            ### SPECIAL FIXES
            # Do some cleanup. There may be geographical inconsistencies caused by a trip's stop being different from the
            # stop of the previous trip. This might be caused by a station having multiple "Haltestellenbereiche"
            # We see if the first four letters of the stations short name are the same, and if so, we change the latter
            # trip's departure station to the former trip's arrival station
            for i in range(len(rotation_trips) - 1):
                cur_trip = rotation_trips[i]
                next_trip = rotation_trips[i + 1]
                if cur_trip.stop_times[-1].station != next_trip.stop_times[0].station:
                    logger.info(
                        f"Trip {cur_trip.id} and {next_trip.id} have different stations: first {cur_trip.stop_times[-1].station.name} (ID f{cur_trip.stop_times[-1].station.id}), then {next_trip.stop_times[0].station.name} (ID f{next_trip.stop_times[0].station.id})."
                    )
            session.add_all(rotation_trips)

        rotation.name = rotation.name.strip()


def recenter_station(station: eflips.model.Station, session: Session) -> None:
    """
    Puts a station's location at the median of it's associations
    :param station:
    :param session:
    :return: Nothing. The station is updated in the database
    """
    # For each association, load the location and add it to a list
    xs, ys, zs = [], [], []
    for assoc in station.assoc_route_stations:
        if isinstance(assoc.location, str):
            loc_str = assoc.location.lstrip("SRID=4326;POINTZ(").rstrip(")")
            x, y, z = loc_str.split(" ")
            x_f, y_f, z_f = float(x), float(y), float(z)
            xs.append(x_f)
            ys.append(y_f)
            zs.append(z_f)
        else:
            assert isinstance(assoc.location, WKBElement)
            shape = to_shape(assoc.location)  # typ
            xs.append(shape.x)
            ys.append(shape.y)
            zs.append(shape.z)

    # Calculate the median of the list
    median_x = statistics.median(xs)
    median_y = statistics.median(ys)
    median_z = statistics.median(zs)

    # Create a new location from the median
    new_location = f"SRID=4326;POINTZ({median_x} {median_y} {median_z})"

    # Update the station
    station.geom = new_location  # type: ignore


def fix_max_sequence(database_url: str) -> None:
    """
    Run some SQL in order to set the nextval() sequence to the maximum value of the id column for each table.
    :param database_url: The database URL to use
    :return: None
    """
    SEQUENCES = [
        "Scenario_id_seq",
        "Plan_id_seq",
        "Process_id_seq",
        "BatteryType_id_seq",
        "VehicleClass_id_seq",
        "Line_id_seq",
        "Station_id_seq",
        "Depot_id_seq",
        "AssocPlanProcess_id_seq",
        "VehicleType_id_seq",
        "Route_id_seq",
        "Area_id_seq",
        "Vehicle_id_seq",
        "AssocVehicleTypeVehicleClass_id_seq",
        "AssocRouteStation_id_seq",
        "AssocAreaProcess_id_seq",
        "Rotation_id_seq",
        "Trip_id_seq",
        "Event_id_seq",
        "StopTime_id_seq",
    ]
    conn = psycopg2.connect(database_url)
    conn.autocommit = True
    for sequence in SEQUENCES:
        table_name = sequence.split("_")[0]
        key_name = sequence.split("_")[1]
        with conn.cursor() as cur:
            cur.execute(f'SELECT MAX("{key_name}") FROM "{table_name}"')
            res = cur.fetchone()
            max_id = res[0] if res is not None else None
            if max_id is not None:
                cur.execute(f'ALTER SEQUENCE "{sequence}" RESTART WITH {max_id + 1}')
                cur.execute(f'SELECT NEXTVAL(\'"public"."{sequence}"\')')
                res = cur.fetchone()
                new_max_id = res[0] if res is not None else None
                if new_max_id <= max_id:
                    raise ValueError(f"Sequence {sequence} did not restart properly. It is still at {new_max_id}")
    conn.close()


def merge_identical_stations(scenario_id: int, session: Session) -> None:
    """
    This method merges stations which have an identical name.
    :param scenario_id: The scenario ID to use
    :param session: An open database session
    :return: Nothing. The stations are updated in the database
    """
    """
    Merge all stations where the first four characters of the short name are the same.
    Also has special handling for three-letter short names, where the first three characters are the same followed by
    an underscore.

    Also very special handling for Berlin Airport, where the short name is "BER1".

    :param scenario:
    :param session:
    :return: Nothing. Databases are updated in place.
    """
    # Load all stations, grouped by the first four characters of the short name
    # If the short name contains an unserscore, we take all the characters before the underscore
    stations_by_short_name: Dict[str, List[Station]] = {}
    for station in session.query(Station).filter(Station.scenario_id == scenario_id).all():
        if station.name_short is None:
            continue

        # We do not merge the depot stations marked with "BF " (the space is important)
        if station.name_short.startswith("BF "):
            continue

        short_name = station.name_short
        if "_" in station.name_short:  # Special case for three-letter short names followed by an underscore
            short_name = station.name_short.split("_")[0]
        short_name = short_name[:4]
        if short_name == "BER1":
            short_name = "BER"  # Special case for Berlin Airport
        if short_name not in stations_by_short_name:
            stations_by_short_name[short_name] = []
        stations_by_short_name[short_name].append(station)

    for short_name, stations in tqdm(stations_by_short_name.items()):
        if len(stations) > 1:
            # Merge the stations
            # The main station will be the one with the shortest name
            main_station = min(stations, key=lambda station: len(station.name))
            for other_station in stations:
                if other_station != main_station:
                    other_station_geom = other_station.geom
                    with session.no_autoflush:
                        # Update all routes, trips, and stoptimes containing the next station to point to the first station instead
                        session.query(Route).filter(Route.departure_station_id == other_station.id).update(
                            {"departure_station_id": main_station.id}
                        )
                        session.query(Route).filter(Route.arrival_station_id == other_station.id).update(
                            {"arrival_station_id": main_station.id}
                        )

                        session.query(AssocRouteStation).filter(
                            AssocRouteStation.station_id == other_station.id
                        ).update({"station_id": main_station.id, "location": other_station_geom})

                        session.query(StopTime).filter(StopTime.station_id == other_station.id).update(
                            {"station_id": main_station.id}
                        )
                    session.flush()
                    session.delete(other_station)


def merge_identical_rotations(scenario_id: int, session: Session) -> None:
    """
    This method merges rotations which have an identical name. It does so by merging everything from
    a rotation beginning at a "Betriebshof"to the next "Betriebshof".
    This needs to be done because different parts of a rotation may be in different files, and so different
    "Rotation" objects for what is essentially the same rotation may be created.
    :param scenario_id: the scenario ID to use
    :param session: an open database session
    :return: Nothing. The rotations are updated in the database
    """

    def is_depot(name: str) -> bool:
        """
        Check if the name of a station contains "Betriebshof" or "Abstellfläche"
        :param name: The name of the station
        :return: True if the name contains "Betriebshof" or "Abstellfläche", False otherwise
        """
        return "Betriebshof" in name or "Abstellfläche" in name

    rotation_names = (
        session.query(eflips.model.Rotation.name)
        .filter(eflips.model.Rotation.scenario_id == scenario_id)
        .group_by(eflips.model.Rotation.name)
        .all()
    )
    for rotation_name in rotation_names:
        all_rots_for_name = (
            session.query(eflips.model.Rotation)
            .filter(eflips.model.Rotation.name == rotation_name[0])
            .filter(eflips.model.Rotation.scenario_id == scenario_id)
            .all()
        )
        # Order them by the first trip's departure time
        all_rots_for_name = sorted(all_rots_for_name, key=lambda rot: rot.trips[0].departure_time)

        list_of_rotation_id_tuples_to_merge: List[List[int]] = []
        rotation_ids_to_merge: List[int] = []

        for rotation in all_rots_for_name:
            first_station_name = rotation.trips[0].route.departure_station.name
            last_station_name = rotation.trips[-1].route.arrival_station.name
            first_station_departure_time = rotation.trips[0].departure_time
            last_station_arrival_time = rotation.trips[-1].arrival_time

            if is_depot(first_station_name) and is_depot(last_station_name):
                # This is a rotation that starts and ends at the depot
                # We don't need to merge it with anything
                continue
            elif is_depot(first_station_name) and not is_depot(last_station_name):
                # This rotation starts at the depot and ends somewhere else
                # Start a new list after appending the current list to the list of lists
                list_of_rotation_id_tuples_to_merge.append(rotation_ids_to_merge)
                rotation_ids_to_merge = [rotation.id]
            elif not is_depot(first_station_name) and is_depot(last_station_name):
                # This rotation starts somewhere else and ends at the depot
                # Append the current rotation to the list
                rotation_ids_to_merge.append(rotation.id)
                list_of_rotation_id_tuples_to_merge.append(rotation_ids_to_merge)
                rotation_ids_to_merge = []
            elif not is_depot(first_station_name) and not is_depot(last_station_name):
                # This rotation starts and ends somewhere else
                # Append the current rotation to the list
                rotation_ids_to_merge.append(rotation.id)
            else:
                raise ValueError("This should never happen")

        # Remove empty lists
        list_of_rotation_id_tuples_to_merge = [x for x in list_of_rotation_id_tuples_to_merge if len(x) > 0]

        # Go through the list of lists and merge the rotations, if they form a valid rotation
        for rotation_ids_to_merge in list_of_rotation_id_tuples_to_merge:
            rotations = (
                session.query(eflips.model.Rotation).filter(eflips.model.Rotation.id.in_(rotation_ids_to_merge)).all()
            )
            # Order the rotations by the first trip's departure time
            rotations = sorted(rotations, key=lambda rot: rot.trips[0].departure_time)

            # Check whether the rotations can be merged
            # The first station of the first rotation must be the same as the last station of the last rotation
            # And both must contain "Betriebshof"
            if (
                rotations[0].trips[0].route.departure_station.name != rotations[-1].trips[-1].route.arrival_station.name
                or not (is_depot(rotations[0].trips[0].route.departure_station.name))
                or not (is_depot(rotations[-1].trips[-1].route.arrival_station.name))
            ):
                # If the merge is not possible we delete these rotations
                for rotation in rotations:
                    for trip in rotation.trips:
                        for stop_time in trip.stop_times:
                            session.delete(stop_time)
                        session.delete(trip)
                    session.delete(rotation)
            else:
                # Merge the rotations by creating a new rotation
                new_rotation = eflips.model.Rotation(
                    name=rotations[0].name,
                    scenario_id=rotations[0].scenario_id,
                    vehicle_type_id=rotations[0].vehicle_type_id,
                    allow_opportunity_charging=rotations[0].allow_opportunity_charging,
                )
                session.add(new_rotation)
                session.flush()
                for rotation in rotations:
                    session.query(eflips.model.Trip).filter(eflips.model.Trip.rotation_id == rotation.id).update(
                        {"rotation_id": new_rotation.id}
                    )
                    session.refresh(
                        rotation
                    )  # Refresh the rotation object to get the updated trips (there are now none)
                    session.delete(rotation)
                    session.flush()


def identify_and_delete_overlapping_rotations(scenario_id: int, session: Session) -> None:
    """
    Identify and delete rotations with overlapping trips. These may be caused by the rotation merging function.
    :param scenario_id: The scenario ID to use
    :param session: An open database session
    :return: Nothing. The rotations are updated in the database
    """
    logger = logging.getLogger(__name__)
    rotations = session.query(eflips.model.Rotation).filter(eflips.model.Rotation.scenario_id == scenario_id).all()
    for rotation in rotations:
        trips = rotation.trips
        for i in range(len(trips) - 1):
            if trips[i].arrival_time > trips[i + 1].departure_time:
                logger.warning(
                    f"Rotation {rotation.id} has overlapping trips {trips[i].id} and {trips[i + 1].id}. Deleting the rotation"
                )
                for trip in trips:
                    for stop_time in trip.stop_times:
                        session.delete(stop_time)
                    session.delete(trip)
                session.delete(rotation)
                break


def ingest_bvgxml(
    paths: Union[str, List[str]],
    database_url: str,
    clear_database: bool = False,
    multithreading: bool = True,
    log_level: str = "WARNING",
) -> None:
    """
    The main method for ingesting BVG-XML format files into the database.

    :param paths: Either a directory or a list of files to ingest
    :param database_url: The database URL to use for the ingestion
    :param clear_database: Whether to clear the database and create a new schema before ingesting
    :param multithreading: Whether to use multithreading or not. Useful to disable for debugging
    :return:
    """

    match log_level:
        case "DEBUG":
            logging.basicConfig(level=logging.DEBUG)
        case "INFO":
            logging.basicConfig(level=logging.INFO)
        case "WARNING":
            logging.basicConfig(level=logging.WARNING)
        case "ERROR":
            logging.basicConfig(level=logging.ERROR)
        case "CRITICAL":
            logging.basicConfig(level=logging.CRITICAL)
        case _:
            raise ValueError("Invalid log level. Must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL")

    logger = logging.getLogger(__name__)

    if isinstance(paths, str):
        if os.path.isdir(paths):
            # Find all xml files in the directory
            paths = glob.glob(os.path.join(paths, "*.xml"))
        else:
            paths = [paths]
    paths_pathlike = [Path(p) for p in paths]

    TOTAL_STEPS = 11

    ### STEP 1: Load the XML files into memory
    # First, we go through all the files and load them into memory
    schedules = []
    if multithreading:
        with Pool() as pool:
            for schedule in tqdm(
                pool.imap_unordered(load_and_validate_xml, paths_pathlike),
                total=len(paths_pathlike),
                desc=f"(1/{TOTAL_STEPS}) Loading XML files",
            ):
                schedules.append(schedule)
    else:
        schedules = []
        for path in tqdm(paths_pathlike, desc=f"(1/{TOTAL_STEPS}) Loading XML files"):
            schedules.append(load_and_validate_xml(path))

    ### STEP 1.5: Create the database session and scenario
    engine = create_engine(database_url)
    if clear_database:
        eflips.model.Base.metadata.drop_all(engine)
        eflips.model.setup_database(engine)
    session = Session(engine)
    scenario = eflips.model.Scenario(
        name=f"Created by BVG-XML Ingestion on {socket.gethostname()} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
    )
    session.add(scenario)
    session.flush()
    scenario_id = scenario.id

    ### STEP 2: Create the stations
    # Now, we go through the schedules and create the stations
    # No multithreading, because that would just create duplicate stations
    for schedule in tqdm(schedules, desc=f"(2/{TOTAL_STEPS}) Creating stations"):
        create_stations(schedule, scenario_id, session)

    ### STEP 3: Create the routes and save some data for later
    # Again no multithreading
    create_route_results: List[
        Tuple[
            Linienfahrplan,
            Dict[int, Dict[int, List[TimeProfile.TimeProfilePoint]]],
            Dict[int, None | eflips.model.Route],
        ]
    ] = []
    for schedule in tqdm(schedules, desc=f"(3/{TOTAL_STEPS}) Creating routes"):
        trip_time_profiles, db_routes_by_lfd_nr = create_routes_and_time_profiles(schedule, scenario_id, session)
        create_route_results.append((schedule, trip_time_profiles, db_routes_by_lfd_nr))

    ### STEP 4: Create the trip prototypes
    # This can be done in parallel, but we don't need to do it, it's fast enough
    all_trip_protoypes: List[Dict[int, None | TimeProfile]] = []
    for create_route_result in tqdm(create_route_results, desc=f"(4/{TOTAL_STEPS}) Creating trip prototypes"):
        trip_prototypes = create_trip_prototypes(create_route_result[0], create_route_result[1], create_route_result[2])
        all_trip_protoypes.append(trip_prototypes)

    # Unify the dictionaries, making sure the contents are the same if there is a duplicate key
    trip_prototypes = {}
    for the_dict in all_trip_protoypes:
        for fahrt_id, time_profile in the_dict.items():
            if fahrt_id in trip_prototypes:
                if trip_prototypes[fahrt_id] != time_profile:
                    raise ValueError(f"Trip {fahrt_id} has two different time profiles in different schedules")
            else:
                trip_prototypes[fahrt_id] = time_profile

    ### STEP 5: Create the trips and vehicle schedules
    for schedule in tqdm(schedules, desc=f"(5/{TOTAL_STEPS}) Creating trips and vehicle schedules"):
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", category=ConsistencyWarning)
            create_trips_and_vehicle_schedules(schedule, trip_prototypes, scenario_id, session)

    ### STEP 6: Set the geom of the stations
    # No multithreading, because it should be fast enough
    stations_without_geom_q = (
        session.query(eflips.model.Station)
        .join(eflips.model.AssocRouteStation)
        .filter(eflips.model.Station.scenario_id == scenario_id)
        .distinct(eflips.model.Station.id)
    )
    for station in tqdm(
        stations_without_geom_q,
        desc=f"(6/{TOTAL_STEPS}) Setting station geom",
        total=stations_without_geom_q.count(),
    ):
        # Get the median of the assoc_route_stations
        recenter_station(station, session)

    # Flush the session to convert the geoms from string to binary
    session.flush()
    session.expire_all()

    ### STEP 7: Fix the routes with very large distances:
    # There are some routes which have a distance of zero even once the last point is reached
    # We set their distance to a very large number. Now we set it to the geometric distance between the first and last
    # point
    long_route_q = (
        session.query(eflips.model.Route)
        .filter(eflips.model.Route.scenario_id == scenario_id)
        .filter(eflips.model.Route.distance >= 1e6 * 1000)
    )
    for route in tqdm(long_route_q, desc=f"(7/{TOTAL_STEPS}) Fixing long routes", total=long_route_q.count()):
        first_point = route.departure_station.geom
        last_point = route.arrival_station.geom

        first_point_soldner = func.ST_Transform(first_point, 3068)
        last_point_soldner = func.ST_Transform(last_point, 3068)
        dist_q = ST_Distance(first_point_soldner, last_point_soldner)

        dist = session.query(dist_q).one()[0]

        with session.no_autoflush:
            route.distance = dist
            route.assoc_route_stations[-1].elapsed_distance = dist
        route.name = "CHECK DISTANCE: " + route.name

    session.flush()
    session.expire_all()

    # STEP 8: Merge identical stations
    print(f"(8/{TOTAL_STEPS}) Merging identical stations")
    merge_identical_stations(scenario_id, session)

    session.flush()
    session.expire_all()

    # STEP 9: Combine rotations with the same name
    print(f"(9/{TOTAL_STEPS}) Merging identical rotations")
    merge_identical_rotations(scenario_id, session)

    # STEP 10: Identify overlapping rotations
    print(f"(10/{TOTAL_STEPS}) Identifying and deleting overlapping rotations")
    identify_and_delete_overlapping_rotations(scenario_id, session)

    # Commit and close this session
    session.commit()
    session.close()

    # STEP 11: Fix the max sequence numbers
    print(f"(11/{TOTAL_STEPS}) Fixing max sequence numbers")
    fix_max_sequence(database_url)

    print(
        """
    The import is complete. You may still want to:
    - Remove some rotations that are not relevant
    - Merge the vehicle types into three major types
    - Figure out what happens with the rotations at the very end of the schedule. There seem to be some borked
      ones there.
    """
    )


if __name__ == "__main__":
    fire.Fire(ingest_bvgxml)
