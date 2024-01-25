#!/usr/bin/env python3
import glob
import logging
import os
import pickle
import socket
import zoneinfo
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from itertools import chain
from multiprocessing import Pool
from multiprocessing.pool import ThreadPool
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union
from tqdm.contrib.concurrent import process_map

import fire  # type: ignore
import eflips.model
from tqdm.auto import tqdm

import eflips.ingest.util
from lxml import etree
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from xsdata.formats.dataclass.parsers import XmlParser

from eflips.ingest.util import soldner_to_pointz
from eflips.ingest.xmldata import (
    FahrtFahrgastrelevant,
    Linienfahrplan,
    NetzpunktNetzpunkttyp,
)


def load_and_validate_xml(filename: Path) -> None | Linienfahrplan:
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
        logger.info(f"File {filename} is not a valid line. Skipping")
        return None
    elif "Keine Umläufe vorhanden." in xml_string:
        logger.info(f"File {filename} does not contain any rotations. Skipping")
        return None

    # We want to remove the occurrences of 'ns2: and ':ns2' in the first and last line. Only then does our schema
    # and the 'xmldata' package work. For some reason, with the ns2 it generates two differnet python files, and
    # then it doesn't work.
    xml_string = xml_string.replace("ns2:", "")
    xml_string = xml_string.replace(":ns2", "")

    xsd_path = Path(__file__).parent.parent.parent / "data" / "bvg_xml.xsd"
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
            geom="POINT(0 0 0)",  # Will be set later
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
        .filter(eflips.model.Scenario.id == scenario_id)
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
            logger.warning(
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
        .filter(eflips.model.VehicleType.name == fahrzeugtyp)
        .one_or_none()
    )
    if vehicle_type is None:
        vehicle_type = eflips.model.VehicleType(
            scenario_id=scenario_id,
            name=fahrzeugtyp,
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

    route_lfd_nr: int
    line: str
    time_profile_no: int
    time_profile_points: List[TimeProfilePoint]


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


def create_routes_and_time_profiles(
    schedule: Linienfahrplan, scenario_id: int, session: Session
) -> Dict[int, TimeProfile]:
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
    :return: A dictionary of time profiles, by their 'Linienfahrplan/Liniendaten/Fahrt.ID'
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
    ] = {}  # Order: route, time profile number, time profile
    for route in schedule.linien_daten.linie.routen_daten.route:
        # Contrary to the naive approach, we first create the AssocRouteStation objects, and then the route object
        # This way, we are sure the departure and arrival stations match as well as the total distance
        assocs: List[eflips.model.AssocRouteStation] = []
        elapsed_distance = 0

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
                logger.warning(f"Route {route.lfd_nr} of line {db_line.name} is a pointless route. Skipping")
                continue
            elif [p.netzpunkt for p in route.punktfolge.punkt] == [102021010, 101021010, 101002083, 102002083]:
                # This might be a turning-around at Osloer Straße. We don't need it
                logger.warning(f"Route {route.lfd_nr} of line {db_line.name} is a pointless route. Skipping")
                continue
            elif [p.netzpunkt for p in route.punktfolge.punkt] == [102004107, 101004107, 101004108, 102004108]:
                # Turning around at Hermannstraße
                logger.warning(f"Route {route.lfd_nr} of line {db_line.name} is a pointless route. Skipping")
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
                    if not time_profile_point.arrival_offset_from_start >= last_time:
                        raise ValueError("The elapsed time should be increasing for each time profile point")
                last_time = time_profile_point.arrival_offset_from_start + time_profile_point.dwell_duration
        del this_vehicles_time_profile_points, time_profile_point, last_time

        # Save the time profile points
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
                existing_route: eflips.model.Route
                if len(existing_route.assoc_route_stations) == len(assocs):
                    equal = True
                    for i in range(len(existing_route.assoc_route_stations)):
                        if (
                            existing_route.assoc_route_stations[i].location != assocs[i].location
                            and existing_route.assoc_route_stations[i].elapsed_distance != assocs[i].elapsed_distance
                        ):
                            equal = False
                            break  # We can stop comparing the assocs
                    if equal:
                        route_already_exists = True
                        break  # We can stop comparing the other routes
        if not route_already_exists:
            session.add(db_route)
            for assoc in assocs:
                assoc.route = db_route
                session.add(assoc)

        del (
            assocs,
            elapsed_distance,
            elapsed_time,
            time_profile_points,
            route_already_exists,
        )


def import_linienfahrplan(schedule: Linienfahrplan, scenario_id: int, database_url: str):
    logger = logging.getLogger(__name__)
    engine = create_engine(database_url)
    with Session(engine) as session:
        try:
            # Create the line object
            db_line = add_or_ret_line(scenario_id, schedule.linien_daten.linie.kurzname, session)

            (
                grid_points,
                segments,
                route_datas,
                route_lfd_nrs,
            ) = setup_working_dictionaries(schedule)

            # We need to make sure there is only one "Linie" object, otherwise the route numbers are non-unique
            # If this turns into a list, we need to change the code below
            assert isinstance(schedule.linien_daten.linie, Linienfahrplan.LinienDaten.Linie)

            # Gp through the Routes in the "RoutenDaten" of the "Linie"
            db_routes: Dict[int, eflips.model.Route] = {}
            for route in schedule.linien_daten.linie.routen_daten.route:
                route: Linienfahrplan.LinienDaten.Linie.RoutenDaten.Route

                # The departure station is the staion corresponding to the first "Punkt" in the route
                departure_station_grid_id = route.punktfolge.punkt[0].netzpunkt
                departure_station = add_or_ret_station_for_grid_point(
                    scenario_id, departure_station_grid_id, grid_points, session
                )

                # The arrival station is the staion corresponding to the last "Punkt" in the route
                arrival_station_grid_id = route.punktfolge.punkt[-1].netzpunkt
                arrival_station = add_or_ret_station_for_grid_point(
                    scenario_id, arrival_station_grid_id, grid_points, session
                )

                name = db_line.name + " "
                if grid_points[departure_station_grid_id].netzpunkttyp == NetzpunktNetzpunkttyp.EPKT:
                    name += "Einsetzfahrt "
                elif grid_points[arrival_station_grid_id].netzpunkttyp == NetzpunktNetzpunkttyp.APKT:
                    name += "Aussetzfahrt "
                elif (
                    grid_points[arrival_station_grid_id].netzpunkttyp == NetzpunktNetzpunkttyp.BPUNKT
                    or grid_points[departure_station_grid_id].netzpunkttyp == NetzpunktNetzpunkttyp.BPUNKT
                ):
                    pass
                else:
                    assert (
                        grid_points[arrival_station_grid_id].netzpunkttyp == NetzpunktNetzpunkttyp.HST
                        and grid_points[departure_station_grid_id].netzpunkttyp == NetzpunktNetzpunkttyp.HST
                    ), "Check what the other grid point types might mean"
                name += departure_station.name + " → " + arrival_station.name
                name_short = db_line.name + " " + departure_station.name_short + " → " + arrival_station.name_short

                db_route = eflips.model.Route(
                    scenario_id=scenario_id,
                    departure_station=departure_station,
                    arrival_station=arrival_station,
                    line=db_line,
                    name=name,
                    name_short=name_short,
                    headsign=None,
                    distance=1,  # Will be set later
                    geom=None,
                )
                session.add(db_route)

                # Now, add AssocRouteStation objects for all the segments and grid points
                # From this we get both the assoc objects and also fill out the geometry of the route
                assert (
                    len(route.punktfolge.punkt) - len(route.streckenfolge.strecke) == 1
                ), "There should be one more point than segments"
                elapsed_distance = 0
                session.flush()  # This one if kept here, to make sure the created stations are available in the next step
                with session.no_autoflush:
                    assocs = []
                    for i in range(len(route.punktfolge.punkt)):
                        point = route.punktfolge.punkt[i]

                        # Update the elapsed distance with the corresponding segment
                        if i > 0:
                            segment_id = route.streckenfolge.strecke[i - 1].strecken_id
                            segment = segments[segment_id]
                            elapsed_distance += segment.streckenlaenge

                        # We only create an AssocRouteStation object if the point is 'fahrgastwechsel' as well as 'veroeffentlicht', and if there is distance from the previous point
                        # except if it is the first or last point
                        if (
                            point.fahrgastwechsel and point.veroeffentlicht and (i > 0 and segment.streckenlaenge > 0)
                        ) or (i == 0 or i == len(route.punktfolge.punkt) - 1):
                            grid_point = grid_points[point.netzpunkt]
                            geom = soldner_to_pointz(grid_point.xkoordinate, grid_point.ykoordinate)
                            station = add_or_ret_station_for_grid_point(
                                scenario_id, point.netzpunkt, grid_points, session
                            )
                            if len(assocs) > 0 and station == assocs[-1].station:
                                # If the station is the same as the previous one, we don't need to add it again
                                # Instead, we just update the elapsed distance (if it's not the first point)
                                if len(assocs) > 1:
                                    assocs[-1].elapsed_distance = elapsed_distance
                                continue

                            # Special case handling: If we are at the last stop and the streckenlaenge is 0, we add 1m to the distance
                            if i == len(route.punktfolge.punkt) - 1 and segment.streckenlaenge == 0:
                                elapsed_distance += 1

                            assoc = eflips.model.AssocRouteStation(
                                scenario_id=scenario_id,
                                route=db_route,
                                station=station,
                                location=geom,
                                elapsed_distance=elapsed_distance,
                            )
                            assocs.append(assoc)
                    session.add_all(assocs)
                    db_route.distance = elapsed_distance

                # Now we check if there already is a route with the same assocs
                session.flush()  # Necessary to make sure the route ID is assigned
                route_q = (
                    session.query(eflips.model.Route)
                    .filter(eflips.model.Route.id != db_route.id)
                    .filter(eflips.model.Route.scenario_id == scenario_id)
                    .filter(eflips.model.Route.name == db_route.name)
                    .filter(eflips.model.Route.name_short == db_route.name_short)
                )
                if route_q.count() > 0:
                    for existing_route in route_q:
                        existing_route: eflips.model.Route
                        if len(existing_route.assoc_route_stations) == len(assocs):
                            equal = True
                            for i in range(len(existing_route.assoc_route_stations)):
                                if (
                                    existing_route.assoc_route_stations[i].location != assocs[i].location
                                    and existing_route.assoc_route_stations[i].elapsed_distance
                                    != assocs[i].elapsed_distance
                                ):
                                    equal = False
                                    break
                            if equal:
                                # We found a route with the same assocs, so we can delete the one we just created
                                session.flush()  # Necessary to make sure the route ID is assigned
                                session.delete(db_route)
                                [session.delete(a) for a in assocs]
                                db_route = existing_route
                                break

                db_routes[route.lfd_nr] = db_route

            # Because the rotation ID needs to be set for each trip, we need to go through the rotations first
            # and then through the trips
            rotation_for_fahrt_id: Dict[int, Tuple[eflips.model.Rotation, date]] = {}
            for fahrzeugumlauf in schedule.fahrzeugumlauf_daten.fahrzeugumlauf:
                for umlauf in fahrzeugumlauf.umlaeufe.umlauf:
                    rotation_id = umlauf.umlauf_id
                    date_str = umlauf.kalenderdatum.split(".")
                    the_date = date(
                        day=int(date_str[0]),
                        month=int(date_str[1]),
                        year=int(date_str[2]),
                    )
                    rotation_name = umlauf.umlaufbezeichnung

                    # The vheicle types of all parts of the rotation should be the same otherwise we have a problem
                    vehicle_type_names = list(
                        chain(*[u.fahrzeugtyp for u in umlauf.umlaufteilgruppen.umlaufteilgruppe])
                    )
                    assert (
                        len(set(vehicle_type_names)) == 1
                    ), "The vehicle type of all parts of the rotation should be the same"

                    vehicle_type = add_or_ret_vehicle_type(scenario_id, vehicle_type_names[0], session)

                    # Now, we can create the rotation object
                    possible_db_rotation = (
                        session.query(eflips.model.Rotation)
                        .filter(eflips.model.Rotation.scenario_id == scenario_id)
                        .filter(eflips.model.Rotation.id == rotation_id)
                        .one_or_none()
                    )
                    if possible_db_rotation is None:
                        db_rotation = eflips.model.Rotation(
                            scenario_id=scenario_id,
                            id=rotation_id,
                            name=rotation_name,
                            vehicle_type=vehicle_type,
                            allow_opportunity_charging=True,
                        )
                        session.add(db_rotation)
                    else:
                        db_rotation = possible_db_rotation

                    has_trip = False
                    for umlaufteilgruppe in umlauf.umlaufteilgruppen.umlaufteilgruppe:
                        if umlaufteilgruppe.fahrtreihenfolge is not None:
                            for fahrt in umlaufteilgruppe.fahrtreihenfolge.fahrt:
                                rotation_for_fahrt_id[fahrt.fahrt_id] = (
                                    db_rotation,
                                    the_date,
                                )
                                has_trip = True

            # Now, we can go through the trips
            with session.no_autoflush:
                for fahrt in schedule.fahrt_daten.fahrt:
                    fahrt_id = fahrt.id
                    if fahrt_id not in rotation_for_fahrt_id:
                        # If this happens, the trip does not actually happen
                        logger.info(f"Trip {fahrt_id} does not actually happen. Skipping")
                        continue
                    db_rotation, the_date = rotation_for_fahrt_id[fahrt_id]
                    route_lfd_nr = route_lfd_nrs[fahrt.lfd_nr_routenvariante]
                    db_route = db_routes[route_lfd_nr]
                    route_data = route_datas[route_lfd_nr]

                    match fahrt.fahrgastrelevant:
                        case FahrtFahrgastrelevant.J:
                            trip_type = eflips.model.TripType.PASSENGER
                        case FahrtFahrgastrelevant.N:
                            trip_type = eflips.model.TripType.EMPTY

                    tz = zoneinfo.ZoneInfo("Europe/Berlin")
                    local_midnight = datetime.combine(the_date, time(hour=0, minute=0, second=0), tzinfo=tz)
                    departure_time = local_midnight + timedelta(seconds=fahrt.startzeit)

                    # Load the timing data (the "Fahrtzeitprofil")
                    timing_profile_id = fahrt.fahrzeitprofil
                    timing_profile = [
                        p
                        for p in route_data.fahrzeitprofile.fahrzeitprofil
                        if p.fahrzeitprofil_nummer == timing_profile_id
                    ]
                    assert len(timing_profile) == 1, "There should be exactly one timing profile"
                    timing_profile = timing_profile[0].fahrzeitprofilpunkte.punkt

                    assert len(timing_profile) == len(
                        route_data.punktfolge.punkt
                    ), "There should be exactly one timing profile point for each route point"

                    # Go through the timing profile, adding up the total time and adding stop times as we go
                    stop_times = []
                    for i in range(len(timing_profile)):
                        route_point = route_data.punktfolge.punkt[i]
                        timing_point = timing_profile[i]

                        # Load the station corresponding to this point
                        grid_point = route_point.netzpunkt
                        station = add_or_ret_station_for_grid_point(scenario_id, grid_point, grid_points, session)

                        if i == 0:
                            # Create an entry for the departure station
                            stop_time = eflips.model.StopTime(
                                scenario_id=scenario_id,
                                station=station,
                                trip=None,  # Will be set later
                                arrival_time=departure_time,
                                dwell_duration=timedelta(seconds=0),
                            )
                            stop_times.append(stop_time)
                        elif timing_point.streckenfahrzeit > 0:
                            # We may have moved to a new station. If so, create an entry for the arrival station
                            if station != stop_times[-1].station:
                                # We have moved to a new station
                                current_time = (
                                    stop_times[-1].arrival_time
                                    + stop_times[-1].dwell_duration
                                    + timedelta(seconds=timing_point.streckenfahrzeit)
                                )
                                stop_time = eflips.model.StopTime(
                                    scenario_id=scenario_id,
                                    station=station,
                                    trip=None,  # Will be set later
                                    arrival_time=current_time,
                                    dwell_duration=timedelta(seconds=0),
                                )
                                stop_times.append(stop_time)
                            else:
                                # We are still at the same station, so we just add the time to the last stop time
                                stop_times[-1].dwell_duration += timedelta(seconds=timing_point.streckenfahrzeit)
                        elif i == len(timing_profile) - 1 and station != stop_times[-1].station:
                            # Create an entry for the arrival station, even if we would not have one otherwise
                            if timing_point.streckenfahrzeit == 0 and timing_point.wartezeit == 0:
                                if i == 1:
                                    # Now we are in the pathological case that we have only two points, and they have no time between them
                                    # We check if the first point's long name has "Einsetzen" or the last point's long name has "Aussetzen"
                                    # in it.
                                    # For "Einsetzen, we shift the first stop time 10 minutes backward in time and touch the departure time
                                    # For "Aussetzen", we shift the last stop time 10 minutes forward in time

                                    logger.warning(
                                        f"Teleporting buses: Trip {fahrt_id} has only two points, and they have no time between them. Trying to fix it"
                                    )

                                    assert len(route_data.punktfolge.punkt) == 2, "There should be exactly two points"
                                    first_point_grid_point = grid_points[route_data.punktfolge.punkt[0].netzpunkt]
                                    last_point_grid_point = grid_points[route_data.punktfolge.punkt[-1].netzpunkt]

                                    if "Einsetzen" in first_point_grid_point.langname:
                                        # We are in the Einsetzen case
                                        stop_times[0].arrival_time -= timedelta(minutes=10)
                                        departure_time -= timedelta(minutes=10)
                                    elif "Aussetzen" in last_point_grid_point.langname:
                                        # We are in the Aussetzen case
                                        pass
                                    else:
                                        raise ValueError(
                                            f"Trip {fahrt_id} has only two points, and they have no time between them, but they are not Einsetzen or Aussetzen"
                                        )
                                    additional_time = timedelta(minutes=10)

                                else:
                                    # Remove a little bit from the previous stop time, so that the arrival time is correct
                                    stop_times[-1].arrival_time -= timedelta(seconds=1)
                                    additional_time = timedelta(seconds=1)
                            else:
                                additional_time = timedelta(seconds=0)

                            stop_time = eflips.model.StopTime(
                                scenario_id=scenario_id,
                                station=station,
                                trip_id=fahrt.id,
                                arrival_time=stop_times[-1].arrival_time
                                + stop_times[-1].dwell_duration
                                + timedelta(seconds=timing_point.streckenfahrzeit)
                                + additional_time,
                                dwell_duration=timedelta(seconds=0),
                            )

                            stop_times.append(stop_time)

                        if timing_point.wartezeit > 0:
                            # We have a wait time at this station
                            stop_times[-1].dwell_duration += timedelta(seconds=timing_point.wartezeit)

                    arrival_time = stop_times[-1].arrival_time + stop_times[-1].dwell_duration

                    db_trip = eflips.model.Trip(
                        id=fahrt_id,
                        scenario_id=scenario_id,
                        route=db_route,
                        rotation=db_rotation,
                        departure_time=departure_time,
                        arrival_time=arrival_time,
                        trip_type=trip_type,
                    )
                    session.add(db_trip)
            session.flush()

            # TODO: Cleanup duplicate trips shifted by one week

            # TODO: Cleanup all the rotations without any trips whatsoever

            # TODO: Set correct geom for stations

        except Exception as e:
            session.rollback()
            raise e
        finally:
            session.commit()
            session.close()


def import_line(scenario_id: int, schedules: Dict[date, Linienfahrplan], database_url: str) -> None:
    """
    Imports a single line into the database

    :param input: the input dictionary from the main method
    :param database_url: the database URL to use
    :return:
    """

    # We go through the dates in descending order
    dates = sorted(list(schedules.keys()), reverse=True)
    for i in range(len(dates)):
        the_date = dates[i]
        schedule = schedules[the_date]

        for the_date, schedule in schedules.items():
            import_linienfahrplan(schedule, scenario_id, database_url)


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

    TOTAL_STEPS = 3

    ### STEP 1: Load the XML files into memory
    # First, we go through all the files and load them into memory
    # Debugging step: List all failing schedules
    if True:  # TODO: Keep only this chain, not the else
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

        # Remove all the None values
        len_before = len(schedules)
        schedules = [s for s in schedules if s is not None]
        len_after = len(schedules)
        logger.info(f"Removed {len_before - len_after} invalid files")

        ### STEP 1.5: Create the database session and scenario
        engine = create_engine(database_url)
        if clear_database:
            eflips.model.Base.metadata.drop_all(engine)
            eflips.model.Base.metadata.create_all(engine)
        session = Session(engine)
        scenario = eflips.model.Scenario(
            name=f"Created by BVG-XML Ingestion on {socket.gethostname()} at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
        )
        session.add(scenario)
        session.flush()
        scenario_id = scenario.id

        # TODO: Wrap it into a try/except and rollback if it fails

        ### STEP 2: Create the stations
        # Now, we go through the schedules and create the stations
        # No multithreading, because that would just create duplicate stations
        for schedule in tqdm(schedules, desc=f"(2/{TOTAL_STEPS}) Creating stations"):
            create_stations(schedule, scenario_id, session)

        # DEBUG STEP TODO: Remove this
        session.commit()
        failing_schedules = []
        for schedule in tqdm(schedules, desc=f"(3/{TOTAL_STEPS}) Creating routes"):
            try:
                create_routes_and_time_profiles(schedule, scenario_id, session)
                session.commit()
            except Exception as e:
                logger.warning(f"Failed to create routes for schedule")
                session.rollback()
                failing_schedules.append(schedule)
            finally:
                session.commit()
        with open("failing_schedules.pickle", "wb") as f:
            pickle.dump(failing_schedules, f, protocol=pickle.HIGHEST_PROTOCOL)
        exit()
    else:
        engine = create_engine(database_url)
        session = Session(engine)
        scenario = session.query(eflips.model.Scenario).one()
        session.commit()
        scenario_id = scenario.id
        failing_schedules = pickle.load(open("failing_schedules.pickle", "rb"))
        for schedule in tqdm(failing_schedules):
            try:
                create_routes_and_time_profiles(schedule, scenario_id, session)
                session.flush()
            except Exception as e:
                breakpoint()
                session.rollback()
            finally:
                session.commit()
        exit(0)

    ### STEP 3: Create the routes and save some data for later
    # Again no multithreading
    time_profile_dictss: List[Dict[int, TimeProfile]] = []
    for schedule in tqdm(schedules, desc=f"(3/{TOTAL_STEPS}) Creating routes"):
        time_profile_dictss.append(create_routes_and_time_profiles(schedule, scenario_id, session))
        session.flush()  # TODO: Remove this

    # Take the stations that are unique by ID
    session.flush()
    session.commit()


if __name__ == "__main__":
    fire.Fire(ingest_bvgxml)
