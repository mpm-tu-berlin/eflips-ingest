#!/usr/bin/env python3
import glob
import logging
import os
import zoneinfo
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from itertools import chain
from pathlib import Path
from typing import Dict, List, Optional, Set, Tuple, Union

import fire  # type: ignore
import eflips.model
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

@dataclass
class PreProcessResult:
    """
    The result of the pre-processing step
    """

    line_name: str
    dates: Set[date]


def load_and_validate_xml(filename: Path) -> Optional[Linienfahrplan]:
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


def preprocess_bvgxml(the_schedule: Linienfahrplan) -> Optional[PreProcessResult]:
    """
    Handles pre-ingest analysis of BVG-XML files. This includes
    - finding the line name
    - finding the date

    :param the_schedule: the Linienfahrplan object to analyze
    :return: a PreProcessResult object
    """
    assert the_schedule.linien_daten is not None
    linie = the_schedule.linien_daten.linie

    if linie is not None:
        name = linie.kurzname
        dates = []
        assert the_schedule.fahrzeugumlauf_daten is not None
        for fahrzeugumlauf in the_schedule.fahrzeugumlauf_daten.fahrzeugumlauf:
            the_date = fahrzeugumlauf.umlaeufe.umlauf[0].kalenderdatum
            day, month, year = the_date.split(".")
            dates.append(date(day=int(day), month=int(month), year=int(year)))
    else:
        raise ValueError(f"No line found in BVG-XML file")

    return PreProcessResult(name, set(dates))


def add_or_ret_station(
    scenario_id: int, id: int, name: str, name_short: str, session: Session
) -> eflips.model.Station:
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
            geom="POINT(0 0 0)",
        )
        session.add(station)
    return station


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
            raise ValueError(
                f"Station for grid point {gridpoint_id} not found, even though it is of type 'Hst'"
            )
    elif grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.BPUNKT:
        # Assumption: A "Betriebspunkt" basically belongs to the station for our purposes
        # We query the station by the four-character short name
        short_name = grid_point.kurzname[0:4]
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
            # TODO: Id this the wqay we want to handle this?
            # Here, we can actually calculate the coordinates already
            geom = soldner_to_pointz(grid_point.xkoordinate, grid_point.ykoordinate)

            station = eflips.model.Station(
                scenario_id=scenario_id,
                name=short_name,
                name_short=short_name,
                is_electrified=False,
                geom=geom,
            )

            # raise ValueError(
            #    f"Station for grid point {gridpoint_id} not found, even though it is of type 'BPUNKT' and should have a station"
            # )
    elif (
        grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.EPKT
        or grid_point.netzpunkttyp == NetzpunktNetzpunkttyp.APKT
    ):
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
    return station


def add_or_ret_vehicle_type(
    scenario_id, fahrzeugtyp, session
) -> eflips.model.VehicleType:
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


def import_linienfahrplan(
    schedule: Linienfahrplan, scenario_id: int, database_url: str
):
    logger = logging.getLogger(__name__)
    engine = create_engine(database_url)
    with Session(engine) as session:
        try:
            # Create the line object
            db_line = add_or_ret_line(
                scenario_id, schedule.linien_daten.linie.kurzname, session
            )

            # Import the Stations from the "Haltestellenbereiche" entries
            # They don't get coordinates yet, because the coordinates of the "station" object might be influenced by
            # all the "netzpunkte" (also of other lines) that belong to it. So we do that later.
            for (
                haltestellenbereich
            ) in schedule.streckennetz_daten.haltestellenbereiche.haltestellenbereich:
                haltestellenbereich: Linienfahrplan.StreckennetzDaten.Haltestellenbereiche.Haltestellenbereich
                id_no = haltestellenbereich.nummer
                short_name = haltestellenbereich.kurzname
                long_name = haltestellenbereich.fahrplanbuchname

                add_or_ret_station(scenario_id, id_no, long_name, short_name, session)

            # Create a dict from the "Netzpunkzte", to be used in reassembling the routes later on.
            grid_points: Dict[
                int, Linienfahrplan.StreckennetzDaten.Netzpunkte.Netzpunkt
            ] = {}
            for netzpunkt in schedule.streckennetz_daten.netzpunkte.netzpunkt:
                netzpunkt: Linienfahrplan.StreckennetzDaten.Netzpunkte.Netzpunkt
                grid_points[netzpunkt.nummer] = netzpunkt

            # Create a similar dict for the "Streacken", whoch will be the segments we use to assemble the route shape
            segments: Dict[int, Linienfahrplan.StreckennetzDaten.Strecken.Strecke] = {}
            for strecke in schedule.streckennetz_daten.strecken.strecke:
                strecke: Linienfahrplan.StreckennetzDaten.Strecken.Strecke
                segments[strecke.id] = strecke

            # Create a similar dict for the route data, which we will use to assemble the trips
            route_datas: Dict[
                int, Linienfahrplan.LinienDaten.Linie.RoutenDaten.Route
            ] = {}
            for route in schedule.linien_daten.linie.routen_daten.route:
                route: Linienfahrplan.LinienDaten.Linie.RoutenDaten.Route
                route_datas[route.lfd_nr] = route

            # Create a list of the route lfd nr for all the variants
            route_lfd_nrs: Dict[int, int] = {}
            for (
                route_variant
            ) in schedule.linien_daten.linie.routenvarianten.routenvariante:
                route_variant: Linienfahrplan.LinienDaten.Linie.Routenvarianten.Routenvariante
                route_lfd_nrs[route_variant.lfd_nr] = route_variant.lfd_nr_route

            # We need to make sure there is only one "Linie" object, otherwise the route numbers are non-unique
            # If this turns into a list, we need to change the code below
            assert isinstance(
                schedule.linien_daten.linie, Linienfahrplan.LinienDaten.Linie
            )

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
                if (
                    grid_points[departure_station_grid_id].netzpunkttyp
                    == NetzpunktNetzpunkttyp.EPKT
                ):
                    name += "Einsetzfahrt "
                elif (
                    grid_points[arrival_station_grid_id].netzpunkttyp
                    == NetzpunktNetzpunkttyp.APKT
                ):
                    name += "Aussetzfahrt "
                elif (
                    grid_points[arrival_station_grid_id].netzpunkttyp
                    == NetzpunktNetzpunkttyp.BPUNKT
                    or grid_points[departure_station_grid_id].netzpunkttyp
                    == NetzpunktNetzpunkttyp.BPUNKT
                ):
                    pass
                else:
                    assert (
                        grid_points[arrival_station_grid_id].netzpunkttyp
                        == NetzpunktNetzpunkttyp.HST
                        and grid_points[departure_station_grid_id].netzpunkttyp
                        == NetzpunktNetzpunkttyp.HST
                    ), "Check what the other grid point types might mean"
                name += departure_station.name + " → " + arrival_station.name
                name_short = (
                    db_line.name
                    + " "
                    + departure_station.name_short
                    + " → "
                    + arrival_station.name_short
                )

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
                            point.fahrgastwechsel
                            and point.veroeffentlicht
                            and (i > 0 and segment.streckenlaenge > 0)
                        ) or (i == 0 or i == len(route.punktfolge.punkt) - 1):
                            grid_point = grid_points[point.netzpunkt]
                            geom = soldner_to_pointz(
                                grid_point.xkoordinate, grid_point.ykoordinate
                            )
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
                            if (
                                i == len(route.punktfolge.punkt) - 1
                                and segment.streckenlaenge == 0
                            ):
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
                                    existing_route.assoc_route_stations[i].location
                                    != assocs[i].location
                                    and existing_route.assoc_route_stations[
                                        i
                                    ].elapsed_distance
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
                        chain(
                            *[
                                u.fahrzeugtyp
                                for u in umlauf.umlaufteilgruppen.umlaufteilgruppe
                            ]
                        )
                    )
                    assert (
                        len(set(vehicle_type_names)) == 1
                    ), "The vehicle type of all parts of the rotation should be the same"

                    vehicle_type = add_or_ret_vehicle_type(
                        scenario_id, vehicle_type_names[0], session
                    )

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
                        logger.info(
                            f"Trip {fahrt_id} does not actually happen. Skipping"
                        )
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
                    local_midnight = datetime.combine(
                        the_date, time(hour=0, minute=0, second=0), tzinfo=tz
                    )
                    departure_time = local_midnight + timedelta(seconds=fahrt.startzeit)

                    # Load the timing data (the "Fahrtzeitprofil")
                    timing_profile_id = fahrt.fahrzeitprofil
                    timing_profile = [
                        p
                        for p in route_data.fahrzeitprofile.fahrzeitprofil
                        if p.fahrzeitprofil_nummer == timing_profile_id
                    ]
                    assert (
                        len(timing_profile) == 1
                    ), "There should be exactly one timing profile"
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
                        station = add_or_ret_station_for_grid_point(
                            scenario_id, grid_point, grid_points, session
                        )

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
                                stop_times[-1].dwell_duration += timedelta(
                                    seconds=timing_point.streckenfahrzeit
                                )
                        elif (
                            i == len(timing_profile) - 1
                            and station != stop_times[-1].station
                        ):
                            # Create an entry for the arrival station, even if we would not have one otherwise
                            if (
                                timing_point.streckenfahrzeit == 0
                                and timing_point.wartezeit == 0
                            ):
                                if i == 1:
                                    # Now we are in the pathological case that we have only two points, and they have no time between them
                                    # We check if the first point's long name has "Einsetzen" or the last point's long name has "Aussetzen"
                                    # in it.
                                    # For "Einsetzen, we shift the first stop time 10 minutes backward in time and touch the departure time
                                    # For "Aussetzen", we shift the last stop time 10 minutes forward in time

                                    logger.warning(
                                        f"Teleporting buses: Trip {fahrt_id} has only two points, and they have no time between them. Trying to fix it"
                                    )

                                    assert (
                                        len(route_data.punktfolge.punkt) == 2
                                    ), "There should be exactly two points"
                                    first_point_grid_point = grid_points[
                                        route_data.punktfolge.punkt[0].netzpunkt
                                    ]
                                    last_point_grid_point = grid_points[
                                        route_data.punktfolge.punkt[-1].netzpunkt
                                    ]

                                    if "Einsetzen" in first_point_grid_point.langname:
                                        # We are in the Einsetzen case
                                        stop_times[0].arrival_time -= timedelta(
                                            minutes=10
                                        )
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
                            stop_times[-1].dwell_duration += timedelta(
                                seconds=timing_point.wartezeit
                            )

                    arrival_time = (
                        stop_times[-1].arrival_time + stop_times[-1].dwell_duration
                    )

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


def import_line(
    scenario_id: int, schedules: Dict[date, Linienfahrplan], database_url: str
) -> None:
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

    engine = create_engine(database_url)
    if clear_database:
        eflips.model.Base.metadata.drop_all(engine)
        eflips.model.Base.metadata.create_all(engine)

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
            raise ValueError(
                "Invalid log level. Must be one of DEBUG, INFO, WARNING, ERROR, CRITICAL"
            )

    logger = logging.getLogger(__name__)

    if isinstance(paths, str):
        if os.path.isdir(paths):
            # Find all xml files in the directory
            paths = glob.glob(os.path.join(paths, "*.xml"))
        else:
            paths = [paths]
    paths_pathlike = [Path(p) for p in paths]

    TOTAL_STEPS = 2


if __name__ == "__main__":
    fire.Fire(ingest_bvgxml)
