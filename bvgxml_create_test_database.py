#! /usr/bin/env python3
import os
from multiprocessing import Pool
from typing import Dict, Tuple

from eflips.model import *
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from tqdm.auto import tqdm

if "DATABASE_URL" not in os.environ:
    raise Exception("DATABASE_URL not set")
DATABASE_URL = os.environ["DATABASE_URL"]
engine = create_engine(DATABASE_URL, echo=False)


def find_first_last_stop_for_rotation_id(rotation_id: int) -> Tuple[int, int]:
    """
    Finds the first and last stop for a given rotation id
    """
    rotation_id = rotation_id[0]
    with Session(engine) as session:
        rotation = session.query(Rotation).filter(Rotation.id == rotation_id).one()
        first_stop = rotation.trips[0].route.departure_station_id
        last_stop = rotation.trips[-1].route.arrival_station_id
        return first_stop, last_stop


def group_rotations_by_start_end_stop() -> Dict[Tuple[int, int], int]:
    """
    Groups rotations by start, end and stop
    """
    with Session(engine) as session:
        rotation_ids = session.query(Rotation.id).filter(Rotation.scenario_id == 1).all()
        grouped_rotations: Dict[Tuple[int, int], int] = {}
        with Pool() as pool:
            for result in tqdm(
                pool.imap_unordered(find_first_last_stop_for_rotation_id, rotation_ids),
                total=len(rotation_ids),
            ):
                key = (result[0], result[1])
                if key not in grouped_rotations:
                    grouped_rotations[key] = 0
                grouped_rotations[key] += 1
        return grouped_rotations


def print_rotations_by_station() -> None:
    grouped_rotations = group_rotations_by_start_end_stop()

    # order by length of list
    sorted_dict = {k: v for k, v in sorted(grouped_rotations.items(), key=lambda item: item[1], reverse=True)}
    with Session(engine) as session:
        for key, rotations in sorted_dict.items():
            first_station_id, last_station_id = key
            first_station = session.query(Station).filter(Station.id == first_station_id).one()
            last_station = session.query(Station).filter(Station.id == last_station_id).one()
            print(f"{first_station.name} ({first_station_id}) -> {last_station.name} ({last_station_id}): {rotations}")


def prune_scenario(station: Station, scenario_id: int, session: Session) -> None:
    """
    Creates a scenario with all the rotations starting and ending at the given station
    """
    # Sum up all the objects that will be pruned
    total_count = session.query(Rotation).filter(Rotation.scenario_id == scenario_id).count()
    total_count += session.query(Route).filter(Route.scenario_id == scenario_id).count()
    total_count += session.query(Line).filter(Line.scenario_id == scenario_id).count()
    total_count += session.query(Station).filter(Station.scenario_id == scenario_id).count()

    # Create a global progress bar
    with tqdm(total=total_count, smoothing=0) as pbar:
        # Find all the rotations *not* starting and ending at this station
        rotations = session.query(Rotation).filter(Rotation.scenario_id == scenario_id).all()
        dropped_rotation_ids = []
        for rotation in rotations:
            if (
                rotation.trips[0].route.departure_station_id == station.id
                and rotation.trips[-1].route.arrival_station_id == station.id
            ):
                pbar.update(1)
                continue
            else:
                pbar.update(1)
                dropped_rotation_ids.append(rotation.id)
        dropped_stop_times_q = session.query(StopTime.id).join(Trip).filter(Trip.rotation_id.in_(dropped_rotation_ids))
        session.query(StopTime).filter(StopTime.id.in_(dropped_stop_times_q)).delete()
        dropped_trips_q = session.query(Trip).filter(Trip.rotation_id.in_(dropped_rotation_ids))
        dropped_trips = dropped_trips_q.delete()
        dropped_rotation_q = session.query(Rotation).filter(Rotation.id.in_(dropped_rotation_ids))
        dropped_rotations = dropped_rotation_q.delete()

        # Find all the routes that now have no trips
        routes = session.query(Route).filter(Route.scenario_id == scenario_id).all()
        droppped_route_ids = []
        for route in routes:
            if len(route.trips) == 0:
                droppped_route_ids.append(route.id)
            pbar.update(1)
        dropped_assoc_route_station_q = session.query(AssocRouteStation).filter(
            AssocRouteStation.route_id.in_(droppped_route_ids)
        )
        dropped_assoc_route_station = dropped_assoc_route_station_q.delete()
        dropped_route_q = session.query(Route).filter(Route.id.in_(droppped_route_ids))
        dropped_routes = dropped_route_q.delete()

        # Find all the lines that now have no routes
        dropped_line_ids = []
        lines = session.query(Line).filter(Line.scenario_id == scenario_id).all()
        for line in lines:
            if len(line.routes) == 0:
                dropped_line_ids.append(line.id)
            pbar.update(1)
        dropped_line_q = session.query(Line).filter(Line.id.in_(dropped_line_ids))
        dropped_lines = dropped_line_q.delete()

        # Find all the stations that now have no routes
        dropped_station_ids = []
        stations = session.query(Station).filter(Station.scenario_id == scenario_id).all()
        for station in stations:
            if len(station.assoc_route_stations) == 0:
                dropped_station_ids.append(station.id)
            pbar.update(1)
        dropped_station_q = session.query(Station).filter(Station.id.in_(dropped_station_ids))
        dropped_stations = dropped_station_q.delete()


if __name__ == "__main__":
    # print_rotations_by_station()

    # Load the most interesting stations and take the first six
    grouped_rotations = group_rotations_by_start_end_stop()
    # order by length of list
    sorted_dict = {k: v for k, v in sorted(grouped_rotations.items(), key=lambda item: item[1], reverse=True)}

    # Take the first six
    stationss = list(sorted_dict.keys())[:6]
    for stations in stationss:
        assert len(stations) == 2
        assert stations[0] == stations[1]
        # Copy a scenario
        with Session(engine) as session:
            try:
                station_name = session.query(Station).filter(Station.id == stations[0]).one().name
                station_short_name = session.query(Station).filter(Station.id == stations[0]).one().name_short

                scenario = session.query(Scenario).filter(Scenario.id == 1).one()
                new_scenario = scenario.clone(session)
                session.expunge_all()  # With the new cloning code, this will not be necessary, but for now it is
                new_scenario.name = f"All Rotations starting and ending at {station_name}"

                # Find this station in the new scenario
                station = (
                    session.query(Station)
                    .filter(Station.scenario_id == new_scenario.id)
                    .filter(Station.name == station_name)
                    .filter(Station.name_short == station_short_name)
                    .one()
                )

                prune_scenario(station, new_scenario.id, session)
            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.commit()
