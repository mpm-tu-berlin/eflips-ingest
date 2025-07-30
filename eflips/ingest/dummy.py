import os
import pickle
import shutil
from dataclasses import dataclass
from datetime import timedelta, datetime
from enum import Enum
from pathlib import Path
from typing import Dict, Tuple, Callable
from uuid import UUID, uuid4

import sqlalchemy
from eflips.model.depot import Depot, AssocPlanProcess, Process, Area, Plan, AreaType
from eflips.model.general import Scenario, VehicleType
from eflips.model.network import Station, Line, Route, VoltageLevel, ChargeType
from eflips.model.schedule import Trip, Rotation, TripType
from sqlalchemy import create_engine
from sqlalchemy.orm import Session

from eflips.ingest.base import AbstractIngester


class BusType(Enum):
    """
    An enumeration of bus types.
    """

    SINGLE_DECKER = "Single Decker"
    DOUBLE_DECKER = "Double Decker"
    ARTICULATED = "Articulated"


@dataclass
class PrepareOptions:
    name: str
    depot_count: int
    line_count: int
    rotation_per_line: int
    opportunity_charging: bool
    bus_type: BusType


class DummyIngester(AbstractIngester):
    """
    The dummy ingester is a dummy implementation of the :class:`IngestBase` class. It is used
    to demonstrate the implementation.
    """

    @staticmethod
    def _create_vehicle_types(scenario: Scenario, session: Session, bus_type: BusType) -> None:
        """
        Create the vehicle types in the database.

        :param session: The database session.
        :return: None
        """
        # Add a vehicle type without a battery type
        # Doppeldecker mit mittlerer Batterie

        match bus_type:
            case BusType.SINGLE_DECKER:
                vehicle_type_1 = VehicleType(
                    scenario=scenario,
                    name="Bus Typ Dagobert",
                    battery_capacity=200,
                    charging_curve=[[0, 200], [1, 150]],
                    opportunity_charging_capable=True,
                    consumption=1,
                )
                session.add(vehicle_type_1)
            case BusType.DOUBLE_DECKER:
                # Add second vehicle type without a battery type
                # Kleiner Bus mit kleiner Batterie
                vehicle_type_2 = VehicleType(
                    scenario=scenario,
                    name="Bus Typ Düsentrieb",
                    battery_capacity=100,
                    charging_curve=[[0, 150], [1, 150]],
                    opportunity_charging_capable=True,
                    consumption=1,
                )
                session.add(vehicle_type_2)
            case BusType.ARTICULATED:
                # Add third vehicle type without a battery type
                # Langer Bus mit großer Batterie
                vehicle_type_3 = VehicleType(
                    scenario=scenario,
                    name="Bus Typ Panzerknacker",
                    battery_capacity=300,
                    charging_curve=[[0, 450], [1, 350]],
                    opportunity_charging_capable=True,
                    consumption=1,
                )
                session.add(vehicle_type_3)
            case _:
                raise ValueError("Invalid bus type")

    def prepare(  # type: ignore[override]
        self,
        name: str,
        depot_count: int,
        line_count: int,
        rotation_per_line: int,
        opportunity_charging: bool,
        bus_type: BusType,
        random_text_file: Path,
        progress_callback: None | Callable[[float], None] = None,
    ) -> Tuple[bool, UUID | Dict[str, str]]:
        """
        Dummy prepare method.

        It just checks the values, and then dumps them into a pickle in the temporary directory.

        :param name: The name of the scenario.
        :param depot_count: How many depots to create.
        :param line_count: How many lines to create.
        :param rotation_per_line: How many rotations to create per line.
        :param opportunity_charging: Whether opportunity charging is enabled.
        :param bus_type: Which bus type to use.
        :return: Either a UUID or a dictionary with the error message.
        """

        errors = {}

        if not isinstance(name, str) or len(name) == 0:
            errors["Wrong Name"] = "Name must be a non-empty string."
        if not isinstance(depot_count, int) or depot_count < 1:
            errors["Wrong Depot Count"] = "Depot count must be a positive integer."
        if not isinstance(line_count, int) or line_count < 1:
            errors["Wrong Line Count"] = "Line count must be a positive integer."
        if not isinstance(rotation_per_line, int) or rotation_per_line < 1:
            errors["Wrong Rotation Per Line"] = "Rotation per line must be a positive integer."
        if not isinstance(opportunity_charging, bool):
            errors["Wrong Opportunity Charging"] = "Opportunity charging must be a boolean."
        if not isinstance(bus_type, BusType):
            errors["Wrong Bus Type"] = "Bus type must be a valid bus type."
        if not random_text_file.name.endswith(".txt"):
            errors["Wrong Random Text File"] = "Random text file must end in .txt."

        if errors:
            return False, errors
        else:
            uuid = uuid4()
            temp_dir = self.path_for_uuid(uuid)
            temp_dir.mkdir(parents=True, exist_ok=False)
            shutil.move(random_text_file, temp_dir / random_text_file.name)

            data = PrepareOptions(
                name=name,
                depot_count=depot_count,
                line_count=line_count,
                rotation_per_line=rotation_per_line,
                opportunity_charging=opportunity_charging,
                bus_type=bus_type,
            )

            with open(temp_dir / "data.pkl", "wb") as f:
                pickle.dump(data, f)

            if progress_callback:
                progress_callback(1.0)

            return True, uuid

    def ingest(self, uuid: UUID, progress_callback: None | Callable[[float], None] = None) -> None:
        if not os.path.exists(self.path_for_uuid(uuid) / "data.pkl"):
            raise ValueError("Data file does not exist.")

        # Also check that there is one file ending in .txt
        if not any(p.name.endswith(".txt") for p in self.path_for_uuid(uuid).iterdir()):
            raise ValueError("No text file found.")

        with open(self.path_for_uuid(uuid) / "data.pkl", "rb") as f:
            params = pickle.load(f)
        assert isinstance(params, PrepareOptions)

        engine = create_engine(self.database_url)

        with Session(engine) as session:
            scenario_or_none = session.query(Scenario).filter(Scenario.task_id == uuid).one_or_none()
            if scenario_or_none:
                scenario = scenario_or_none
            else:
                scenario = Scenario(
                    name=params.name,
                )
                session.add(scenario)

            self._create_vehicle_types(scenario, session, params.bus_type)
            session.flush()  # To put the vehicle types into the database

            for i in range(params.depot_count):
                depot = self._create_depot(scenario, session, i)
                for j in range(params.line_count):
                    line = self._create_line(scenario, session, depot, i, j, params.opportunity_charging)
                    for k in range(params.rotation_per_line):
                        self._create_rotation(scenario, session, line, k, params.opportunity_charging)
                        if progress_callback:
                            progress_callback(
                                (i * params.line_count * params.rotation_per_line + j * params.rotation_per_line + k)
                                / (params.depot_count * params.line_count * params.rotation_per_line)
                            )

            session.commit()

    @classmethod
    def prepare_param_names(self) -> Dict[str, str | Dict[Enum, str]]:
        return {
            "name": "Name",
            "depot_count": "Depot Count",
            "line_count": "Lines per Depot",
            "rotation_per_line": "Rotations per Line",
            "opportunity_charging": "Opportunity Charging",
            "bus_type": {
                BusType.SINGLE_DECKER: "Single Decker",
                BusType.DOUBLE_DECKER: "Double Decker",
                BusType.ARTICULATED: "Articulated",
            },
            "random_text_file": "Random Text File",
        }

    @classmethod
    def prepare_param_description(self) -> Dict[str, str | Dict[Enum, str]]:
        return {
            "name": "The name of the scenario.",
            "depot_count": "The number of depots to create.",
            "line_count": "The number of lines to create. For each line, two routes are created.",
            "rotation_per_line": "The number of rotations per line. For each rotation, 20 trips are created.",
            "opportunity_charging": "Whether opportunity charging is enabled. If true, one of the terminals will have a charging station.",
            "bus_type": {
                BusType.SINGLE_DECKER: "Single Decker",
                BusType.DOUBLE_DECKER: "Double Decker",
                BusType.ARTICULATED: "Articulated",
            },
            "random_text_file": "A random text file that is not used for anything. Must end in .txt.",
        }

    def _create_depot(self, scenario: Scenario, session: sqlalchemy.orm.Session, i: int) -> Depot:
        """
        Creates a dummy depot.

        :param scenario: The scenario
        :param session: An SQLAlchemy session
        :param i: The number of the depot
        :return: The depot object (it is already added to the session)
        """

        station = Station(scenario=scenario, name=f"Station for Depot {i}", is_electrified=False)
        session.add(station)

        depot = Depot(scenario=scenario, name=f"Depot {i}", name_short=f"D{i}", station=station)
        session.add(depot)

        # Dirtily load the vehicle type
        with session.no_autoflush:
            vehicle_type = session.query(VehicleType).filter(VehicleType.scenario_id == scenario.id).one()

        # Create plan
        plan = Plan(scenario=scenario, name="Entenhausen Plan")
        session.add(plan)

        depot.default_plan = plan

        # Create areas
        arrival_area = Area(
            scenario=scenario,
            name="Entenhausen Depot Arrival Area",
            depot=depot,
            area_type=AreaType.DIRECT_ONESIDE,
            capacity=6,
            vehicle_type=vehicle_type,
        )
        session.add(arrival_area)

        cleaning_area = Area(
            scenario=scenario,
            name="Entenhausen Depot Cleaning Area",
            depot=depot,
            area_type=AreaType.DIRECT_ONESIDE,
            capacity=6,
            vehicle_type=vehicle_type,
        )
        session.add(cleaning_area)

        charging_area = Area(
            scenario=scenario,
            name="Entenhausen Depot Area",
            depot=depot,
            area_type=AreaType.LINE,
            capacity=6,
            row_count=1,
            vehicle_type=vehicle_type,
        )
        session.add(charging_area)

        # Create processes
        standby_arrival = Process(
            name="Standby Arrival",
            scenario=scenario,
            dispatchable=False,
        )
        session.add(standby_arrival)

        clean = Process(
            name="Clean",
            scenario=scenario,
            dispatchable=False,
            duration=timedelta(minutes=30),
        )
        session.add(clean)

        charging = Process(
            name="Charging",
            scenario=scenario,
            dispatchable=False,
            electric_power=150,
        )
        session.add(charging)

        standby_departure = Process(
            name="Standby Departure",
            scenario=scenario,
            dispatchable=True,
        )
        session.add(standby_departure)

        # Connect the areas and processes. *The final area needs to have both a charging and standby_departure process*

        arrival_area.processes.append(standby_arrival)
        cleaning_area.processes.append(clean)
        charging_area.processes.append(charging)
        charging_area.processes.append(standby_departure)

        assocs = [
            AssocPlanProcess(scenario=scenario, process=standby_arrival, plan=plan, ordinal=0),
            AssocPlanProcess(scenario=scenario, process=clean, plan=plan, ordinal=1),
            AssocPlanProcess(scenario=scenario, process=charging, plan=plan, ordinal=2),
            AssocPlanProcess(scenario=scenario, process=standby_departure, plan=plan, ordinal=3),
        ]
        session.add_all(assocs)

        return depot

    def _create_line(
        self,
        scenario: Scenario,
        session: sqlalchemy.orm.Session,
        depot: Depot,
        i: int,
        j: int,
        opportuinity_charging: bool,
    ) -> Line:
        bus_line = Line(scenario=scenario, name=f"Bus Line {i}-{j}")
        session.add(bus_line)

        # Create an outbound terminal
        outbound_terminal = Station(scenario=scenario, name=f"Outbound Terminal for {i}-{j}")
        if opportuinity_charging:
            outbound_terminal.is_electrified = True
            outbound_terminal.amount_charging_places = 1
            outbound_terminal.power_per_charger = 150
            outbound_terminal.power_total = 150
            outbound_terminal.voltage_level = VoltageLevel.MV
            outbound_terminal.charge_type = ChargeType.OPPORTUNITY
        else:
            outbound_terminal.is_electrified = False
        session.add(outbound_terminal)

        # Create outbound and inbound routes
        outbound_route = Route(
            scenario=scenario,
            name=f"Outbound Route for {i}-{j}",
            line=bus_line,
            departure_station=depot.station,
            arrival_station=outbound_terminal,
            distance=5000,
        )
        session.add(outbound_route)
        inbound_route = Route(
            scenario=scenario,
            name=f"Inbound Route for {i}-{j}",
            line=bus_line,
            departure_station=outbound_terminal,
            arrival_station=depot.station,
            distance=5000,
        )
        session.add(inbound_route)

        return bus_line

    def _create_rotation(
        self, scenario: Scenario, session: sqlalchemy.orm.Session, line: Line, k: int, opportunity_charging: bool
    ) -> None:
        # dirtily load the vehicle type
        vehicle_type = session.query(VehicleType).filter(VehicleType.scenario_id == scenario.id).one()

        # Create 10 inbound and outbound trips with 20 minutes between them offset by k*10 minutes
        first_departure = datetime(2024, 1, 1, 6, 0) + timedelta(minutes=k * 10)
        trip_duration = timedelta(minutes=30)
        break_duration = timedelta(minutes=10)

        inbound_route = session.query(Route).filter(Route.line_id == line.id, Route.name.like("Inbound%")).one()
        outbound_route = session.query(Route).filter(Route.line_id == line.id, Route.name.like("Outbound%")).one()

        next_departure = first_departure

        rotation = Rotation(
            scenario=scenario,
            name=f"Rotation {line.name} {k}",
            vehicle_type=vehicle_type,
            allow_opportunity_charging=opportunity_charging,
        )
        session.add(rotation)

        for i in range(10):
            outbound_trip = Trip(
                scenario=scenario,
                rotation=rotation,
                route=outbound_route,
                departure_time=next_departure,
                arrival_time=next_departure + trip_duration,
                trip_type=TripType.PASSENGER,
            )
            session.add(outbound_trip)
            next_departure += trip_duration + break_duration

            inbound_trip = Trip(
                scenario=scenario,
                rotation=rotation,
                route=inbound_route,
                departure_time=next_departure,
                arrival_time=next_departure + trip_duration,
                trip_type=TripType.PASSENGER,
            )
            session.add(inbound_trip)
            next_departure += trip_duration + break_duration
