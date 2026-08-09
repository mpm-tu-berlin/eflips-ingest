"""
The finished, database-free model of one import.

Everything upstream of this module reads XML; everything downstream writes rows. A
:class:`Schedule` is the handover: it is fully resolved, internally consistent, and knows
nothing about SQLAlchemy. Building it is also the last point at which anything is dropped,
so :class:`~eflips.ingest.bvgxml._report.IngestReport` can account for the whole import in
one place.
"""
import logging
from dataclasses import dataclass, field
from datetime import date, datetime, timedelta
from typing import Dict, List, Sequence, Tuple
from zoneinfo import ZoneInfo

from eflips.ingest.bvgxml._network import Network, StationKind, StationRef
from eflips.ingest.bvgxml._read import Fahrt, RawCorpus
from eflips.ingest.bvgxml._report import IngestReport
from eflips.ingest.bvgxml._rotations import RawRotation, RotationTable, describe_truncation
from eflips.ingest.bvgxml._routes import ResolvedRoute, RouteTable, TimeProfile

#: The export gives times as seconds since local midnight of the ``Kalenderdatum``.
DEFAULT_TIMEZONE = ZoneInfo("Europe/Berlin")

#: ``Fahrtart`` codes that mean the vehicle carries no passengers: Einsetzfahrt (pull-out),
#: Aussetzfahrt (pull-in) and Betriebsfahrt (a positioning run between two stops).
#:
#: This is exactly the set for which ``fahrgastrelevant`` is ``N``: over the 403,929 trips
#: of the three reference corpora (Berlin 2025-06, the 2026 export and the 2023 dump) the
#: two predicates do not disagree once, in either direction. Everything else (``ST``, ``SF``,
#: ``US``, ``NV``, ``S3``, ``E1``…``E8``) is in service.
DEADHEAD_FAHRTARTEN = frozenset({"E", "A", "B"})


class _UnresolvableRoute(Exception):
    """A rotation runs a route :mod:`._routes` could make nothing of. Internal to this module."""


@dataclass(frozen=True)
class Stop:
    """One stop of a trip, at an absolute time."""

    station: StationRef
    arrival: datetime
    dwell: timedelta


@dataclass(frozen=True)
class Trip:
    """One journey of one vehicle over one route."""

    fahrt_id: int
    route: ResolvedRoute
    is_deadhead: bool
    stops: Tuple[Stop, ...]

    @property
    def departure(self) -> datetime:
        return self.stops[0].arrival

    @property
    def arrival(self) -> datetime:
        return self.stops[-1].arrival + self.stops[-1].dwell


@dataclass(frozen=True)
class Rotation:
    """One vehicle rotation, as a chain of trips."""

    name: str
    vehicle_type: str
    trips: Tuple[Trip, ...]


@dataclass
class Schedule:
    """Everything one import writes to the database."""

    network: Network
    lines: List[str] = field(default_factory=list)
    vehicle_types: List[str] = field(default_factory=list)
    routes: List[ResolvedRoute] = field(default_factory=list)
    rotations: List[Rotation] = field(default_factory=list)
    report: IngestReport = field(default_factory=IngestReport)

    @property
    def stations(self) -> List[StationRef]:
        """Only the stations that are actually used, in a stable order."""
        used = {stop.station for route in self.routes for stop in route.stops}
        return sorted(used, key=lambda ref: ref.sort_key)


def build_schedule(
    corpus: RawCorpus,
    network: Network,
    route_table: RouteTable,
    rotation_table: RotationTable,
    timezone: ZoneInfo = DEFAULT_TIMEZONE,
) -> Schedule:
    """
    Turn the resolved corpus into the finished schedule.

    Only *complete* vehicle rotations are kept. A truncated rotation is one whose
    remaining trips are in files the user did not supply, or on days outside the export;
    keeping it would put a vehicle on the road that never returns to a depot.

    :param corpus: the merged input
    :param network: the resolved network
    :param route_table: the resolved routes
    :param rotation_table: the reassembled vehicle rotations
    :param timezone: the zone the export's midnight offsets are relative to
    :return: the finished schedule
    """
    logger = logging.getLogger(__name__)
    report = IngestReport()
    report.absorb_routes(route_table)

    rotations: List[Rotation] = []
    used_routes: Dict[int, ResolvedRoute] = {}
    used_vehicle_types: Dict[str, None] = {}

    for raw_rotation in rotation_table.rotations.values():
        if not raw_rotation.is_complete:
            report.note_truncated_rotation(raw_rotation, describe_truncation(raw_rotation))
            continue

        try:
            trips = _trips_of(raw_rotation, corpus, network, route_table, timezone, report)
        except _UnresolvableRoute as e:
            # Unlike a degenerate route, one that could not be resolved at all has no known
            # endpoints, so leaving it out would break the vehicle's chain silently. Drop
            # the whole rotation instead, the same way a truncated one is dropped.
            report.note_rotation_on_unresolvable_route(raw_rotation, str(e))
            continue
        if not trips:
            report.note_empty_rotation()
            continue

        _check_chain(raw_rotation, trips, report)

        rotations.append(Rotation(name=raw_rotation.name, vehicle_type=raw_rotation.vehicle_type, trips=tuple(trips)))
        used_vehicle_types.setdefault(raw_rotation.vehicle_type, None)
        for trip in trips:
            used_routes.setdefault(id(trip.route), trip.route)

    routes = sorted(used_routes.values(), key=lambda r: (r.shape.line, r.shape.points))
    lines = sorted({route.shape.line for route in routes})
    report.absorb_written_routes(routes)
    report.n_rotations = len(rotations)
    report.n_trips = sum(len(rotation.trips) for rotation in rotations)
    report.n_routes = len(routes)
    report.n_fahrten_total = len(corpus.fahrten)

    logger.info(
        "Built %d rotations with %d trips over %d routes and %d lines.",
        len(rotations),
        report.n_trips,
        len(routes),
        len(lines),
    )
    return Schedule(
        network=network,
        lines=lines,
        vehicle_types=sorted(used_vehicle_types),
        routes=routes,
        rotations=rotations,
        report=report,
    )


def _trips_of(
    raw_rotation: RawRotation,
    corpus: RawCorpus,
    network: Network,
    route_table: RouteTable,
    timezone: ZoneInfo,
    report: IngestReport,
) -> List[Trip]:
    """Materialise a rotation's trips, in the order the vehicle runs them."""
    trips: List[Trip] = []
    for segment in sorted(raw_rotation.segments, key=lambda s: (s.day, s.beginn_s, s.key)):
        for fahrt_id in segment.fahrt_ids:
            fahrt = corpus.fahrten.get(fahrt_id)
            if fahrt is None:
                raise ValueError(
                    f"Vehicle rotation {raw_rotation.name!r} references Fahrt {fahrt_id}, which "
                    f"no input file defines, even though the Umlaufteilgruppe carrying it "
                    f"does have a Fahrtreihenfolge. The input zip is inconsistent."
                )
            route_id = corpus.route_of_fahrt[fahrt_id]
            if route_id in route_table.unresolvable:
                raise _UnresolvableRoute(f"trip {fahrt_id} runs on a route that could not be resolved")
            route = route_table.route_for(route_id)
            if route is None:
                # The route was dropped as degenerate. Its endpoints are the same station,
                # so the chain stays continuous without it.
                report.n_trips_on_degenerate_routes += 1
                continue
            profile = route_table.profiles[(route_id, fahrt.fahrzeitprofil)]
            trips.append(_trip(fahrt_id, fahrt, route, profile, segment.day, timezone))
    return trips


def _trip(
    fahrt_id: int,
    fahrt: Fahrt,
    route: ResolvedRoute,
    profile: TimeProfile,
    day: date,
    timezone: ZoneInfo,
) -> Trip:
    """One trip, at absolute times."""
    midnight = datetime.combine(day, datetime.min.time(), tzinfo=timezone)
    start = midnight + timedelta(seconds=fahrt.startzeit)
    stops = tuple(
        Stop(
            station=route_stop.station,
            arrival=start + timedelta(seconds=timing.arrival_offset_s),
            dwell=timedelta(seconds=timing.dwell_s),
        )
        for route_stop, timing in zip(route.stops, profile.stops)
    )
    return Trip(
        fahrt_id=fahrt_id,
        route=route,
        # The export says so outright; do not re-derive it from the generated route name,
        # which cannot tell a Betriebsfahrt from an in-service run.
        is_deadhead=fahrt.fahrtart in DEADHEAD_FAHRTARTEN,
        stops=stops,
    )


def _check_chain(raw_rotation: RawRotation, trips: Sequence[Trip], report: IngestReport) -> None:
    """
    Verify that the vehicle can actually run the trips in this order.

    Both properties below hold for every complete rotation in both reference corpora, so a
    violation means either the input contradicts itself or this ingester has a bug. It is
    reported rather than raised: one bad rotation should not fail a whole import.
    """
    for current, following in zip(trips, trips[1:]):
        if current.stops[-1].station != following.stops[0].station:
            report.note_discontinuity(raw_rotation, current, following)
        if current.arrival > following.departure:
            report.note_overlap(raw_rotation, current, following)


def first_and_last_are_depots(rotation: Rotation) -> bool:
    """Whether a rotation runs depot to depot. Used by the report, not to filter."""
    return (
        rotation.trips[0].stops[0].station.kind == StationKind.DEPOT
        and rotation.trips[-1].stops[-1].station.kind == StationKind.DEPOT
    )
