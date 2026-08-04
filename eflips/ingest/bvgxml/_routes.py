"""
Resolving ``Route`` elements into the geometry and timing of the output model.

A route is a ``Punktfolge`` — an ordered list of grid points — plus one
``Fahrzeitprofil`` per driving-time variant. Turning that into an
:class:`eflips.model.Route` is a fold over the points:

1. **hop lengths** come from the corpus-wide segment table, keyed by ``(from, to)``.
   ``Strecke/ID`` is file-local and is not used.
2. **repair**: if a ``Fahrzeitprofil`` has more points than the ``Punktfolge``, the export
   collapsed the route and the omitted stops are restored from the Streckennetz.
3. **canonicalisation**: each point is replaced by the station it belongs to (see
   :mod:`._network`) and consecutive duplicates are folded away. The ``BPunkt`` bracket
   that wraps every in-service route disappears here on its own, because ``BPunkt(X)`` and
   ``Hst(X)`` resolve to the same station.
4. **timing**: driving and waiting times are accumulated into arrival offsets, then spread
   so that they strictly increase — the export's resolution is one minute, so several
   stops routinely share an arrival time.

Route identity is the :class:`RouteShape`: the line plus the verbatim ``Punktfolge``.
``Route/LfdNr`` is renumbered between export days and ``externeRoutennummer`` maps to more
than one Punktfolge, so neither can be used.
"""
import logging
import math
from dataclasses import dataclass, field
from typing import Dict, FrozenSet, List, Optional, Sequence, Set, Tuple

from eflips.ingest.bvgxml._network import Network, StationRef
from eflips.ingest.bvgxml._read import RawCorpus, Route, RouteId
from eflips.ingest.bvgxml._xmldata import NetzpunktNetzpunkttyp

# --------------------------------------------------------------------------------------
# Calibration
#
# The two constants below stand in for data the export does not carry. Both were measured
# on the Berlin 2025-06 corpus (1403 files, 194,870 trips) rather than guessed.
# --------------------------------------------------------------------------------------

#: Multiplier from crow-fly to road distance, used when no Strecke is available.
#:
#: From data: over the 1,230 depot legs of the Berlin 2025-06 corpus that carry both a
#: Streckenlänge and coordinates, the road/crow-fly ratio has a median of 1.29 and a 90th
#: percentile of 1.66. 1.4 sits between the two: deliberately conservative, because an
#: underestimated deadhead distance understates energy consumption, which is the thing
#: this data is imported to compute.
CROW_FLY_DETOUR_FACTOR: float = 1.4

#: Floor for an estimated distance, for when even the crow-fly distance is zero because
#: the two grid points share a coordinate.
MIN_ESTIMATED_DISTANCE_M: float = 1000.0

#: Smallest road-to-crow-fly ratio a depot connection's ``Streckenlänge`` may have before it
#: is rejected as impossible and estimated instead.
#:
#: A road cannot be shorter than the straight line between its ends, so anything below 1.0
#: is already impossible; the tolerance below it absorbs coordinate noise on short hops.
#:
#: From data: the ratio is sharply bimodal on all three reference corpora. The believable
#: depot Strecken sit between 1.11 (p25) and 1.65 (p90) with a median of 1.29 — the same
#: 1.29 that :data:`CROW_FLY_DETOUR_FACTOR` is calibrated on — while the bad ones sit at
#: about 0.01. *Nothing at all* falls between 0.31 and 1.11, so the exact threshold within
#: that gap does not matter; 0.8 is inside it with room to spare.
#:
#: What it catches: 186 of 186 depot Strecken in the 2026 UGFPL export, which claims a flat
#: 60 m between points its own coordinates place up to 21.5 km apart, and about 15 % of them
#: in Berlin 2025-06 and the 2023 dump. Taking those at face value produced nine-second
#: pull-outs for hour-long deadheads — understating deadhead energy and overstating the time
#: a vehicle has to charge, both in the unsafe direction.
MIN_PLAUSIBLE_ROAD_TO_CROW_FLY_RATIO: float = 0.8

#: Assumed speed on a depot leg whose Fahrzeitprofil carries no driving time at all.
#:
#: From data: in a 400-file sample of the Berlin 2025-06 corpus, the 3,381 depot legs that
#: *do* carry a driving time run at a median of 24 km/h (p10 17, p90 32). The previous
#: value of 30 km/h sat at roughly the 88th percentile and so systematically produced
#: deadheads that were too short — the unsafe direction, since a deadhead that is quicker
#: than reality leaves the vehicle more time to charge than it will really have.
DEPOT_LEG_SPEED_KMH: float = 24.0

#: Reject a reconstruction whose driving times — which the search itself never looks at —
#: would imply a higher speed than this on any spliced-in segment. Genuine express-bus
#: hops peak at ~79 km/h in the 2026 export.
MAX_RECONSTRUCTION_SPEED_KMH: float = 100.0

#: Ceiling on the number of segments the reconstruction search tries before giving up, so
#: that a densely connected network cannot stall the ingest.
MAX_RECONSTRUCTION_SEARCH_STEPS: int = 1_000_000

#: The export's temporal resolution: every Streckenfahrzeit and Wartezeit in both
#: reference corpora is a whole number of minutes. Stops sharing an arrival time are
#: therefore spread across at most this many seconds — see :func:`spread_arrivals`.
EXPORT_TIME_RESOLUTION_S: int = 60


@dataclass(frozen=True)
class RouteShape:
    """The identity of a route: its line and the grid points it runs over, verbatim."""

    line: str
    points: Tuple[int, ...]


@dataclass(frozen=True)
class RouteStop:
    """One station along a resolved route."""

    station: StationRef
    grid_point: int  # the point whose coordinates represent this stop
    elapsed_distance_m: float


@dataclass(frozen=True)
class ResolvedRoute:
    """A route of the output model. One per distinct :class:`RouteShape`."""

    shape: RouteShape
    stops: Tuple[RouteStop, ...]
    distance_m: float
    distance_estimated: bool
    name: str
    name_short: str
    headsign: Optional[str]

    @property
    def departure(self) -> RouteStop:
        return self.stops[0]

    @property
    def arrival(self) -> RouteStop:
        return self.stops[-1]


@dataclass(frozen=True)
class StopTiming:
    """When a vehicle reaches one stop of a route, relative to the trip's ``Startzeit``."""

    arrival_offset_s: int
    dwell_s: int

    @property
    def departure_offset_s(self) -> int:
        return self.arrival_offset_s + self.dwell_s


@dataclass(frozen=True)
class TimeProfile:
    """One ``Fahrzeitprofil``, folded onto the stops of the resolved route."""

    stops: Tuple[StopTiming, ...]

    @property
    def departure_offset_s(self) -> int:
        return self.stops[0].arrival_offset_s

    @property
    def arrival_offset_s(self) -> int:
        return self.stops[-1].departure_offset_s


@dataclass
class RouteTable:
    """Every route of the corpus, resolved."""

    #: The shape of each input route, or ``None`` if it was dropped as degenerate.
    shape_of: Dict[RouteId, Optional[RouteShape]]

    #: The output route for each distinct shape.
    routes: Dict[RouteShape, ResolvedRoute]

    #: The timings of each input route's Fahrzeitprofile, by ``(route, profile number)``.
    profiles: Dict[Tuple[RouteId, int], TimeProfile]

    #: Routes whose own data is self-contradictory, so nothing could be made of them. Kept
    #: apart from the degenerate ones: a degenerate route can be dropped without breaking
    #: the vehicle's chain, this kind cannot, so its workings have to go too.
    unresolvable: Set[RouteId] = field(default_factory=set)

    #: Counts for the ingest summary.
    n_estimated_distance: int = 0
    n_reconstructed: int = 0
    n_reconstruction_failed: int = 0
    n_degenerate: int = 0
    n_zero_duration: int = 0

    def route_for(self, route_id: RouteId) -> Optional[ResolvedRoute]:
        shape = self.shape_of.get(route_id)
        return None if shape is None else self.routes[shape]


def resolve_routes(corpus: RawCorpus, network: Network) -> RouteTable:
    """
    Resolve every ``Route`` of the corpus.

    :param corpus: the merged input
    :param network: the resolved network
    :return: the route table
    """
    logger = logging.getLogger(__name__)
    table = RouteTable(shape_of={}, routes={}, profiles={})

    for route_id, route in corpus.routen.items():
        line = corpus.files[route_id[0]].linie
        try:
            resolved = _resolve_one(route, route_id, line, corpus, network, table)
        except ValueError as e:
            # One route whose own numbers contradict each other must not cost the user a
            # 1,400-file import. Record it and carry on; the workings that run it are
            # dropped in :func:`~eflips.ingest.bvgxml._schedule.build_schedule`, because
            # unlike a degenerate route this one cannot just be left out of the chain.
            logger.warning(
                "Route %d of line %s could not be resolved: %s Dropping the route and "
                "every vehicle working that runs it.",
                route.lfd_nr,
                line,
                e,
            )
            table.shape_of[route_id] = None
            table.unresolvable.add(route_id)
            continue
        if resolved is None:
            table.shape_of[route_id] = None
            continue
        shape, output_route, profiles = resolved
        table.shape_of[route_id] = shape
        # Geometry is a function of the shape alone, so an identical shape read from
        # another file yields an identical route; keep the first and let the rest alias it.
        table.routes.setdefault(shape, output_route)
        for number, profile in profiles.items():
            table.profiles[(route_id, number)] = profile

    logger.info(
        "Resolved %d input routes into %d distinct routes (%d with an estimated distance, "
        "%d reconstructed from the Streckennetz, %d dropped as degenerate).",
        len(table.shape_of),
        len(table.routes),
        table.n_estimated_distance,
        table.n_reconstructed,
        table.n_degenerate,
    )
    return table


def _resolve_one(
    route: Route,
    route_id: RouteId,
    line: str,
    corpus: RawCorpus,
    network: Network,
    table: RouteTable,
) -> Optional[Tuple[RouteShape, ResolvedRoute, Dict[int, TimeProfile]]]:
    """Resolve one route, or return ``None`` if it collapses to fewer than two stations."""
    logger = logging.getLogger(__name__)

    points = [punkt.netzpunkt for punkt in route.punktfolge.punkt]
    profiles_raw = {
        profil.fahrzeitprofil_nummer: [
            (punkt.streckenfahrzeit, punkt.wartezeit) for punkt in profil.fahrzeitprofilpunkte.punkt
        ]
        for profil in route.fahrzeitprofile.fahrzeitprofil
    }

    covered = corpus.route_covered_hops.get(route_id, frozenset())
    points, profiles_raw, covered = _repair_collapsed(points, profiles_raw, covered, network, line, route.lfd_nr, table)

    # The hop the export collapsed, if any: the first one its Streckenfolge does not cover,
    # else the last (which is where a circular route's omitted loop went).
    gap = next(
        (i for i in range(len(points) - 1) if (points[i], points[i + 1]) not in covered),
        len(points) - 2,
    )

    stations = [network.station_of(point) for point in points]
    runs = _folded_runs(stations)
    if len(runs) < 2:
        # The whole Punktfolge is one station: a vehicle standing at a turnaround. Three
        # such routes exist in Berlin 2025-06 and none in the 2026 export, whose one
        # candidate the repair above turns back into a real loop. Dropping the route keeps
        # its rotation continuous, because a route that folds to one station necessarily
        # begins and ends at that station.
        table.n_degenerate += 1
        logger.warning(
            "Route %d of line %s stays at one station (%s) for its whole Punktfolge %s. "
            "Dropping the route and any trips on it.",
            route.lfd_nr,
            line,
            network.stations[stations[0]].name,
            points,
        )
        return None

    hop_lengths, estimated = _hop_lengths(points, network, line, route.lfd_nr)
    elapsed = [0.0]
    for length in hop_lengths:
        elapsed.append(elapsed[-1] + length)

    if elapsed[-1] == 0.0:
        # No Streckenlänge anywhere along the route — typical of a depot connection the
        # export never measured. Estimate the whole leg end to end.
        elapsed[-1] = _estimated_distance_m(network, points[0], points[-1])
        estimated = True
        logger.warning(
            "Route %d of line %s has no distance at all. Estimating %.0f m from the "
            "crow-fly distance between its endpoints.",
            route.lfd_nr,
            line,
            elapsed[-1],
        )
    if estimated:
        table.n_estimated_distance += 1

    stops = tuple(
        RouteStop(station=stations[index], grid_point=points[index], elapsed_distance_m=elapsed[index])
        for index in runs
    )
    stops = _enforce_increasing_distance(stops, network, line, route.lfd_nr)

    shape = RouteShape(line=line, points=tuple(points))
    output_route = _build_route(shape, stops, estimated, route, network, points)

    context = _RouteContext(
        network=network,
        line=line,
        lfd_nr=route.lfd_nr,
        points=points,
        runs=runs,
        gap=gap,
        distance_m=stops[-1].elapsed_distance_m,
    )
    profiles: Dict[int, TimeProfile] = {}
    derived_any = False
    for number, raw in profiles_raw.items():
        profiles[number], derived = _build_profile(raw, number, context)
        derived_any |= derived
    if derived_any:
        table.n_zero_duration += 1
    return shape, output_route, profiles


# --------------------------------------------------------------------------------------
# Geometry
# --------------------------------------------------------------------------------------


def _hop_lengths(points: Sequence[int], network: Network, line: str, lfd_nr: int) -> Tuple[List[float], bool]:
    """
    Length in metres of each hop.

    A hop the corpus has no Strecke for is estimated from the coordinates, and so is one
    whose Strecke is too short to be a road between its own endpoints — see
    :data:`MIN_PLAUSIBLE_ROAD_TO_CROW_FLY_RATIO`. Both cases mark the route's distance as
    estimated, which puts ``CHECK DISTANCE:`` in its name.
    """
    logger = logging.getLogger(__name__)
    lengths: List[float] = []
    estimated = False
    for start, end in zip(points, points[1:]):
        length = network.segment_length.get((start, end))
        if length is not None and not _is_implausibly_short(network, start, end, length):
            lengths.append(float(length))
            continue

        estimate = _estimated_distance_m(network, start, end, floor=1.0)
        lengths.append(estimate)
        estimated = True
        if length is None:
            logger.warning(
                "Route %d of line %s has no Strecke for the leg from %s to %s. Estimating "
                "%.0f m from the crow-fly distance.",
                lfd_nr,
                line,
                network.grid_points[start].name,
                network.grid_points[end].name,
                estimate,
            )
        else:
            logger.warning(
                "Route %d of line %s records %d m for the depot leg from %s to %s, which "
                "are %.0f m apart in a straight line — a road cannot be that short. "
                "Estimating %.0f m instead.",
                lfd_nr,
                line,
                length,
                network.grid_points[start].name,
                network.grid_points[end].name,
                _crow_fly_m(network, start, end),
                estimate,
            )
    return lengths, estimated


def _is_implausibly_short(network: Network, start: int, end: int, length: int) -> bool:
    """
    Whether a depot connection's recorded length is too short to be a road.

    Restricted to depot legs because that is where the export gets it wrong — a flat 60 m
    for every connection, whatever the depot's actual distance — and because the in-service
    hops are short enough that coordinate noise would make the same test fire on good data.
    """
    if not (network.grid_points[start].is_depot or network.grid_points[end].is_depot):
        return False
    return length < MIN_PLAUSIBLE_ROAD_TO_CROW_FLY_RATIO * _crow_fly_m(network, start, end)


def _crow_fly_m(network: Network, start: int, end: int) -> float:
    """The straight-line distance between two grid points, in metres."""
    first, last = network.grid_points[start], network.grid_points[end]
    return math.hypot(first.x - last.x, first.y - last.y) / 1000.0  # Soldner, in mm


def _estimated_distance_m(network: Network, start: int, end: int, floor: float = MIN_ESTIMATED_DISTANCE_M) -> float:
    """Estimate a road distance the export does not give us, from the coordinates."""
    return max(CROW_FLY_DETOUR_FACTOR * _crow_fly_m(network, start, end), floor)


def _folded_runs(stations: Sequence[StationRef]) -> List[int]:
    """
    Group consecutive points that belong to the same station, and pick one to represent
    each group.

    A run is represented by its **last** point — the moment the vehicle leaves the station
    — except the first run, which is represented by its **first** point. That is not a
    special case for its own sake: the export's ``Startzeit`` is the departure at point 0
    of the ``Punktfolge``, so the trip has to begin there, at elapsed distance zero. The
    two choices together also give exactly what the output model requires of a route: its
    first stop at distance zero and its last at the route's full length.

    :return: one representative point index per station along the route
    """
    runs: List[Tuple[int, int]] = []
    for index, station in enumerate(stations):
        if runs and stations[runs[-1][1]] == station:
            runs[-1] = (runs[-1][0], index)
        else:
            runs.append((index, index))
    return [first if position == 0 else last for position, (first, last) in enumerate(runs)]


def _enforce_increasing_distance(
    stops: Tuple[RouteStop, ...], network: Network, line: str, lfd_nr: int
) -> Tuple[RouteStop, ...]:
    """
    Make the elapsed distance strictly increase, which the output model requires.

    Two distinct stations at the same elapsed distance mean the export recorded a zero
    length for a hop that really is a hop; estimate it like any other missing length.
    """
    logger = logging.getLogger(__name__)
    fixed = [stops[0]]
    shift = 0.0
    for previous, stop in zip(stops, stops[1:]):
        distance = stop.elapsed_distance_m + shift
        if distance <= fixed[-1].elapsed_distance_m:
            estimate = _estimated_distance_m(network, previous.grid_point, stop.grid_point, floor=1.0)
            logger.info(
                "Route %d of line %s records no distance between %s and %s. Estimating "
                "%.0f m so the elapsed distance keeps increasing.",
                lfd_nr,
                line,
                network.stations[previous.station].name,
                network.stations[stop.station].name,
                estimate,
            )
            distance = fixed[-1].elapsed_distance_m + estimate
            shift = distance - stop.elapsed_distance_m
        fixed.append(RouteStop(station=stop.station, grid_point=stop.grid_point, elapsed_distance_m=distance))
    return tuple(fixed)


def _build_route(
    shape: RouteShape,
    stops: Tuple[RouteStop, ...],
    estimated: bool,
    route: Route,
    network: Network,
    points: Sequence[int],
) -> ResolvedRoute:
    """Name a resolved route and package it up."""
    departure = network.stations[stops[0].station]
    arrival = network.stations[stops[-1].station]

    first_typ = network.grid_points[points[0]].typ
    last_typ = network.grid_points[points[-1]].typ
    if first_typ == NetzpunktNetzpunkttyp.EPKT:
        kind = "Einsetzfahrt "
    elif last_typ == NetzpunktNetzpunkttyp.APKT:
        kind = "Aussetzfahrt "
    else:
        kind = ""

    name = f"{shape.line} {kind}{departure.name} → {arrival.name}"
    if estimated:
        # Keep the marker the previous implementation used, so an operator can still find
        # every route whose length is a guess with one query.
        name = "CHECK DISTANCE: " + name

    return ResolvedRoute(
        shape=shape,
        stops=stops,
        distance_m=stops[-1].elapsed_distance_m,
        distance_estimated=estimated,
        name=name,
        name_short=f"{shape.line} {departure.name_short} → {arrival.name_short}",
        headsign=_headsign(route),
    )


def _headsign(route: Route) -> Optional[str]:
    """
    The destination sign shown on this route.

    Points may switch the sign part-way along a route; the one that matters for the route
    as a whole is the first. Falls back to the route's own display list.
    """
    displays = {display.nummer: display.anzeige_text for display in route.zielanzeigen.zielanzeige}
    for punkt in route.punktfolge.punkt:
        if punkt.zielanzeige is not None and punkt.zielanzeige in displays:
            return displays[punkt.zielanzeige]
    for display in route.zielanzeigen.zielanzeige:
        return display.anzeige_text
    return None


# --------------------------------------------------------------------------------------
# Repair of collapsed Punktfolgen
# --------------------------------------------------------------------------------------


def _repair_collapsed(
    points: List[int],
    profiles_raw: Dict[int, List[Tuple[int, int]]],
    covered: FrozenSet[Tuple[int, int]],
    network: Network,
    line: str,
    lfd_nr: int,
    table: RouteTable,
) -> Tuple[List[int], Dict[int, List[Tuple[int, int]]], FrozenSet[Tuple[int, int]]]:
    """
    Restore the points a collapsed ``Punktfolge`` is missing, from the Streckennetz.

    Some routes come out of the export with their ``Punktfolge`` reduced to the endpoints
    of the run while the ``Fahrzeitprofil`` still lists every stop. The stops are not lost:
    the ``Strecken`` connecting them are still in the network, usually referenced by no
    route at all. So the omitted stops are the walk through that network which leads from
    the last point before the gap to the first point after it in exactly as many segments
    as the profile has entries to spare.

    The walk is only accepted when it is the *only* one of that length and when the
    profile's driving times — which the search never looks at — imply a plausible speed
    over every one of its segments. Otherwise the points are left alone and the collapsed
    leg is estimated instead.

    This affects 13 of 490 routes in the 2026 UGFPL export and none at all in Berlin
    2025-06, so it is a deployment quirk rather than a property of the format.
    """
    logger = logging.getLogger(__name__)

    profile_lengths = {len(raw) for raw in profiles_raw.values()}
    if len(profile_lengths) != 1:
        return points, profiles_raw, covered  # Contradictory profiles: we cannot tell
    missing = profile_lengths.pop() - len(points)
    if missing <= 0 or len(points) < 2:
        return points, profiles_raw, covered

    gap = next(
        (i for i in range(len(points) - 1) if (points[i], points[i + 1]) not in covered),
        len(points) - 2,
    )
    walks, exhaustive = _walks_between(network, points[gap], points[gap + 1], missing + 1)
    if len(walks) != 1 or not exhaustive:
        table.n_reconstruction_failed += 1
        if not exhaustive:
            outcome = "the search for one was given up on"
        else:
            outcome = f"the Streckennetz has {'none' if not walks else 'more than one'}"
        logger.warning(
            "Route %d of line %s omits %d points of its Punktfolge, so they would have to "
            "be a connection of %d Strecken between %s and %s, but %s. Not reconstructing "
            "them; the leg's distance will be estimated instead.",
            lfd_nr,
            line,
            missing,
            missing + 1,
            network.grid_points[points[gap]].name,
            network.grid_points[points[gap + 1]].name,
            outcome,
        )
        return points, profiles_raw, covered
    walk = walks[0]

    # The driving times now line up one-to-one with the segments of the walk. They are an
    # independent measure of each segment's length, so an implausible speed means we would
    # be splicing in the wrong part of the network.
    for raw in profiles_raw.values():
        for (start, end), (driving, _dwell) in zip(walk, raw[gap + 1 : gap + 1 + len(walk)]):
            if driving <= 0:
                continue  # Whole-minute rounding leaves short hops at zero
            speed_kmh = 3.6 * network.segment_length[(start, end)] / driving
            if speed_kmh > MAX_RECONSTRUCTION_SPEED_KMH:
                table.n_reconstruction_failed += 1
                logger.warning(
                    "Route %d of line %s: the connection reconstructed for its %d omitted "
                    "points would have to be driven at %.0f km/h between %s and %s. Not "
                    "reconstructing them.",
                    lfd_nr,
                    line,
                    missing,
                    speed_kmh,
                    network.grid_points[start].name,
                    network.grid_points[end].name,
                )
                return points, profiles_raw, covered

    restored = points[: gap + 1] + [end for _start, end in walk[:-1]] + points[gap + 1 :]
    table.n_reconstructed += 1
    logger.info(
        "Route %d of line %s omits %d points of its Punktfolge. Restored them from the "
        "Streckennetz: %d Strecken totalling %d m between %s and %s.",
        lfd_nr,
        line,
        missing,
        len(walk),
        sum(network.segment_length[hop] for hop in walk),
        network.grid_points[points[gap]].name,
        network.grid_points[points[gap + 1]].name,
    )
    return restored, profiles_raw, covered | set(walk)


def _walks_between(network: Network, start: int, end: int, hops: int) -> Tuple[List[List[Tuple[int, int]]], bool]:
    """
    Find the walks of exactly ``hops`` segments from ``start`` to ``end``.

    A segment may not be used twice within a walk, but a grid point may be visited twice
    (a circular route comes back to where it started). The search stops once two walks are
    found — the caller only needs to know whether it is unique — and gives up after
    :data:`MAX_RECONSTRUCTION_SEARCH_STEPS` steps.

    :return: up to two walks as lists of ``(from, to)`` hops, and whether the search ran to
        completion. A single walk is the *only* walk only if it did.
    """
    found: List[List[Tuple[int, int]]] = []
    walk: List[Tuple[int, int]] = []
    used: Set[Tuple[int, int]] = set()
    steps = 0

    def step(node: int) -> None:
        nonlocal steps
        if len(walk) == hops:
            if node == end:
                found.append(list(walk))
            return
        for next_node, _length in network.adjacency.get(node, []):
            hop = (node, next_node)
            if hop in used or len(found) > 1 or steps > MAX_RECONSTRUCTION_SEARCH_STEPS:
                continue
            steps += 1
            used.add(hop)
            walk.append(hop)
            step(next_node)
            walk.pop()
            used.remove(hop)

    step(start)
    return found, steps <= MAX_RECONSTRUCTION_SEARCH_STEPS


# --------------------------------------------------------------------------------------
# Timing
# --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class _RouteContext:
    """Everything the timing pass needs to know about the route it is folding onto."""

    network: Network
    line: str
    lfd_nr: int
    points: Sequence[int]
    #: One representative point index per stop, from :func:`_folded_runs`.
    runs: Sequence[int]
    #: The hop the export collapsed, if it collapsed one.
    gap: int
    distance_m: float


def _build_profile(raw: List[Tuple[int, int]], number: int, context: _RouteContext) -> Tuple[TimeProfile, bool]:
    """
    Fold one ``Fahrzeitprofil`` onto the stops of the resolved route.

    :return: the profile, and whether its duration had to be derived from the route's
        length because the export carried none
    """
    points = context.points
    per_point = _driving_times_for_points(raw, points, context.gap, context.line, context.lfd_nr, number)

    # Arrival at point i = everything driven up to it, plus every wait before it.
    arrivals: List[int] = [0]
    for index in range(1, len(points)):
        driving, _ = per_point[index]
        _, previous_dwell = per_point[index - 1]
        arrivals.append(arrivals[-1] + previous_dwell + driving)

    stops = [StopTiming(arrival_offset_s=arrivals[index], dwell_s=per_point[index][1]) for index in context.runs]
    stops, derived = _fill_zero_duration(stops, context)
    return TimeProfile(stops=tuple(spread_arrivals(stops))), derived


def _driving_times_for_points(
    raw: Sequence[Tuple[int, int]],
    points: Sequence[int],
    gap: int,
    line: str,
    lfd_nr: int,
    number: int,
) -> List[Tuple[int, int]]:
    """
    Map a ``Fahrzeitprofil`` onto the points of the ``Punktfolge``.

    Normally the two are the same length. When a collapsed route could not be repaired the
    profile still describes every stop of the underlying journey, and indexing it by point
    position would silently truncate the trip — a 13 minute run turned into a 2 minute one.
    The surplus entries all belong to the collapsed hop, so they are summed into it.
    """
    logger = logging.getLogger(__name__)

    surplus = len(raw) - len(points)
    if surplus < 0:
        raise ValueError(
            f"Route {lfd_nr} of line {line!r}, Fahrzeitprofil {number}: the profile has "
            f"{len(raw)} points, fewer than the {len(points)} of the Punktfolge. The "
            f"export is inconsistent with itself."
        )
    if surplus == 0:
        return list(raw)

    collapsed_into = gap + 1
    logger.warning(
        "Route %d of line %s has %d points in its Punktfolge but %d in Fahrzeitprofil %d. "
        "Adding the driving times of the %d omitted stops to the leg ending at point %d.",
        lfd_nr,
        line,
        len(points),
        len(raw),
        number,
        surplus,
        collapsed_into + 1,
    )

    per_point: List[Tuple[int, int]] = []
    for index in range(len(points)):
        if index < collapsed_into:
            window = raw[index : index + 1]
        elif index == collapsed_into:
            window = raw[index : index + surplus + 1]
        else:
            window = raw[index + surplus : index + surplus + 1]
        # The waits at the omitted stops are elapsed time too, so they go into the driving
        # time of the collapsed leg. Only the last entry of the window is a point of the
        # Punktfolge, so only its wait stays a wait.
        driving = sum(entry[0] for entry in window) + sum(entry[1] for entry in window[:-1])
        per_point.append((driving, window[-1][1]))
    return per_point


def _fill_zero_duration(stops: List[StopTiming], context: _RouteContext) -> Tuple[List[StopTiming], bool]:
    """
    Give a route that takes no time at all a duration derived from its length.

    The export models a depot leg it has no driving time for as instantaneous: the
    Einsetzfahrt's ``Startzeit`` is the moment the first service trip departs, and the
    vehicle teleports out of the depot. 838 Einsetzfahrten in Berlin 2025-06 and *every*
    depot leg in the 2026 UGFPL export are like this.

    The end that is pinned by the neighbouring trip stays where it is, and the other end
    moves: an Einsetzfahrt departs earlier, an Aussetzfahrt arrives later.

    :return: the stops, and whether a duration had to be derived
    """
    logger = logging.getLogger(__name__)

    if stops[-1].arrival_offset_s != stops[0].arrival_offset_s:
        return stops, False

    # Every stop of the route is at the same offset, so all but the pinned one have to fit
    # into the duration invented here — :func:`spread_arrivals` needs a second apiece. The
    # floor is therefore the stop count, not 1: deriving a duration from the length alone
    # would let this step hand the next one a task it cannot do, and the ValueError raised
    # there would blame the export for a contradiction introduced right here. Every
    # zero-duration route in the three reference corpora is a two-stop depot leg (1,081 of
    # them in Berlin 2025-06, tightest margin 8 s), so this floor does not bind on real
    # data — it stops one absurdly short multi-stop route from failing a whole import.
    duration = max(len(stops) - 1, math.ceil(context.distance_m / (DEPOT_LEG_SPEED_KMH / 3.6)))
    logger.info(
        "Route %d of line %s takes no time at all in the export. Deriving %d s from its %.0f m at %.0f km/h.",
        context.lfd_nr,
        context.line,
        duration,
        context.distance_m,
        DEPOT_LEG_SPEED_KMH,
    )

    if context.network.grid_points[context.points[0]].typ == NetzpunktNetzpunkttyp.EPKT:
        # Pull-out: the arrival is pinned by the service trip it feeds, so leave earlier.
        return [StopTiming(stops[0].arrival_offset_s - duration, stops[0].dwell_s)] + stops[1:], True

    if context.network.grid_points[context.points[-1]].typ != NetzpunktNetzpunkttyp.APKT:
        logger.warning(
            "Route %d of line %s takes no time at all but is neither a pull-out nor a "
            "pull-in. Extending its end by %d s.",
            context.lfd_nr,
            context.line,
            duration,
        )
    # Pull-in (or an unexpected shape): the departure is pinned, so arrive later.
    return stops[:-1] + [StopTiming(stops[-1].arrival_offset_s + duration, stops[-1].dwell_s)], True


def spread_arrivals(stops: Sequence[StopTiming]) -> List[StopTiming]:
    """
    Make arrival offsets strictly increasing without moving the ends of the trip.

    Every driving and waiting time in the export is a whole number of minutes, so runs of
    stops sharing an arrival offset are the norm rather than an anomaly. Rather than
    nudging each by a second and leaving the rest of the minute empty, spread the run
    evenly across the time actually available to it — which is a better estimate of where
    the vehicle was, and keeps the trip's own departure and arrival exact.

    :param stops: the stops, with non-decreasing arrival offsets
    :return: the stops with strictly increasing arrival offsets
    :raises ValueError: if a run has more stops than there are seconds to place them in
    """
    arrivals = [stop.arrival_offset_s for stop in stops]
    dwells = [stop.dwell_s for stop in stops]
    count = len(arrivals)

    start = 0
    while start < count:
        end = start
        while end + 1 < count and arrivals[end + 1] == arrivals[start]:
            end += 1
        run = end - start + 1
        if run > 1:
            value = arrivals[start]
            if end == count - 1:
                # A run that reaches the end of the trip: the last arrival is the trip's
                # own arrival time, so hold it and spread the run backwards. Dwell times
                # are ignored here and clamped below; the arrivals are the firmer datum.
                floor = arrivals[start - 1] if start > 0 else value - EXPORT_TIME_RESOLUTION_S
                span = _span(value - floor, run, value)
                for offset in range(run):
                    arrivals[start + offset] = value - (run - 1 - offset) * span // run
            else:
                span = _span(arrivals[end + 1] - value, run, value)
                for offset in range(run):
                    arrivals[start + offset] = value + offset * span // run
        start = end + 1

    fixed: List[StopTiming] = []
    for index in range(count):
        dwell = dwells[index]
        if index + 1 < count:
            # A wait longer than the gap to the next stop would make the vehicle leave
            # after it has already arrived somewhere else. Trust the arrival times.
            dwell = min(dwell, arrivals[index + 1] - arrivals[index] - 1)
        fixed.append(StopTiming(arrival_offset_s=arrivals[index], dwell_s=max(0, dwell)))
    return fixed


def _span(room: int, run: int, value: int) -> int:
    """
    How far to spread a run of ``run`` stops that has ``room`` seconds available.

    Normally a run is spread over at most one minute, which is the export's resolution and
    therefore the width of the window the stops really fall in. When a run is too long for
    that — because an adjacent run has already eaten into the minute — the cosmetic cap is
    dropped and the whole available room is used instead.
    """
    if room < run:
        raise ValueError(
            f"{run} stops share the arrival offset {value} s but only {room} s are "
            f"available to spread them over, so they cannot be given distinct times. "
            f"Either the export's own timing is inconsistent, or the duration derived for "
            f"a route the export carries no driving time for was too short for its stops."
        )
    span = min(EXPORT_TIME_RESOLUTION_S, room)
    return span if span >= run else room
