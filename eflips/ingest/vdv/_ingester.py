import codecs
import csv
import glob
import json
import logging
import os
import pickle
import re
from collections import defaultdict, deque
from contextlib import ExitStack
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, IO, List, Optional, Tuple, Union, cast
from uuid import UUID, uuid4
from zipfile import ZipFile

import pytz
from sqlalchemy.orm import Session

from eflips.model import Line, Rotation, Route, Scenario, Station, StopTime, Trip, TripType, VehicleType, create_engine
from eflips.ingest.base import AbstractIngester
from eflips.ingest.vdv._xmldata import (
    BasisVerGueltigkeit,
    Firmenkalender,
    LidVerlauf,
    MengeFzgTyp,
    OrtHztf,
    PrimaryKey,
    RecFrt,
    RecFrtHzt,
    RecLid,
    RecOrt,
    RecSel,
    RecUmlauf,
    SelFztFeld,
    VdvBaseObject,
)


class _DebugSink:
    """Append-only JSONL sink for tracing VDV ingest decisions.

    Activated by setting the environment variable ``EFLIPS_VDV_DEBUG_LOG`` to a file path.
    When unset, ``log()`` is a no-op. Records are flushed immediately so a crash mid-ingest
    still produces a useful trace.
    """

    def __init__(self) -> None:
        path = os.environ.get("EFLIPS_VDV_DEBUG_LOG")
        self.enabled = bool(path)
        self._fp = open(path, "w", encoding="utf-8") if path else None
        if self.enabled:
            self.log("debug_session_start", pid=os.getpid())

    def log(self, event: str, **fields: object) -> None:
        if not self.enabled:
            return
        assert self._fp is not None
        rec = {"event": event, **fields}
        # Coerce common non-JSON types.
        for k, v in list(rec.items()):
            if isinstance(v, (date, datetime)):
                rec[k] = v.isoformat()
            elif isinstance(v, timedelta):
                rec[k] = v.total_seconds()
            elif isinstance(v, tuple):
                rec[k] = list(v)
        self._fp.write(json.dumps(rec, default=str) + "\n")
        self._fp.flush()

    def close(self) -> None:
        if self._fp is not None:
            try:
                self._fp.close()
            finally:
                self._fp = None
                self.enabled = False


class VDV_Data_Type(Enum):
    """
    An enum for the data types as specified in VDV 451/452. We map the different data types to two main types:

    - ``CHAR``: character data
    - ``INT`` / ``FLOAT``: numeric data
    """

    CHAR = "char"
    INT = "num"
    FLOAT = "float"


class VDV_Table_Name(Enum):
    """
    An enum for the table names as specified in VDV 451/452. Only the string names are used here — the numeric
    aliases have not been encountered in practice.
    """

    MENGE_BASIS_VERSIONEN = "MENGE_BASIS_VERSIONEN"
    BASIS_VER_GUELTIGKEIT = "BASIS_VER_GUELTIGKEIT"
    FIRMENKALENDER = "FIRMENKALENDER"
    MENGE_TAGESART = "MENGE_TAGESART"
    MENGE_ONR_TYP = "MENGE_ONR_TYP"
    MENGE_ORT_TYP = "MENGE_ORT_TYP"
    REC_HP = "REC_HP"
    REC_OM = "REC_OM"
    REC_ORT = "REC_ORT"
    FAHRZEUG = "FAHRZEUG"
    ZUL_VERKEHRSBETRIEB = "ZUL_VERKEHRSBETRIEB"
    MENGE_BEREICH = "MENGE_BEREICH"
    MENGE_FZG_TYP = "MENGE_FZG_TYP"
    REC_ANR = "REC_ANR"
    REC_ZNR = "REC_ZNR"
    REC_SEL = "REC_SEL"
    REC_SEL_ZP = "REC_SEL_ZP"
    MENGE_FGR = "MENGE_FGR"
    ORT_HZTF = "ORT_HZTF"
    SEL_FZT_FELD = "SEL_FZT_FELD"
    REC_UEB = "REC_UEB"
    UEB_FZT = "UEB_FZT"
    MENGE_FAHRTART = "MENGE_FAHRTART"
    LID_VERLAUF = "LID_VERLAUF"
    REC_LID = "REC_LID"
    REC_FRT = "REC_FRT"
    REC_FRT_HZT = "REC_FRT_HZT"
    REC_UMLAUF = "REC_UMLAUF"


@dataclass
class VdvRequiredTables:
    """
    Lists the tables required for the VDV 452 data ingestion.

    BASIS_VER_GUELTIGKEIT
        Identifies the currently valid schedule version. (No handling for the case of multiple versions; if only
        one is present, we use it.)
    FIRMENKALENDER
        Maps calendar days to day-types (Tagesarten), needed to materialise rotations across the calendar.
    REC_ORT
        Carries human-readable stop names plus optional lat/lon/altitude.
    MENGE_FZG_TYP
        Vehicle type metadata (length/width, optional battery/consumption parameters, ...).
    REC_SEL
        Distances between consecutive stops, needed for route length.
    SEL_FZT_FELD
        Travel times between two stops per Fahrzeitgruppe.
    LID_VERLAUF
        Ordered stop sequence per route variant.
    REC_FRT
        Trip-to-rotation/line/Tagesart linkage.
    REC_UMLAUF
        The rotations themselves.

    Dwell durations: at least one of
        * ``ORT_HZTF`` — default dwell per (Fahrzeitgruppe, stop), or
        * ``REC_FRT_HZT`` — per-trip overrides
    must be present. Real IVU.plan exports usually ship both: REC_FRT_HZT for overrides, ORT_HZTF for defaults.

    Überläuferfahrten (VDV 452 chapter 9.8.2) — ``REC_UEB`` and ``UEB_FZT`` are sometimes needed for depot
    ingress/egress trips, but in many datasets these are encoded as ordinary passenger trips. We do not
    currently consume them.

    Out of scope (textual descriptions, ITCS-only data, schedule-version metadata): MENGE_BASIS_VERSIONEN,
    MENGE_TAGESART, MENGE_ONR_TYP, MENGE_ORT_TYP, REC_HP, REC_OM, FAHRZEUG, ZUL_VERKEHRSBETRIEB,
    MENGE_BEREICH, REC_ANR, REC_ZNR, REC_SEL_ZP, MENGE_FGR, FLAECHEN_ZONE/FL_ZONE_ORT/MENGE_FLAECHEN_ZONE_TYP,
    SEL_FZT_FELD_ZP, MENGE_FAHRTART. The 14 e-mobility tables (VDV 452 chapters 11.6–11.14) are also deferred.
    """

    required_tables = {
        VDV_Table_Name.BASIS_VER_GUELTIGKEIT: BasisVerGueltigkeit,
        VDV_Table_Name.FIRMENKALENDER: Firmenkalender,
        VDV_Table_Name.REC_ORT: RecOrt,
        VDV_Table_Name.MENGE_FZG_TYP: MengeFzgTyp,
        VDV_Table_Name.REC_SEL: RecSel,
        VDV_Table_Name.SEL_FZT_FELD: SelFztFeld,
        VDV_Table_Name.LID_VERLAUF: LidVerlauf,
        VDV_Table_Name.REC_FRT: RecFrt,
        VDV_Table_Name.REC_UMLAUF: RecUmlauf,
        VDV_Table_Name.REC_LID: RecLid,
    }


@dataclass
class VDVTable:
    abs_file_path: str | Path
    character_set: str
    table_name: VDV_Table_Name
    column_names_and_data_types: list[
        Tuple[str, Optional[VDV_Data_Type]]
    ]  # ``None`` represents an unrecognised data type — the column is skipped on import.


# Spread duplicate arrival times symmetrically around the original timestamp within a 1-minute window.
_DUPLICATE_SPREAD_HALFWIDTH = timedelta(seconds=29)

# Minimum travel time between two distinct stops. VDV SEL_FZT_FELD legitimately contains 0-second
# segments for some Fahrzeitgruppen (notably degenerate ones), which would give two distinct stops the
# same arrival timestamp; we floor each inter-stop step to this so arrival times stay strictly increasing.
_MIN_SEGMENT_TRAVEL = timedelta(seconds=1)


def fix_identical_stop_times(stop_times: List[StopTime]) -> None:
    """
    Give a distinct timestamp to runs of consecutive stop times that share an arrival time, by spreading
    each run within ``[T-29s, T+29s]`` -- but never moving the trip's first or last stop and never
    crossing the neighbouring stops.

    Both constraints matter. The first and last stop are pinned to the trip's ``departure_time`` /
    ``arrival_time`` (the latter clamped to the next trip's start on the same rotation), so moving them
    would re-introduce rotation overlaps. And after clamping, adjacent stops can be only ~1s apart, so a
    naive ±29s spread would shove a stop past its neighbour and break the model's requirement that stop
    times be ordered like the route's stations. We therefore confine each run to the open interval
    between its surrounding stops; if there is no room, the stops keep their (equal) time, which the model
    permits (it only requires a non-decreasing order, and stop times are created in route order).

    With :func:`normalize_trip_offsets` flooring every step to >=1s, ties are rare here; this mainly
    cleans up the occasional tie left by clamp rounding. Modifies ``stop_times`` in place; dwells untouched.
    """
    n = len(stop_times)
    if n < 3:
        # Any tie in a 1- or 2-stop trip can only involve a pinned endpoint, which we never move.
        return

    hw = _DUPLICATE_SPREAD_HALFWIDTH
    one = timedelta(seconds=1)
    i = 0
    while i < n:
        j = i
        while j + 1 < n and stop_times[j + 1].arrival_time == stop_times[i].arrival_time:
            j += 1
        if j > i:  # consecutive tied run stop_times[i..j]
            anchor_low = i == 0  # run pins the first stop -> keep it, spread the rest forward
            anchor_high = j == n - 1  # run pins the last stop -> keep it, spread the rest backward
            if not (anchor_low and anchor_high):
                t = stop_times[i].arrival_time
                lo = t if anchor_low else max(stop_times[i - 1].arrival_time + one, t - hw)
                hi = t if anchor_high else min(stop_times[j + 1].arrival_time - one, t + hw)
                if hi < lo:
                    lo = hi = t
                span_s = (hi - lo).total_seconds()
                m = j - i + 1
                for k in range(m):
                    stop_times[i + k].arrival_time = lo + timedelta(seconds=round(k / (m - 1) * span_s))
        i = j + 1


def _apportion_seconds(deltas: List[int], budget: int) -> List[int]:
    """
    Scale a list of non-negative integer ``deltas`` down so they sum to ``budget`` seconds, using
    largest-remainder rounding so the result sums to exactly ``budget``. If the deltas already fit
    (``sum <= budget``) they are returned unchanged. Used to compress an over-long trip into the time
    the schedule actually budgets before the next trip departs.
    """
    total = sum(deltas)
    if total <= budget or total == 0:
        return deltas
    scale = budget / total
    ideal = [d * scale for d in deltas]
    out = [int(x) for x in ideal]
    remainder = budget - sum(out)
    for i in sorted(range(len(deltas)), key=lambda i: ideal[i] - out[i], reverse=True)[:remainder]:
        out[i] += 1
    return out


def normalize_trip_offsets(
    arrival_offsets: List[timedelta],
    dwell_durations: List[timedelta],
    frt_start: timedelta,
    next_trip_start: Optional[timedelta],
) -> Tuple[List[timedelta], List[timedelta]]:
    """
    Normalise a trip's per-stop arrival offsets and dwells so the trip is internally consistent and does
    not overlap the next trip on its rotation.

    Two adjustments, both expressed on the inter-stop steps ``arrival[i+1] - arrival[i]``:

    * **Floor** every step to :data:`_MIN_SEGMENT_TRAVEL` so two distinct stops never share a timestamp
      (VDV emits 0-second SEL_FZT for some Fahrzeitgruppen).
    * **Clamp**: if ``next_trip_start`` is given and the (floored) trip would still arrive after the next
      trip departs, scale the steps down (preserving their shape) so the trip ends exactly at
      ``next_trip_start``. This treats REC_FRT.frt_start as authoritative over the nominal SEL_FZT sum,
      which systematically over-estimates the scheduled running time at tight connections.

    Returns new ``(arrival_offsets, dwell_durations)`` in whole seconds; the first offset stays at
    ``frt_start`` and the terminus dwell stays 0.
    """
    n = len(arrival_offsets)
    if n <= 1:
        return arrival_offsets, dwell_durations

    steps = [max(_MIN_SEGMENT_TRAVEL, arrival_offsets[i + 1] - arrival_offsets[i]) for i in range(n - 1)]
    step_seconds = [int(round(s.total_seconds())) for s in steps]

    if next_trip_start is not None:
        budget = int(round((next_trip_start - frt_start).total_seconds()))
        if budget > 0:
            step_seconds = _apportion_seconds(step_seconds, budget)

    new_offsets = [frt_start]
    for s in step_seconds:
        new_offsets.append(new_offsets[-1] + timedelta(seconds=s))

    # Keep each dwell within its outgoing step so arrival[i] + dwell[i] never passes arrival[i+1];
    # the terminus dwell stays 0 (trip.arrival_time is the arrival at the last stop).
    new_dwells: List[timedelta] = []
    for i in range(n):
        if i == n - 1:
            new_dwells.append(timedelta(0))
        else:
            step = timedelta(seconds=step_seconds[i])
            new_dwells.append(max(timedelta(0), min(dwell_durations[i], step)))
    return new_offsets, new_dwells


# ── Rotation fragment chaining ──────────────────────────────────────────────────────────────────
#
# IVU.plan VDV exports cut a vehicle's working day into per-Umlauf *fragments*: REC_UMLAUF rows that
# begin or end at an ordinary stop rather than at a depot. This happens at line changes (the same bus
# continues under the next line's Umlauf number) and at the ~03:00 service-day boundary (night buses
# parked at a street terminal are fetched by the next service day's Umlauf). Downstream consumers
# require depot-to-depot rotations, so unchained fragments would be discarded there — together with
# all their revenue kilometres.
#
# We therefore re-join fragments into full vehicle workings. There is no explicit chain table to use
# (REC_UMS is empty in real exports; REC_ABLOESESTELLE covers driver reliefs only), so the join is
# reconstructed from physical continuity, with *hard* constraints only:
#
#   * the successor's first departure station is the predecessor's last arrival station,
#   * both fragments use the same vehicle type (validated on BVG data: greedy matching without this
#     constraint pairs e.g. the typ-1022 'N20/74' with the typ-1013 'N20/72' on gap alone, while the
#     1022 bus actually continues as day-line '220/10'),
#   * both fragments belong to the same home depot (BHOF_ORT_NR; 436 of 437 pairs in the validation
#     dataset agree on it — treated as satisfied when either side does not declare one),
#   * the time gap is non-negative (no overlap) and at most _MAX_CHAIN_GAP (largest genuine gap
#     observed is ~24 h: a bus positioned at a terminal in the early morning and picked up by the
#     following night service).
#
# Umlauf *names* are deliberately not compared: IVU renumbers Umläufe per line and per service day,
# so almost every genuine continuation changes its name (only 1 of 437 validated pairs kept it).
#
# Ambiguity (several same-type buses parked at the same stop) is resolved 1:1, smallest gap first,
# with fully specified tie-breakers. Swapping two interchangeable buses does not change the
# simulation outcome, so any deterministic feasible assignment is equally valid. Unmatched fragments
# (working chains that cross the export window boundary, or vehicles that continue on lines outside
# the exported subset) are left untouched.

_MAX_CHAIN_GAP = timedelta(hours=30)

# MENGE_ONR_TYP: 1 = HP (passenger stop), 2 = BHOF (depot point), 6 = BP (operational point).
# Both BHOF and BP are treated as depot-like endpoints where a rotation may open/close: the
# zero-duration deadhead handling (see the "Private / Linienwagen" synthesis below) also anchors
# synthetic depot trips at BP-typed places, so a rotation closed there must not be mistaken for an
# open fragment by the chainer.
_ONR_TYP_BHOF = 2
_ONR_TYP_BP = 6
_ONR_TYP_DEPOT = frozenset({_ONR_TYP_BHOF, _ONR_TYP_BP})


@dataclass(frozen=True)
class RotationFragment:
    """
    Endpoint summary of one materialised rotation, the unit of :func:`match_rotation_fragments`.

    ``start_station`` / ``end_station`` / ``vehicle_type`` are opaque identity tokens — any objects
    with equality semantics (ORM instances in production, plain strings in tests).
    """

    key: Tuple  # unique and sortable; used for deterministic tie-breaking
    start_station: object
    start_time: datetime
    end_station: object
    end_time: datetime
    start_at_depot: bool
    end_at_depot: bool
    vehicle_type: object
    home_depot: Optional[int]  # BHOF_ORT_NR; None = not declared


def match_rotation_fragments(
    fragments: List[RotationFragment], max_gap: timedelta = _MAX_CHAIN_GAP
) -> List[Tuple[Tuple, Tuple]]:
    """
    Match open rotation fragments into (predecessor, successor) pairs under the hard constraints
    described above. Rotations that start *and* end at a depot are closed and never take part.

    :return: List of ``(predecessor.key, successor.key)`` pairs. Each fragment appears at most once
        as predecessor and at most once as successor; transitive pairs form longer chains.
    """
    zero = timedelta(0)
    tails_by_station_and_type: Dict[Tuple[object, object], List[RotationFragment]] = defaultdict(list)
    for f in sorted((f for f in fragments if not f.end_at_depot), key=lambda f: f.key):
        tails_by_station_and_type[(f.end_station, f.vehicle_type)].append(f)

    candidates: List[Tuple[timedelta, Tuple, Tuple]] = []
    for s in sorted((f for f in fragments if not f.start_at_depot), key=lambda f: f.key):
        for p in tails_by_station_and_type.get((s.start_station, s.vehicle_type), []):
            if p.key == s.key:
                continue
            if p.home_depot is not None and s.home_depot is not None and p.home_depot != s.home_depot:
                continue
            gap = s.start_time - p.end_time
            if zero <= gap <= max_gap:
                candidates.append((gap, p.key, s.key))

    # Greedy 1:1, smallest gap first. Cycles cannot arise: every link consumes non-negative time and
    # every fragment has positive duration, so a chain can never return to its own start.
    candidates.sort()
    used_pred: set = set()
    used_succ: set = set()
    pairs: List[Tuple[Tuple, Tuple]] = []
    for gap, p_key, s_key in candidates:
        if p_key in used_pred or s_key in used_succ:
            continue
        used_pred.add(p_key)
        used_succ.add(s_key)
        pairs.append((p_key, s_key))
    return pairs


def chain_rotation_fragments(
    session: Session,
    rotations_by_key: Dict[Tuple, Rotation],
    home_depot_by_rotation_pk: Dict[Tuple, Optional[int]],
    depot_stations: set,
    debug: "_DebugSink",
    max_gap: timedelta = _MAX_CHAIN_GAP,
) -> None:
    """
    Join materialised open rotation fragments into full vehicle workings (see module comment above).

    :param rotations_by_key: Materialised rotations keyed by ``(basis_version, tagesart_nr, um_uid,
        date)``. Rotations without trips are ignored.
    :param home_depot_by_rotation_pk: ``RecUmlauf.bhof_ort_nr`` keyed by ``(basis_version,
        tagesart_nr, um_uid)``.
    :param depot_stations: Stations that represent a depot (any VDV ort with ONR_TYP_NR in
        {2 (BHOF), 6 (BP)} maps to them). Fragments are only chained at non-depot stations.
    """
    logger = logging.getLogger(__name__)

    fragments: List[RotationFragment] = []
    rotation_by_fragment_key: Dict[Tuple, Rotation] = {}
    for key in sorted(rotations_by_key.keys()):
        rotation = rotations_by_key[key]
        if len(rotation.trips) == 0:
            continue
        trips = sorted(rotation.trips, key=lambda t: (t.departure_time, t.arrival_time))
        first_trip, last_trip = trips[0], trips[-1]
        fragment_key = (key[0], key[1], key[2], key[3].isoformat())
        fragments.append(
            RotationFragment(
                key=fragment_key,
                start_station=first_trip.route.departure_station,
                start_time=first_trip.departure_time,
                end_station=last_trip.route.arrival_station,
                end_time=last_trip.arrival_time,
                start_at_depot=first_trip.route.departure_station in depot_stations,
                end_at_depot=last_trip.route.arrival_station in depot_stations,
                vehicle_type=rotation.vehicle_type,
                home_depot=home_depot_by_rotation_pk.get((key[0], key[1], key[2])),
            )
        )
        rotation_by_fragment_key[fragment_key] = rotation

    n_open = sum(int(not f.start_at_depot) + int(not f.end_at_depot) for f in fragments)
    pairs = match_rotation_fragments(fragments, max_gap)

    succ_of = dict(pairs)
    has_pred = {s for _, s in pairs}
    n_merged_fragments = 0
    n_chains = 0
    # Chain roots are the fragments with no predecessor. If the safety net below aborts a link
    # mid-chain, the aborted successor is re-queued here as a fresh root so its own downstream
    # links still get built instead of being silently dropped with the discarded predecessor.
    roots = deque(sorted(k for k in succ_of.keys() if k not in has_pred))
    processed: set = set()
    while roots:
        root_key = roots.popleft()
        if root_key in processed:
            continue
        processed.add(root_key)
        head = rotation_by_fragment_key[root_key]
        member_names = [str(head.name)]
        member_keys = [root_key]
        key = root_key
        while key in succ_of:
            successor_key = succ_of[key]
            successor = rotation_by_fragment_key[successor_key]
            # Safety net: the matcher already guarantees non-overlap, so this only fires if the
            # matcher and the trips it saw ever get out of sync. Better to leave two open fragments
            # (dropped downstream) than to build a rotation with overlapping trips.
            head_last_arrival = max(t.arrival_time for t in head.trips)
            successor_first_departure = min(t.departure_time for t in successor.trips)
            if successor_first_departure < head_last_arrival:
                logger.warning(
                    f"Not chaining rotation {successor.name!r} onto {head.name!r}: it departs "
                    f"{successor_first_departure} before the chain's last arrival {head_last_arrival}."
                )
                debug.log(
                    "fragment_chain_link_rejected",
                    chain_head=list(root_key),
                    successor=list(successor_key),
                    head_last_arrival=head_last_arrival,
                    successor_first_departure=successor_first_departure,
                )
                # The aborted successor may itself head a valid downstream chain; process it as a
                # new root so C->D in A->B->C->D survives even when B->C is rejected.
                roots.append(successor_key)
                break
            for trip in list(successor.trips):
                trip.rotation = head
            member_names.append(str(successor.name))
            member_keys.append(successor_key)
            processed.add(successor_key)
            session.delete(successor)
            n_merged_fragments += 1
            key = successor_key
        if len(member_names) > 1:
            head.name = " + ".join(member_names)
            n_chains += 1
            debug.log(
                "fragment_chain_merged",
                chain_head=list(root_key),
                members=[list(k) for k in member_keys],
                name=head.name,
            )
    session.flush()

    n_still_open = n_open - 2 * n_merged_fragments  # each executed join closes one tail and one head
    logger.info(
        f"Rotation fragment chaining: {len(fragments)} rotations, {n_open} open fragment endpoints "
        f"before, {n_merged_fragments} joins into {n_chains} chained rotations, "
        f"{n_still_open} open endpoints left (export-window edges or vehicles leaving the exported "
        f"line subset)."
    )
    debug.log(
        "fragment_chain_summary",
        n_rotations=len(fragments),
        n_open_endpoints=n_open,
        n_joins=n_merged_fragments,
        n_chained_rotations=n_chains,
        n_open_endpoints_left=n_still_open,
    )


class VdvIngester(AbstractIngester):
    def prepare(  # type: ignore[override]
        self,
        x10_zip_file: Path,
        progress_callback: None | Callable[[float], None] = None,
    ) -> Tuple[bool, UUID | Dict[str, str]]:
        """
        Prepare and validate the input data for ingestion.

        :param x10_zip_file: Path to the x10 zip archive.
        :param progress_callback: Optional progress callback (0..1).
        :return: ``(True, uuid)`` if the input is valid, otherwise ``(False, {filename: error})``.
        """
        valid_or_error = validate_zip_file(x10_zip_file)
        if valid_or_error is not True:
            assert isinstance(valid_or_error, dict)
            return False, valid_or_error

        uuid = uuid4()
        out_dir = self.path_for_uuid(uuid)
        os.makedirs(out_dir, exist_ok=False)

        with ExitStack() as stack:
            zip_file = stack.enter_context(ZipFile(x10_zip_file, "r"))

            # Drill through nested single-file zips so we land on the archive that actually contains the .x10s.
            while len(zip_file.namelist()) == 1 and zip_file.namelist()[0].lower().endswith(".zip"):
                nested_name = zip_file.namelist()[0]
                nested_handle = stack.enter_context(zip_file.open(nested_name))
                zip_file = stack.enter_context(ZipFile(nested_handle))

            members = zip_file.infolist()
            names = zip_file.namelist()

            # Reject any entry whose raw name contains path-traversal components before
            # any prefix stripping — the strip itself could otherwise launder a "../" prefix.
            for name in names:
                p = Path(name)
                if p.is_absolute() or ".." in p.parts:
                    raise ValueError(
                        f"Zip entry {name!r} contains a path traversal component. "
                        "Archive may be malicious (Zip Slip)."
                    )

            # If every entry shares a single top-level folder prefix, strip it so files land at the root of out_dir.
            if names and all("/" in n for n in names) and len({n.split("/", 1)[0] for n in names}) == 1:
                cut = len(names[0].split("/", 1)[0]) + 1
                for m in members:
                    m.filename = m.filename[cut:]

            resolved_out = out_dir.resolve()
            for m in members:
                if not m.filename:
                    continue
                target = (resolved_out / m.filename).resolve()
                if not target.is_relative_to(resolved_out):
                    raise ValueError(
                        f"Zip entry {m.filename!r} would extract outside the target directory. "
                        "Archive may be malicious (Zip Slip)."
                    )
                zip_file.extract(m, out_dir)

        try:
            all_tables = validate_input_data_vdv_451(out_dir)
        except ValueError as e:
            return False, {"validation": str(e)}

        with open(out_dir / "all_tables.pkl", "wb") as fp:
            pickle.dump(all_tables, fp)

        return True, uuid

    def ingest(self, uuid: UUID, progress_callback: None | Callable[[float], None] = None) -> None:
        logger = logging.getLogger(__name__)
        debug = _DebugSink()
        debug.log("ingest_start", uuid=str(uuid))

        temp_dir = self.path_for_uuid(uuid)
        all_tables_file = Path(temp_dir) / "all_tables.pkl"
        with open(all_tables_file, "rb") as fp:
            all_tables = pickle.load(fp)

        all_data: Dict[VDV_Table_Name, List[VdvBaseObject]] = {}
        for tbl in all_tables:
            all_data[tbl] = import_vdv452_table_records(all_tables[tbl])

        if debug.enabled:
            for tbl_name, rows in all_data.items():
                debug.log("table_loaded", table=tbl_name.value, row_count=len(rows))

        engine = create_engine(self.database_url)
        with Session(engine) as session:
            try:
                # Scenario: reuse if one already exists for this UUID, otherwise create.
                scenario = session.query(Scenario).filter(Scenario.task_id == str(uuid)).one_or_none()
                if scenario is None:
                    scenario = Scenario(name=f"Created from VDV data with UUID {uuid}")
                    session.add(scenario)

                # Vehicle types.
                vehicle_types_by_vdv_pk: Dict[PrimaryKey, VehicleType] = {}
                for vdv_vt in all_data[VDV_Table_Name.MENGE_FZG_TYP]:
                    assert isinstance(vdv_vt, MengeFzgTyp)
                    db_vt = vdv_vt.to_vehicle_type(scenario)
                    session.add(db_vt)
                    vehicle_types_by_vdv_pk[vdv_vt.primary_key] = db_vt

                # Rotations. A dummy vehicle type covers rotations without an explicit FZG_TYP_NR.
                rotations_by_vdv_pk: Dict[PrimaryKey, Rotation] = {}
                vdv_rotations_by_pk: Dict[PrimaryKey, RecUmlauf] = {}
                dummy_vehicle_type: Optional[VehicleType] = None
                for vdv_rotation in all_data[VDV_Table_Name.REC_UMLAUF]:
                    assert isinstance(vdv_rotation, RecUmlauf)
                    if vdv_rotation.fzg_typ_nr is None and dummy_vehicle_type is None:
                        dummy_vehicle_type = self.create_dummy_vehicle_type(scenario)
                        session.add(dummy_vehicle_type)
                    db_rotation = vdv_rotation.to_rotation(
                        scenario, vehicle_types_by_vdv_pk, dummy_vehicle_type=dummy_vehicle_type
                    )
                    session.add(db_rotation)
                    rotations_by_vdv_pk[vdv_rotation.primary_key] = db_rotation
                    vdv_rotations_by_pk[vdv_rotation.primary_key] = vdv_rotation
                    debug.log(
                        "rec_umlauf",
                        key=list(vdv_rotation.primary_key),
                        um_uid=vdv_rotation.um_uid,
                        tagesart_nr=vdv_rotation.tagesart_nr,
                        anf_station_pk=list(vdv_rotation.start_station_primary_key),
                        end_station_pk=list(vdv_rotation.end_station_primary_key),
                        fzg_typ_nr=vdv_rotation.fzg_typ_nr,
                    )

                # Stations. RecOrt records are more like RouteStopAssociations in eflips-model; the actual
                # Station objects are extracted via RecOrt.list_of_stations.
                rec_orts = cast(List[RecOrt], all_data[VDV_Table_Name.REC_ORT])
                stations_by_vdv_pk: Dict[PrimaryKey, Station] = RecOrt.list_of_stations(rec_orts, scenario)
                for station in set(stations_by_vdv_pk.values()):
                    session.add(station)

                # Lines. Multiple RecLid entries can share a single Line (keyed by li_kuerzel).
                rec_lids = cast(List[RecLid], all_data[VDV_Table_Name.REC_LID])
                lines_by_li_kuerzel: Dict[str, Line] = {}
                lines_by_vdv_pk: Dict[PrimaryKey, Line] = {}
                for rec_lid in rec_lids:
                    line = lines_by_li_kuerzel.get(rec_lid.li_kuerzel)
                    if line is None:
                        line = Line(name=rec_lid.li_kuerzel, scenario=scenario)
                        session.add(line)
                        lines_by_li_kuerzel[rec_lid.li_kuerzel] = line
                    lines_by_vdv_pk[rec_lid.primary_key] = line

                # Routes. Construct from RecLid + LID_VERLAUF + REC_SEL.
                lines_by_basis_version_and_li_nr: Dict[PrimaryKey, Line] = {
                    (k[0], k[1]): v for k, v in lines_by_vdv_pk.items()
                }
                rec_orts_by_position: Dict[Tuple[int, int, int], RecOrt] = {
                    (r.basis_version, r.onr_typ_nr, r.ort_nr): r for r in rec_orts
                }

                rec_sels = cast(List[RecSel], all_data[VDV_Table_Name.REC_SEL])
                rec_sel_by_endpoints: Dict[Tuple[int, int, int, int, int], RecSel] = {
                    (r.basis_version, r.onr_typ_nr, r.ort_nr, r.sel_ziel_typ, r.sel_ziel): r for r in rec_sels
                }

                lid_verlaufs = cast(List[LidVerlauf], all_data[VDV_Table_Name.LID_VERLAUF])
                lid_verlaufs_by_route: Dict[Tuple[int, int, str], List[LidVerlauf]] = defaultdict(list)
                for lv in lid_verlaufs:
                    lid_verlaufs_by_route[(lv.basis_version, lv.li_nr, lv.str_li_var)].append(lv)
                for entries in lid_verlaufs_by_route.values():
                    entries.sort(key=lambda x: x.li_lfd_nr)

                routes_by_vdv_pk: Dict[PrimaryKey, Route] = {}
                rec_sels_by_route: Dict[PrimaryKey, List[RecSel]] = {}
                for rec_lid in rec_lids:
                    route_key = (rec_lid.basis_version, rec_lid.li_nr, rec_lid.str_li_var)
                    route, route_rec_sels = rec_lid.to_route(
                        scenario=scenario,
                        lines_by_basis_version_and_li_nr=lines_by_basis_version_and_li_nr,
                        rec_orts_by_basis_version_and_onr_typ_nr_and_ort_nr=rec_orts_by_position,
                        lid_verlaufs_by_basis_version_and_li_nr_and_str_li_var=lid_verlaufs_by_route,
                        stations_by_basis_version_and_onr_typ_nr_and_ort_nr=stations_by_vdv_pk,
                        rec_sel_by_basis_version_and_start_type_and_start_nr_and_end_type_and_end_nr=rec_sel_by_endpoints,
                    )
                    if route.distance == 0:
                        dep = route.departure_station.name.strip() if route.departure_station else "?"
                        arr = route.arrival_station.name.strip() if route.arrival_station else "?"
                        if dep == arr:
                            logger.info(
                                f"Skipping zero-length route {rec_lid.primary_key} with identical "
                                f"endpoint names ({dep!r})."
                            )
                        else:
                            logger.warning(
                                f"Skipping zero-length route {rec_lid.primary_key} "
                                f"({dep!r} -> {arr!r}): all segments have zero distance and no coordinates."
                            )
                        # Detach from all parent collections so SQLAlchemy doesn't cascade-persist it.
                        if route in scenario.routes:
                            scenario.routes.remove(route)
                        if route.line is not None and route in route.line.routes:
                            route.line.routes.remove(route)
                        continue
                    session.add(route)
                    routes_by_vdv_pk[rec_lid.primary_key] = route
                    rec_sels_by_route[route_key] = route_rec_sels

                # Indices over the timing-related tables, looked up many times below.
                sel_fzt_felds = cast(List[SelFztFeld], all_data[VDV_Table_Name.SEL_FZT_FELD])
                sel_fzt_felds_by_pk: Dict[PrimaryKey, List[SelFztFeld]] = defaultdict(list)
                for sel_fzt_feld in sel_fzt_felds:
                    sel_fzt_felds_by_pk[sel_fzt_feld.primary_key].append(sel_fzt_feld)

                rec_frt_hzts = cast(List[RecFrtHzt], all_data.get(VDV_Table_Name.REC_FRT_HZT, []))
                rec_frt_hzts_by_fid: Dict[int, List[RecFrtHzt]] = defaultdict(list)
                for hzt in rec_frt_hzts:
                    rec_frt_hzts_by_fid[hzt.frt_fid].append(hzt)

                ort_hztfs = cast(List[OrtHztf], all_data.get(VDV_Table_Name.ORT_HZTF, []))
                ort_hztfs_by_key: Dict[Tuple[Tuple[int, int, int], int], timedelta] = {
                    (o.position_key, o.fgr_nr): o.hp_hzt for o in ort_hztfs
                }

                firmenkalenders = cast(List[Firmenkalender], all_data[VDV_Table_Name.FIRMENKALENDER])
                firmenkalenders_by_tagesart: Dict[int, List[Firmenkalender]] = defaultdict(list)
                for fk in firmenkalenders:
                    firmenkalenders_by_tagesart[fk.tagesart_nr].append(fk)

                rec_frts = cast(List[RecFrt], all_data[VDV_Table_Name.REC_FRT])

                if debug.enabled:
                    for rec_frt in rec_frts:
                        debug.log(
                            "rec_frt",
                            frt_fid=rec_frt.frt_fid,
                            li_nr=rec_frt.li_nr,
                            str_li_var=rec_frt.str_li_var,
                            tagesart_nr=rec_frt.tagesart_nr,
                            um_uid=rec_frt.um_uid,
                            fgr_nr=rec_frt.fgr_nr,
                            fahrtart_nr=rec_frt.fahrtart_nr,
                            frt_start_s=int(rec_frt.frt_start.total_seconds()),
                        )

                # For each trip, the FRT_START of the *next* trip on the same rotation -- the time by
                # which the bus must be ready to depart again. Used below to clamp a trip whose computed
                # arrival (from the nominal SEL_FZT sum) would overrun the next trip's start. The rotation
                # template is the same on every calendar day, so this is day-independent. We clamp against
                # the next REC_FRT by FRT_START regardless of whether it is later dropped, which is
                # conservative (it never leaves an overlap, at worst compresses a touch more than needed).
                next_frt_start_by_fid: Dict[int, timedelta] = {}
                rec_frts_by_rotation: Dict[Tuple[int, int, int], List[RecFrt]] = defaultdict(list)
                for rec_frt in rec_frts:
                    rec_frts_by_rotation[(rec_frt.basis_version, rec_frt.tagesart_nr, rec_frt.um_uid)].append(rec_frt)
                for rotation_rec_frts in rec_frts_by_rotation.values():
                    rotation_rec_frts.sort(key=lambda f: f.frt_start)
                    for earlier, later in zip(rotation_rec_frts, rotation_rec_frts[1:]):
                        if later.frt_start > earlier.frt_start:
                            next_frt_start_by_fid[earlier.frt_fid] = later.frt_start

                rotations_by_vdv_pk_and_date: Dict[Tuple[int, int, int, date], Rotation] = {}
                tz = pytz.timezone("Europe/Berlin")

                for rec_frt in rec_frts:
                    route_key = (rec_frt.basis_version, rec_frt.li_nr, rec_frt.str_li_var)
                    if route_key not in routes_by_vdv_pk:
                        logger.debug(f"Skipping trip {rec_frt.frt_fid}: route {route_key} was rejected.")
                        debug.log(
                            "trip_skipped", frt_fid=rec_frt.frt_fid, reason="route_rejected", route_key=list(route_key)
                        )
                        continue
                    route = routes_by_vdv_pk[route_key]
                    route_rec_sels = rec_sels_by_route[route_key]
                    if not route_rec_sels:
                        raise ValueError(f"Trip {rec_frt.frt_fid} references route {route_key} which has no segments.")

                    # REC_FRT_HZT carries per-trip dwell overrides. Per VDV 452 these are authoritative for
                    # the stops they name (a value of 0 means a genuine 0 s dwell), so we take them at face
                    # value. A stop without a REC_FRT_HZT row falls back to the ORT_HZTF default in
                    # resolve_dwell. (We previously discarded the whole set when every row was 0, on the
                    # theory that IVU.plan emits all-zero rows as filler -- but that substituted ORT_HZTF
                    # passenger dwells onto trips the schedule budgeted no dwell for, inflating their
                    # duration and overrunning the next trip on the rotation.)
                    trip_rec_frt_hzts = rec_frt_hzts_by_fid.get(rec_frt.frt_fid, [])

                    def resolve_dwell(position_pk: Tuple[int, int, int]) -> timedelta:
                        matches = [x for x in trip_rec_frt_hzts if x.position_key == position_pk]
                        if matches:
                            return matches[0].frt_hzt_zeit
                        return ort_hztfs_by_key.get((position_pk, rec_frt.fgr_nr), timedelta(0))

                    # Look up the travel time for every segment. Fail loudly on missing or contradictory rows;
                    # silently picking a neighbouring fgr_nr would produce plausible-looking but wrong timings.
                    segment_durations: List[timedelta] = []
                    for rec_sel in route_rec_sels:
                        sel_fzt_pk = (
                            rec_sel.basis_version,
                            rec_sel.bereich_nr,
                            rec_frt.fgr_nr,
                            rec_sel.onr_typ_nr,
                            rec_sel.ort_nr,
                            rec_sel.sel_ziel_typ,
                            rec_sel.sel_ziel,
                        )
                        matches = sel_fzt_felds_by_pk.get(sel_fzt_pk)
                        if not matches:
                            raise ValueError(
                                f"Trip {rec_frt.frt_fid}: no SEL_FZT_FELD entry for segment "
                                f"{rec_sel.primary_key} with fgr_nr={rec_frt.fgr_nr}."
                            )
                        durations = {m.sel_fzt for m in matches}
                        if len(durations) > 1:
                            raise ValueError(
                                f"Trip {rec_frt.frt_fid}: multiple SEL_FZT_FELD entries for {sel_fzt_pk} "
                                f"disagree on duration: {durations}."
                            )
                        segment_durations.append(matches[0].sel_fzt)

                    # Build the ordered list of stations from the segments. station[i] = segment[i].start
                    # for i in 0..N-1, plus segment[N-1].end as the terminal stop.
                    stations_in_order: List[Station] = [stations_by_vdv_pk[route_rec_sels[0].start_station_primary_key]]
                    for rec_sel in route_rec_sels:
                        stations_in_order.append(stations_by_vdv_pk[rec_sel.end_station_primary_key])

                    # Walk the route, accumulating arrival times and per-stop dwells. REC_FRT.frt_start is
                    # the *departure* time from the first stop, and VDV 452 forbids a dwell at the start (and
                    # end) stop of a trip, so the first stop carries no dwell: the first segment begins
                    # exactly at frt_start.
                    elapsed = rec_frt.frt_start
                    arrival_offsets: List[timedelta] = []
                    dwell_durations: List[timedelta] = []
                    for i, rec_sel in enumerate(route_rec_sels):
                        if i == 0:
                            arrival_offsets.append(elapsed)
                            dwell_durations.append(timedelta(0))
                        elapsed += segment_durations[i]
                        arrival_offsets.append(elapsed)
                        next_pk = rec_sel.end_station_primary_key
                        next_dwell = resolve_dwell(next_pk)
                        dwell_durations.append(next_dwell)
                        elapsed += next_dwell

                    # Drop the terminus dwell: trip.arrival_time should be when the bus arrives at the
                    # last stop, not when it would depart again. REC_FRT.frt_start of the *next* trip is
                    # an absolute time-of-day with no inter-trip dwell baked in, so leaving the terminus
                    # dwell in here makes consecutive trips on the same Umlauf overlap by exactly that
                    # dwell.
                    if dwell_durations:
                        elapsed -= dwell_durations[-1]
                        dwell_durations[-1] = timedelta(0)

                    # FRT_START and the computed end time are integer-seconds; the epsilon here just guards
                    # against floating-point drift inside timedelta arithmetic.
                    trip_duration = elapsed - rec_frt.frt_start

                    if debug.enabled:
                        _stop_pks = [route_rec_sels[0].start_station_primary_key] + [
                            rs.end_station_primary_key for rs in route_rec_sels
                        ]

                        def _dwell_source(pk: Tuple[int, int, int]) -> str:
                            if any(x.position_key == pk for x in trip_rec_frt_hzts):
                                return "rec_frt_hzt"
                            if (pk, rec_frt.fgr_nr) in ort_hztfs_by_key:
                                return "ort_hztf"
                            return "none"

                        _raw_hzts = rec_frt_hzts_by_fid.get(rec_frt.frt_fid, [])
                        debug.log(
                            "trip_timing",
                            frt_fid=rec_frt.frt_fid,
                            fgr_nr=rec_frt.fgr_nr,
                            fahrtart_nr=rec_frt.fahrtart_nr,
                            route_key=[rec_frt.basis_version, rec_frt.li_nr, rec_frt.str_li_var],
                            frt_start_s=int(rec_frt.frt_start.total_seconds()),
                            computed_duration_s=int(trip_duration.total_seconds()),
                            n_stops=len(_stop_pks),
                            stop_onr_typ=[pk[1] for pk in _stop_pks],
                            stop_ort_nr=[pk[2] for pk in _stop_pks],
                            seg_fzt_s=[int(d.total_seconds()) for d in segment_durations],
                            # dwell_durations[-1] has already had the terminus dwell zeroed above.
                            dwell_s=[int(d.total_seconds()) for d in dwell_durations],
                            first_dwell_s=int(dwell_durations[0].total_seconds()) if dwell_durations else 0,
                            dwell_src=[_dwell_source(pk) for pk in _stop_pks],
                            raw_hzt_count=len(_raw_hzts),
                            raw_hzt_all_zero=(
                                bool(_raw_hzts) and all(h.frt_hzt_zeit == timedelta(0) for h in _raw_hzts)
                            ),
                            hzt_used=bool(trip_rec_frt_hzts),
                        )

                    if abs(trip_duration.total_seconds()) < 1.0:
                        # Per MENGE_ONR_TYP: 1=HP (passenger stop), 2=BHOF (depot point),
                        # 6=BP (operational point). IVU.plan emits zero-duration connector trips
                        # that touch a BHOF or BP — e.g. a depot deadhead-out, or a relief handover
                        # at the same physical place under its operational identity. A zero-duration
                        # trip between only-HP stops would be genuinely suspect, so raise on those.
                        ONR_TYP_HP = 1
                        endpoint_types = {route_rec_sels[0].onr_typ_nr} | {s.sel_ziel_typ for s in route_rec_sels}
                        if not (endpoint_types - {ONR_TYP_HP}):
                            raise ValueError(
                                f"Trip {rec_frt.frt_fid} has effectively zero duration "
                                f"({trip_duration.total_seconds():.3f}s)."
                            )
                        dep_name = stations_in_order[0].name.strip()
                        arr_name = stations_in_order[-1].name.strip()
                        # IVU.plan exports use the placeholder station "Private / Linienwagen"
                        # for the depot end of dead-head connector trips. Without a synthetic
                        # depot, these zero-duration connectors get dropped here, leaving the
                        # rotation's first/last kept trip at a non-depot service station and
                        # breaking the "rotation closes at a depot" invariant downstream.
                        # We rebuild the trip as a 5-minute, 1000-m deadhead anchored to the
                        # original frt_start: outbound (depot → service) arrives at frt_start
                        # so it lines up with the first passenger departure; inbound (service →
                        # depot) departs at frt_start so it lines up with the last passenger
                        # arrival.
                        LINIENWAGEN_NAME = "Private / Linienwagen"
                        DEADHEAD_DURATION = timedelta(minutes=5)
                        DEADHEAD_DISTANCE_M = 1000.0
                        is_outbound = dep_name == LINIENWAGEN_NAME
                        is_inbound = arr_name == LINIENWAGEN_NAME
                        if is_outbound or is_inbound:
                            n_stops = len(stations_in_order)
                            base = rec_frt.frt_start - DEADHEAD_DURATION if is_outbound else rec_frt.frt_start
                            step = DEADHEAD_DURATION / (n_stops - 1) if n_stops > 1 else timedelta(0)
                            arrival_offsets = [base + step * i for i in range(n_stops)]
                            dwell_durations = [timedelta(0)] * n_stops
                            elapsed = arrival_offsets[-1]
                            if is_outbound:
                                rec_frt.frt_start = base
                            # eflips.model invariant (Route.before_insert/before_update validator):
                            # the first assoc_route_station.elapsed_distance must be 0 and the last
                            # must equal route.distance. Redistribute evenly over [0, 1000] and set
                            # route.distance to match exactly, avoiding any float-precision drift.
                            # All values written here are derived solely from DEADHEAD_DISTANCE_M
                            # (a constant) and n_ars (fixed by the route construction). Multiple
                            # trips on the same route key write identical numbers, so this is safe
                            # to repeat without a seen-set guard.
                            sorted_ars = sorted(route.assoc_route_stations, key=lambda x: x.elapsed_distance)
                            n_ars = len(sorted_ars)
                            if n_ars >= 2:
                                ars_step = DEADHEAD_DISTANCE_M / (n_ars - 1)
                                for i, ars in enumerate(sorted_ars):
                                    ars.elapsed_distance = DEADHEAD_DISTANCE_M if i == n_ars - 1 else ars_step * i
                            route.distance = DEADHEAD_DISTANCE_M
                            logger.debug(
                                f"Synthesised depot deadhead trip {rec_frt.frt_fid} "
                                f"({dep_name!r} -> {arr_name!r}) as 5 min / 1000 m."
                            )
                            debug.log(
                                "trip_synthesised_deadhead",
                                frt_fid=rec_frt.frt_fid,
                                direction="outbound" if is_outbound else "inbound",
                                dep=dep_name,
                                arr=arr_name,
                                um_uid=rec_frt.um_uid,
                            )
                            # Fall through to the materialisation loop below.
                        else:
                            logger.debug(
                                f"Skipping zero-duration non-revenue trip {rec_frt.frt_fid} "
                                f"({dep_name!r} -> {arr_name!r})."
                            )
                            debug.log(
                                "trip_skipped",
                                frt_fid=rec_frt.frt_fid,
                                reason="zero_duration_non_revenue",
                                um_uid=rec_frt.um_uid,
                                tagesart_nr=rec_frt.tagesart_nr,
                                dep=dep_name,
                                arr=arr_name,
                            )
                            continue

                    # Floor each inter-stop step to >=1s (so two distinct stops never share a timestamp)
                    # and, if the nominal SEL_FZT sum would have this trip arrive after the next trip on
                    # its rotation departs, compress it to end exactly then. Both keep the materialised
                    # rotation free of temporally overlapping trips.
                    arrival_offsets, dwell_durations = normalize_trip_offsets(
                        arrival_offsets,
                        dwell_durations,
                        rec_frt.frt_start,
                        next_frt_start_by_fid.get(rec_frt.frt_fid),
                    )
                    elapsed = arrival_offsets[-1]

                    # Materialise the trip on every calendar day that matches the trip's day-type.
                    for fk in firmenkalenders_by_tagesart.get(rec_frt.tagesart_nr, []):
                        the_date = fk.betriebstag

                        rotation_key = (rec_frt.basis_version, rec_frt.tagesart_nr, rec_frt.um_uid, the_date)
                        rotation = rotations_by_vdv_pk_and_date.get(rotation_key)
                        if rotation is None:
                            orig_rotation = rotations_by_vdv_pk[
                                (rec_frt.basis_version, rec_frt.tagesart_nr, rec_frt.um_uid)
                            ]
                            rotation = Rotation(
                                scenario=scenario,
                                name=orig_rotation.name,
                                vehicle_type=orig_rotation.vehicle_type,
                                trips=[],
                                allow_opportunity_charging=orig_rotation.allow_opportunity_charging,
                            )
                            rotations_by_vdv_pk_and_date[rotation_key] = rotation
                            session.add(rotation)

                        local_midnight = tz.localize(datetime.combine(the_date, time(0, 0)))
                        trip = Trip(
                            scenario=scenario,
                            route=route,
                            departure_time=local_midnight + rec_frt.frt_start,
                            arrival_time=local_midnight + elapsed,
                            trip_type=TripType.PASSENGER if rec_frt.fahrtart_nr == 1 else TripType.EMPTY,
                        )

                        stop_times: List[StopTime] = []
                        for i, station in enumerate(stations_in_order):
                            stop_times.append(
                                StopTime(
                                    scenario=scenario,
                                    trip=trip,
                                    station=station,
                                    arrival_time=local_midnight + arrival_offsets[i],
                                    dwell_duration=dwell_durations[i],
                                )
                            )
                        fix_identical_stop_times(stop_times)
                        # Model checks: departure_time == first_stop.arrival_time and
                        # arrival_time == last_stop.arrival_time + last_stop.dwell_duration.
                        # Spreading may shift either end, so sync both after the spread.
                        trip.departure_time = stop_times[0].arrival_time
                        trip.arrival_time = (
                            stop_times[-1].arrival_time + stop_times[-1].dwell_duration
                            if stop_times[-1].dwell_duration is not None
                            else stop_times[-1].arrival_time
                        )

                        trip.rotation = rotation
                        session.add(trip)
                        debug.log(
                            "trip_materialized",
                            rotation_key=[rotation_key[0], rotation_key[1], rotation_key[2], the_date.isoformat()],
                            frt_fid=rec_frt.frt_fid,
                            route_key=list(route_key),
                            dep_station_name=stations_in_order[0].name.strip(),
                            arr_station_name=stations_in_order[-1].name.strip(),
                            dep_station_pk_in_route=list(route_rec_sels[0].start_station_primary_key),
                            arr_station_pk_in_route=list(route_rec_sels[-1].end_station_primary_key),
                            departure_time=trip.departure_time.isoformat(),
                            arrival_time=trip.arrival_time.isoformat(),
                            fahrtart_nr=rec_frt.fahrtart_nr,
                        )

                # Drop rotations that ended up with no trips. Snapshot the collection first — deleting from
                # the live collection while iterating can confuse the SQLAlchemy unit of work.
                session.flush()

                # ── Per-rotation post-mortem: compare what the VDV input claimed (anf_ort/end_ort
                # on REC_UMLAUF) with what we actually assembled from the REC_FRT rows. This is the
                # single most useful artefact for diagnosing "first dep ≠ last arr" failures
                # downstream.
                if debug.enabled:
                    for rotation_key, rotation in rotations_by_vdv_pk_and_date.items():
                        bv, tag, um, the_date = rotation_key
                        orig = vdv_rotations_by_pk.get((bv, tag, um))
                        trips_sorted = sorted(rotation.trips, key=lambda t: t.departure_time)
                        trip_info = []
                        for t in trips_sorted:
                            dep_stop = t.stop_times[0] if t.stop_times else None
                            arr_stop = t.stop_times[-1] if t.stop_times else None
                            trip_info.append(
                                {
                                    "dep_station": dep_stop.station.name.strip() if dep_stop else None,
                                    "arr_station": arr_stop.station.name.strip() if arr_stop else None,
                                    "dep_time": t.departure_time.isoformat(),
                                    "arr_time": t.arrival_time.isoformat(),
                                    "route_id": id(t.route),
                                }
                            )
                        first_dep = trip_info[0]["dep_station"] if trip_info else None
                        actual_last_arr = trip_info[-1]["arr_station"] if trip_info else None
                        # Find the station the VDV input claims as start/end (resolved via stations_by_vdv_pk).
                        vdv_anf = (
                            stations_by_vdv_pk[orig.start_station_primary_key].name.strip()
                            if orig and orig.start_station_primary_key in stations_by_vdv_pk
                            else None
                        )
                        vdv_end = (
                            stations_by_vdv_pk[orig.end_station_primary_key].name.strip()
                            if orig and orig.end_station_primary_key in stations_by_vdv_pk
                            else None
                        )
                        debug.log(
                            "rotation_built",
                            rotation_key=[bv, tag, um, the_date.isoformat()],
                            name=str(orig.um_uid) if orig else None,
                            vdv_anf_station_pk=list(orig.start_station_primary_key) if orig else None,
                            vdv_end_station_pk=list(orig.end_station_primary_key) if orig else None,
                            vdv_anf_station_name=vdv_anf,
                            vdv_end_station_name=vdv_end,
                            actual_first_dep_station=first_dep,
                            actual_last_arr_station=actual_last_arr,
                            anf_matches=(vdv_anf == first_dep) if vdv_anf and first_dep else None,
                            end_matches=(vdv_end == actual_last_arr) if vdv_end and actual_last_arr else None,
                            trip_count=len(trip_info),
                            trips=trip_info,
                        )

                for rotation in list(scenario.rotations):
                    if len(rotation.trips) == 0:
                        session.delete(rotation)

                # Re-join rotation fragments (vehicle workings cut at line changes / the service-day
                # boundary) into full depot-to-depot rotations. See the module comment on
                # match_rotation_fragments for the evidence behind the matching rules.
                if not os.environ.get("EFLIPS_VDV_DISABLE_FRAGMENT_CHAINING"):
                    depot_stations = {
                        station for pk, station in stations_by_vdv_pk.items() if pk[1] in _ONR_TYP_DEPOT
                    }
                    home_depot_by_rotation_pk = {
                        pk: vdv_rotation.bhof_ort_nr for pk, vdv_rotation in vdv_rotations_by_pk.items()
                    }
                    chain_rotation_fragments(
                        session,
                        rotations_by_vdv_pk_and_date,
                        home_depot_by_rotation_pk,
                        depot_stations,
                        debug,
                    )

                session.commit()
                debug.log("ingest_complete")
            except Exception as exc:
                debug.log("ingest_failed", error=repr(exc))
                session.rollback()
                raise
            finally:
                debug.close()

    @classmethod
    def create_dummy_vehicle_type(cls, scenario: Scenario) -> VehicleType:
        dummy_vehicle_type = VehicleType(
            scenario=scenario,
            name="Dummy Vehicle Type",
            opportunity_charging_capable=False,
            battery_capacity=10000,
            charging_curve=[[0, 1000], [1, 1000]],
        )
        return dummy_vehicle_type

    @classmethod
    def prepare_param_names(cls) -> Dict[str, str | Dict[Enum, str]]:
        """
        Short human-readable names for the parameters of :meth:`prepare`.
        """
        return {
            "x10_zip_file": "VDV data archive",
        }

    @classmethod
    def prepare_param_description(cls) -> Dict[str, str | Dict[Enum, str]]:
        """
        Longer descriptions for the parameters of :meth:`prepare`.
        """
        return {
            "x10_zip_file": (
                "A zip file containing the VDV data. The zip file must contain all necessary tables for the "
                "VDV 451/452 format. These should be in the form of .x10 files in the top level of the zip file."
            ),
        }


MAXIMUM_FILE_SIZE_B = 4 * 1024 * 1024 * 1024  # 4 GiB; anything above is probably junk.


def validate_zip_file(zipfile: Union[Path, IO[bytes]]) -> bool | Dict[str, str]:
    """
    Validate the zip file (or a file-like object wrapping one).

    Returns ``True`` if every entry is acceptable, or a ``{filename: error}`` dict listing the problems.
    Directory entries are skipped (the archive may legitimately use folders; ``prepare`` handles them).
    Nested zips are recursed into and their errors merged.
    """
    error_messages: Dict[str, str] = {}
    try:
        with ZipFile(zipfile) as zf:
            for entry in zf.infolist():
                if entry.is_dir():
                    continue
                name = entry.filename
                lower = name.lower()
                if lower.endswith(".x10"):
                    if entry.file_size == 0:
                        error_messages[name] = "Empty file"
                    elif entry.file_size > MAXIMUM_FILE_SIZE_B:
                        size_mib = MAXIMUM_FILE_SIZE_B / (1024 * 1024)
                        error_messages[name] = f"File size exceeds {size_mib} MiB"
                elif lower.endswith(".txt"):
                    # VDV metadata files; not consumed but not invalid either.
                    continue
                elif lower.endswith(".zip"):
                    with zf.open(entry) as nested:
                        nested_result = validate_zip_file(nested)
                    if isinstance(nested_result, dict):
                        error_messages.update(nested_result)
                else:
                    error_messages[name] = "Invalid file extension"
    except Exception as e:
        error_messages["zipfile"] = str(e)

    return error_messages or True


def validate_input_data_vdv_451(
    abs_path_to_folder_with_vdv_files: str | Path,
) -> dict[VDV_Table_Name, VDVTable]:
    """
    Check that the given directory contains every required VDV 451/452 ``.x10`` file. Raises ``ValueError``
    if not.

    :param abs_path_to_folder_with_vdv_files: Absolute path to the directory containing the VDV 451 files.
    :return: A dict of table-name → :class:`VDVTable`.
    """
    logger = logging.getLogger(__name__)

    # macOS glob is case-sensitive (Windows is not), so search both extensions and de-duplicate.
    search_pattern_lowercase = os.path.join(abs_path_to_folder_with_vdv_files, "*.x10")
    search_pattern_uppercase = os.path.join(abs_path_to_folder_with_vdv_files, "*.X10")
    x10_files_unique = list(set(glob.glob(search_pattern_lowercase) + glob.glob(search_pattern_uppercase)))

    # VDV 451 has two naming schemes (chapter 3.1/3.2). The table name is also stored inside the file, so we
    # identify each file by its header rather than its filename.
    all_tables: dict[VDV_Table_Name, VDVTable] = {}
    for abs_file_path in x10_files_unique:
        try:
            eingangsdatentable = check_vdv451_file_header(abs_file_path)
            if eingangsdatentable.table_name in all_tables:
                raise ValueError(
                    f"The table {eingangsdatentable.table_name.value} is present in multiple files. Aborting."
                )
            all_tables[eingangsdatentable.table_name] = eingangsdatentable
        except (ValueError, UnicodeDecodeError) as e:
            logger.debug(f"While processing {abs_file_path} the following exception occurred:", exc_info=e)
            continue

    missing_tables = set(VdvRequiredTables.required_tables.keys()) - set(all_tables.keys())
    if missing_tables:
        missing_str = ", ".join(t.value for t in missing_tables)
        raise ValueError(
            f"Not all necessary tables are present in the directory (or present, but empty). "
            f"Missing tables are: {missing_str}. Aborting."
        )

    # At least one of REC_FRT_HZT or ORT_HZTF must be present. Real exports usually ship both.
    if VDV_Table_Name.REC_FRT_HZT not in all_tables and VDV_Table_Name.ORT_HZTF not in all_tables:
        raise ValueError("Neither REC_FRT_HZT nor ORT_HZTF present in the directory. Aborting.")

    logger.info("All necessary tables are present in the directory.")
    return all_tables


def check_vdv451_file_header(abs_file_path: str) -> VDVTable:
    """
    Parse a VDV 451 (.x10) file header and return its declared table name, character set, and column types.
    """

    # Per VDV 451 § 4.1 the header is always ASCII, so the "chs" line that declares the body encoding can
    # itself always be read as ASCII. Two passes: first ASCII-only to extract the charset, then a second
    # pass in the resolved encoding for the rest of the header.
    logger = logging.getLogger(__name__)

    # codecs.lookup() canonicalises the various spellings of these names that exist in the wild
    # ("ISO-8859-1" vs "ISO8859-1", "WINDOWS-1252" vs "cp1252", "UTF-8" vs "UTF8").
    allowed_canonical_charsets = {"ascii", "iso8859-1", "cp1252", "utf-8"}

    # Read in binary and decode each line as ASCII. Text-mode I/O would fill an ~8 KiB buffer and try to
    # decode the whole block at once, which fails when later record lines contain non-ASCII bytes — even
    # though the pre-'chs' header is itself pure ASCII.
    character_set: Optional[str] = None
    with open(abs_file_path, "rb") as f:
        for raw_line in f:
            try:
                line = raw_line.decode("ascii")
            except UnicodeDecodeError as e:
                e.add_note(
                    f"The header of the file {abs_file_path} contains non-ASCII characters before the 'chs' "
                    f"line. This is not allowed according to the VDV 451 specification."
                )
                raise

            parts = line.strip().split(";")
            if parts[0] != "chs":
                continue

            # The chs value is quoted (e.g. ``chs; "ISO8859-1"``); use csv to strip the quotes.
            parts = next(csv.reader([line], delimiter=";", skipinitialspace=True))
            raw_charset = parts[1].strip().upper()

            try:
                character_set = codecs.lookup(raw_charset).name
            except LookupError:
                raise ValueError(f"The file {abs_file_path} declares an unknown character set: {raw_charset!r}.")

            if character_set not in allowed_canonical_charsets:
                raise ValueError(
                    f"The file {abs_file_path} uses an encoding that is not allowed by VDV 451: "
                    f"{raw_charset!r} (resolved to {character_set!r}). "
                    "Allowed: ASCII, ISO8859-1, WINDOWS-1252, UTF-8."
                )
            break

    if character_set is None:
        msg = f"The file {abs_file_path} does not contain a character set in the header."
        logger.info(msg)
        raise ValueError(msg)

    table_name_str: Optional[str] = None
    datatypes: Optional[List[Optional[VDV_Data_Type]]] = None
    column_names: Optional[List[str]] = None

    with open(abs_file_path, "r", encoding=character_set) as f:
        for line in f:
            parts = line.strip().split(";")
            command = parts[0]

            if command == "chs":
                continue
            if command == "tbl":
                table_name_str = parts[1].upper().strip()
            elif command == "frm":
                formats = parts[1:]
                try:
                    datatypes = parse_datatypes(formats)
                except ValueError as e:
                    e.add_note(
                        f"The file {abs_file_path} contains invalid column data types. Please check the "
                        "formatting of the data types in the file."
                    )
                    raise
            elif command == "atr":
                column_names = [x.upper().strip() for x in parts[1:]]
            elif command == "rec":
                if table_name_str is not None:
                    break
            elif command == "eof":
                # File has no records, but its header may still be complete. Real IVU.plan exports
                # routinely ship empty REC_FRT_HZT/ORT_HZTF tables (header + ``end; 0``); the
                # downstream ingest code already handles the empty-record case, so treat an empty
                # table as a valid (just empty) table rather than as a parsing failure.
                break

    if table_name_str is None:
        msg = f"The file {abs_file_path} does not contain a table name in the header."
        logger.info(msg)
        raise ValueError(msg)
    if datatypes is None:
        msg = f"The file {abs_file_path} does not contain the data types of the columns in the header."
        logger.info(msg)
        raise ValueError(msg)
    if column_names is None:
        raise ValueError(
            f"The file {abs_file_path} does not contain the column names in the header. "
            "Please check the file and try again."
        )
    if table_name_str not in {x.value for x in VDV_Table_Name}:
        raise ValueError(f"The file {abs_file_path} contains an unknown table name: {table_name_str}. Skipping it.")
    if len(column_names) != len(datatypes):
        raise ValueError(
            f"The file {abs_file_path} contains an unequal number of column names and column data types "
            f"in the header: {len(column_names)} column names, but {len(datatypes)} column data types."
        )

    return VDVTable(
        abs_file_path=abs_file_path,
        character_set=character_set,
        table_name=VDV_Table_Name[table_name_str],
        column_names_and_data_types=list(zip(column_names, datatypes)),
    )


# Permissible VDV 451 column-type literals are ``char[n]`` (string) and ``num[n.m]`` (numeric). ``num[n.0]`` is
# integer, anything else is float. The dots are escaped so e.g. ``num[10x0]`` doesn't accidentally match int.
_RE_CHAR = re.compile(r"char\[[0-9]+\]$")
_RE_INT = re.compile(r"num\[[0-9]+\.0\]$")
_RE_FLOAT = re.compile(r"num\[[0-9]+\.[0-9]+\]$")


def parse_datatypes(datatype_str: list[str]) -> list[Optional[VDV_Data_Type]]:
    """
    Convert a list of VDV 451 datatype strings (e.g. ``['num[9.0]', 'char[40]', 'num[2.4]']``) into
    :class:`VDV_Data_Type` members. Unknown formats are recorded as ``None`` and the corresponding column
    will be skipped on import.
    """
    logger = logging.getLogger(__name__)

    dtypes: list[Optional[VDV_Data_Type]] = []
    for raw in datatype_str:
        part = raw.lstrip()
        if _RE_CHAR.match(part):
            dtypes.append(VDV_Data_Type.CHAR)
        elif _RE_INT.match(part):
            dtypes.append(VDV_Data_Type.INT)
        elif _RE_FLOAT.match(part):
            dtypes.append(VDV_Data_Type.FLOAT)
        else:
            dtypes.append(None)
            logger.warning(
                f"Invalid datatype formatting in VDV 451 file: {part!r} does not match 'char[n]' or "
                "'num[n.n]'. Column will not be imported."
            )

    return dtypes


def import_vdv452_table_records(eingangsdaten_tabelle: VDVTable) -> list[VdvBaseObject]:
    """
    Read the records from a VDV 451 table file and return them as a list of typed VDV objects.
    """
    logger = logging.getLogger(__name__)

    with open(eingangsdaten_tabelle.abs_file_path, "r", encoding=eingangsdaten_tabelle.character_set) as f:
        reader = csv.reader(f, delimiter=";", skipinitialspace=True)
        dict_list: List[Dict[str, str | int | float | None]] = []
        for row in reader:
            if len(row) == 0 or row[0].strip() != "rec":
                logger.debug(f"Skipping line: {row}")
                continue

            row_data = row[1:]
            if len(row_data) != len(eingangsdaten_tabelle.column_names_and_data_types):
                raise ValueError(
                    f"The file {eingangsdaten_tabelle.abs_file_path} contains a record with more or fewer "
                    f"columns than the header specifies. The record contains {row_data}, aborting."
                )

            e_data: Dict[str, str | int | float | None] = {}
            for i_col, (column_name, column_data_type) in enumerate(eingangsdaten_tabelle.column_names_and_data_types):
                if column_data_type is None:
                    # Header listed this column with an unrecognised datatype. Skip consistently so the dict
                    # has a stable set of keys across all rows.
                    continue

                value = row_data[i_col]
                if value.strip() == "":
                    e_data[column_name] = None
                    continue

                if column_data_type is VDV_Data_Type.INT:
                    try:
                        e_data[column_name] = int(value)
                    except ValueError as e:
                        e.add_note(
                            f"The file {eingangsdaten_tabelle.abs_file_path} contains a non-numeric value "
                            "in a column declared as int. Aborting."
                        )
                        raise
                elif column_data_type is VDV_Data_Type.FLOAT:
                    try:
                        e_data[column_name] = float(value)
                    except ValueError as e:
                        e.add_note(
                            f"The file {eingangsdaten_tabelle.abs_file_path} contains a non-numeric value "
                            "in a column declared as float. Aborting."
                        )
                        raise
                elif column_data_type is VDV_Data_Type.CHAR:
                    e_data[column_name] = value
                else:
                    raise ValueError(
                        f"The file {eingangsdaten_tabelle.abs_file_path} contains a column with an invalid "
                        f"data type: {column_data_type}. Aborting."
                    )

            dict_list.append(e_data)

        table_name = eingangsdaten_tabelle.table_name
        if table_name is VDV_Table_Name.BASIS_VER_GUELTIGKEIT:
            # We only support a single distinct BasisVerGueltigkeit entry. Duplicates are tolerated.
            objects = [BasisVerGueltigkeit.from_dict(d) for d in dict_list]
            if len(set(objects)) != 1:
                raise ValueError(
                    f"The table {table_name} contains multiple distinct entries. Only one entry is "
                    "allowed. Aborting."
                )
            return cast(List[VdvBaseObject], objects)
        if table_name is VDV_Table_Name.FIRMENKALENDER:
            return [Firmenkalender.from_dict(d) for d in dict_list]
        if table_name is VDV_Table_Name.REC_ORT:
            return [RecOrt.from_dict(d) for d in dict_list]
        if table_name is VDV_Table_Name.MENGE_FZG_TYP:
            return [MengeFzgTyp.from_dict(d) for d in dict_list]
        if table_name is VDV_Table_Name.REC_SEL:
            return [RecSel.from_dict(d) for d in dict_list]
        if table_name is VDV_Table_Name.SEL_FZT_FELD:
            return [SelFztFeld.from_dict(d) for d in dict_list]
        if table_name is VDV_Table_Name.LID_VERLAUF:
            return [LidVerlauf.from_dict(d) for d in dict_list]
        if table_name is VDV_Table_Name.REC_FRT:
            return [RecFrt.from_dict(d) for d in dict_list]
        if table_name is VDV_Table_Name.REC_UMLAUF:
            return [RecUmlauf.from_dict(d) for d in dict_list]
        if table_name is VDV_Table_Name.REC_LID:
            return [RecLid.from_dict(d) for d in dict_list]
        if table_name is VDV_Table_Name.REC_FRT_HZT:
            return [RecFrtHzt.from_dict(d) for d in dict_list]
        if table_name is VDV_Table_Name.ORT_HZTF:
            return [OrtHztf.from_dict(d) for d in dict_list]
        return []
