"""
Reassembling physical vehicle workings from their per-line slices.

The export slices the network by ``(Linie, Stichtag)``. A vehicle working
(``Fahrzeugumlauf``) that serves several lines therefore appears in several files, and each
copy carries only that file's line's trips — every other ``Umlaufteilgruppe`` comes through
with its ``Fahrtreihenfolge`` omitted. Reassembling them is a join, not a heuristic: the
element repeats its *full* ``Umlauf`` group in every copy, so the ordered tuple of
``(UmlaufID, Kalenderdatum)`` pairs identifies the working exactly. In the Berlin 2025-06
corpus 13,968 ``Fahrzeugumlauf`` elements collapse into 10,365 workings with no member
belonging to two groups.

That join also gives an **exact completeness test**, which is the part that matters
downstream. A working is truncated iff some ``Umlaufteilgruppe`` has no
``Fahrtreihenfolge`` in *any* input file, and the cause is readable from the data:

``WINDOW_EDGE``
    The teilgruppe's ``Kalenderdatum`` lies outside the exported ``Stichtag`` range — a
    night bus whose evening half belongs to the day before the export starts. 370 of the
    443 truncated workings in Berlin 2025-06.
``LINE_NOT_SUPPLIED``
    No input file covers the teilgruppe's ``(Linie, Stichtag)``. The user left a file out
    of the zip. 75 of the 443, naming four lines (two of the workings hit both causes).

All 9,922 structurally complete workings in that corpus start at an ``EPkt`` and end at an
``APkt``, so this test subsumes the depot-name heuristic it replaces — and unlike that
heuristic it does not also keep 8 workings that are in fact truncated.
"""
import logging
from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import Dict, List, Tuple

from eflips.ingest.bvgxml._read import RawCorpus, parse_kalenderdatum

#: The identity of one physical vehicle working: the ordered ``(UmlaufID, Kalenderdatum)``
#: pairs of the Umläufe grouped into one ``Fahrzeugumlauf`` element.
WorkingKey = Tuple[Tuple[int, date], ...]

#: The identity of one ``Umlaufteilgruppe`` within a working: ``(UmlaufID, LfdNr)``.
SegmentKey = Tuple[int, int]


class TruncationCause(Enum):
    """Why a part of a vehicle working has no trips anywhere in the input."""

    #: Its ``Kalenderdatum`` is outside the exported ``Stichtag`` range.
    WINDOW_EDGE = "window_edge"

    #: No input file covers its ``(Linie, Stichtag)`` slice.
    LINE_NOT_SUPPLIED = "line_not_supplied"

    #: Its slice *is* covered, yet no file carries its trips. Not observed in any
    #: reference corpus; reported rather than silently folded into the others.
    UNEXPLAINED = "unexplained"


@dataclass(frozen=True)
class WorkingSegment:
    """One ``Umlaufteilgruppe`` that a file supplied trips for."""

    key: SegmentKey
    day: date
    line: str
    beginn_s: int
    fahrt_ids: Tuple[int, ...]


@dataclass(frozen=True)
class MissingSegment:
    """One ``Umlaufteilgruppe`` that no file supplied trips for."""

    key: SegmentKey
    day: date
    line: str
    cause: TruncationCause


@dataclass
class Working:
    """One physical vehicle working, reassembled from every file that mentions it."""

    key: WorkingKey
    name: str
    vehicle_type: str
    depot: int
    segments: List[WorkingSegment] = field(default_factory=list)
    missing: List[MissingSegment] = field(default_factory=list)

    @property
    def is_complete(self) -> bool:
        return not self.missing

    def fahrt_ids(self) -> List[int]:
        """
        Every trip of the working, in the order the vehicle runs them.

        The authoritative order is ``(Kalenderdatum, Umlaufteilgruppe/Beginn,
        Fahrt/LfdNr)``. Sorting by ``Startzeit`` instead would tie a pull-out against the
        service trip it feeds, because the export gives both the same departure second
        whenever it has no driving time for the pull-out.
        """
        ordered: List[int] = []
        for segment in sorted(self.segments, key=lambda s: (s.day, s.beginn_s, s.key)):
            ordered.extend(segment.fahrt_ids)
        return ordered


@dataclass
class WorkingTable:
    """Every vehicle working of the corpus."""

    workings: Dict[WorkingKey, Working]

    @property
    def complete(self) -> List[Working]:
        return [w for w in self.workings.values() if w.is_complete]

    @property
    def truncated(self) -> List[Working]:
        return [w for w in self.workings.values() if not w.is_complete]

    def truncation_summary(self) -> Dict[TruncationCause, int]:
        counts: Dict[TruncationCause, int] = Counter()
        for working in self.truncated:
            for cause in {segment.cause for segment in working.missing}:
                counts[cause] += 1
        return dict(counts)

    def missing_lines(self) -> List[str]:
        return sorted(
            {
                segment.line
                for working in self.truncated
                for segment in working.missing
                if segment.cause is TruncationCause.LINE_NOT_SUPPLIED
            }
        )


def build_workings(corpus: RawCorpus) -> WorkingTable:
    """
    Group every ``Fahrzeugumlauf`` element of the corpus into physical vehicle workings.

    :param corpus: the merged input
    :return: the working table
    :raises ValueError: if two files contradict each other about a working
    """
    logger = logging.getLogger(__name__)

    workings: Dict[WorkingKey, Working] = {}
    key_by_member: Dict[Tuple[int, date], WorkingKey] = {}
    # Every Umlaufteilgruppe the corpus mentions, whether or not it has trips.
    declared: Dict[WorkingKey, Dict[SegmentKey, Tuple[date, str]]] = {}
    supplied: Dict[WorkingKey, Dict[SegmentKey, WorkingSegment]] = {}

    for _file_index, element in corpus.fahrzeugumlaeufe:
        members = tuple(
            (umlauf.umlauf_id, parse_kalenderdatum(umlauf.kalenderdatum)) for umlauf in element.umlaeufe.umlauf
        )
        name = " ".join(umlauf.umlaufbezeichnung for umlauf in element.umlaeufe.umlauf)

        working = workings.get(members)
        if working is None:
            _check_grouping(members, key_by_member)
            working = Working(key=members, name=name, vehicle_type=element.fahrzeugtyp, depot=element.betriebshof)
            workings[members] = working
            declared[members] = {}
            supplied[members] = {}
            for member in members:
                key_by_member[member] = members
        else:
            _check_agreement(working, name, element.fahrzeugtyp, element.betriebshof)

        for umlauf in element.umlaeufe.umlauf:
            day = parse_kalenderdatum(umlauf.kalenderdatum)
            for teilgruppe in umlauf.umlaufteilgruppen.umlaufteilgruppe:
                segment_key = (umlauf.umlauf_id, teilgruppe.lfd_nr)
                declared[members][segment_key] = (day, teilgruppe.linie)
                if teilgruppe.fahrtreihenfolge is None:
                    continue
                fahrt_ids = tuple(fahrt.fahrt_id for fahrt in teilgruppe.fahrtreihenfolge.fahrt)
                previous = supplied[members].get(segment_key)
                if previous is not None and previous.fahrt_ids != fahrt_ids:
                    raise ValueError(
                        f"Vehicle working {name!r} ({members}): Umlaufteilgruppe "
                        f"{segment_key} has different trips in two input files. The "
                        f"per-line slices of a working must agree."
                    )
                supplied[members][segment_key] = WorkingSegment(
                    key=segment_key,
                    day=day,
                    line=teilgruppe.linie,
                    # Beginn is only present on a teilgruppe that carries trips, which is
                    # exactly the case we are in here.
                    beginn_s=teilgruppe.beginn if teilgruppe.beginn is not None else 0,
                    fahrt_ids=fahrt_ids,
                )

    stichtage = corpus.stichtage
    slices = corpus.slices
    for key, working in workings.items():
        working.segments = list(supplied[key].values())
        for segment_key, (day, line) in declared[key].items():
            if segment_key in supplied[key]:
                continue
            if day not in stichtage:
                cause = TruncationCause.WINDOW_EDGE
            elif (line, day) not in slices:
                cause = TruncationCause.LINE_NOT_SUPPLIED
            else:
                cause = TruncationCause.UNEXPLAINED
            working.missing.append(MissingSegment(key=segment_key, day=day, line=line, cause=cause))

    table = WorkingTable(workings=workings)
    logger.info(
        "Reassembled %d Fahrzeugumlauf elements into %d vehicle workings; %d are complete.",
        len(corpus.fahrzeugumlaeufe),
        len(workings),
        len(table.complete),
    )
    return table


def _check_grouping(members: WorkingKey, key_by_member: Dict[Tuple[int, date], WorkingKey]) -> None:
    """Refuse to build a second working around an Umlauf that already belongs to one."""
    for member in members:
        conflicting = key_by_member.get(member)
        if conflicting is not None:
            raise ValueError(
                f"Inconsistent Fahrzeugumlauf grouping between input files: Umlauf "
                f"(UmlaufID={member[0]}, Kalenderdatum={member[1]}) appears both in the "
                f"group {conflicting} and in the group {members}. The input files "
                f"contradict each other about which Umläufe form one vehicle working."
            )


def _check_agreement(working: Working, name: str, vehicle_type: str, depot: int) -> None:
    """All copies of a working must describe the same vehicle."""
    if working.name != name:
        raise ValueError(
            f"Vehicle working {working.key} is named {working.name!r} in one input file "
            f"but {name!r} in another. The input files contradict each other."
        )
    if working.vehicle_type != vehicle_type:
        raise ValueError(
            f"Vehicle working {working.name!r} has vehicle type {working.vehicle_type!r} "
            f"in one input file but {vehicle_type!r} in another. The input files "
            f"contradict each other."
        )
    if working.depot != depot:
        raise ValueError(
            f"Vehicle working {working.name!r} has Betriebshof {working.depot} in one "
            f"input file but {depot} in another. The input files contradict each other."
        )


def describe_truncation(working: Working) -> str:
    """A one-line explanation of why a working was dropped, for the ingest log."""
    causes: Dict[TruncationCause, List[MissingSegment]] = {}
    for segment in working.missing:
        causes.setdefault(segment.cause, []).append(segment)
    parts: List[str] = []
    if TruncationCause.WINDOW_EDGE in causes:
        days = sorted({segment.day.isoformat() for segment in causes[TruncationCause.WINDOW_EDGE]})
        parts.append(f"{len(causes[TruncationCause.WINDOW_EDGE])} part(s) run on {', '.join(days)}, outside the export")
    if TruncationCause.LINE_NOT_SUPPLIED in causes:
        slices = sorted(
            {f"{segment.line} on {segment.day.isoformat()}" for segment in causes[TruncationCause.LINE_NOT_SUPPLIED]}
        )
        parts.append(f"the input has no file for {', '.join(slices)}")
    if TruncationCause.UNEXPLAINED in causes:
        slices = sorted(
            {f"{segment.line} on {segment.day.isoformat()}" for segment in causes[TruncationCause.UNEXPLAINED]}
        )
        parts.append(f"the file for {', '.join(slices)} carries no trips for it")
    return "; ".join(parts)
