"""
The physical network: grid points, the stations they belong to, and the segments between.

The one thing this module exists to do is resolve a ``Netzpunkt`` to the station it is
part of. That resolution is **arithmetic**, because ``Netzpunkt/Nummer`` is a structured
key rather than an opaque id. Every number is exactly nine digits:

===================  ===========================================================
``101``  + 6 digits  ``Hst`` — one platform. The *only* type that carries the
                     ``<Haltestellenbereich>`` foreign key, in every export
                     examined (2023 R21, 2025 R23, 2026 UGFPL).
``102``  + 6 digits  ``BPunkt`` — the layover position *at* that ``Hst``. Its
                     number is the ``Hst``'s plus :data:`BPUNKT_OFFSET`.
``103`` + ``1``/``2`` ``EPkt`` / ``APkt`` — the pull-out and pull-in point of one
        + 5 digits   depot. The trailing five digits are the depot code and are
                     shared by the pair.
``101``  + 6 digits  ``GPkt`` — boundary point. Never appears in a ``Punktfolge``.
===================  ===========================================================

Verified across three reference corpora: 1,648 of 1,651 ``BPunkt`` grid points have an
``Hst`` twin at ``Nummer - 1_000_000``, with matching coordinates and Langname; every
``EPkt`` has an ``APkt`` at the same coordinates. The mapping is total on Berlin 2025-06
(761 of 761) and the 2026 export (140 of 140); the three exceptions are all in the 2023
dump, and they fall back to :attr:`StationKind.UNMATCHED_STOP` with a warning rather than
being dropped. Under the resulting station mapping *every* consecutive trip pair in a
vehicle working shares an endpoint, in all three corpora. That is what makes station
merging, short-name prefix matching and depot-name token lists unnecessary.

The twin is looked up in the merged corpus, not in one file: roughly 8 % of the ``BPunkt``
grid points in a given file have no twin *in that file*.
"""
import logging
import statistics
from dataclasses import dataclass
from enum import Enum
from functools import lru_cache
from typing import Dict, List, Mapping, Optional, Tuple

from eflips.ingest.bvgxml._read import Netzpunkt, RawCorpus
from eflips.ingest.bvgxml._xmldata import NetzpunktNetzpunkttyp
from eflips.ingest.util import soldner_to_pointz

#: A ``BPunkt``'s number is its ``Hst``'s number plus this. See the module docstring.
BPUNKT_OFFSET = 1_000_000

#: Where the Einsetzen/Aussetzen discriminator sits in a depot point's number, and where
#: the depot code starts. ``103`` ``1`` ``09400`` -> depot ``9400``.
_DEPOT_DISCRIMINATOR_DIGIT = 3
_DEPOT_CODE_DIGIT = 4


class StationKind(Enum):
    """What sort of place a :class:`StationRef` denotes."""

    #: A passenger stop, identified by its ``Haltestellenbereich`` number.
    STOP = "stop"

    #: A depot, identified by the code shared by its ``EPkt``/``APkt`` pair.
    DEPOT = "depot"

    #: A ``BPunkt`` whose ``Hst`` twin is in none of the input files, so no
    #: ``Haltestellenbereich`` is reachable. Identified by the twin's *number*, which is
    #: still a stable identity — two such points coalesce iff they name the same place.
    UNMATCHED_STOP = "unmatched_stop"


@dataclass(frozen=True)
class StationRef:
    """The canonical identity of a place. Distinct refs are distinct stations."""

    kind: StationKind
    key: int

    @property
    def sort_key(self) -> Tuple[str, int]:
        """A total order, so that a schedule is written in a reproducible sequence."""
        return self.kind.value, self.key


@dataclass(frozen=True)
class GridPoint:
    """A ``Netzpunkt``, resolved."""

    number: int
    typ: NetzpunktNetzpunkttyp
    station: StationRef
    x: int  # Soldner, millimetres
    y: int  # Soldner, millimetres
    name: str

    @property
    def is_stop(self) -> bool:
        """
        Whether passengers may board or alight here.

        ``Punkt/Fahrgastwechsel`` and ``Netzpunkt/mitFahrgastwechsel`` are both exactly
        equivalent to this in both reference corpora (79,611 + 6,528 points checked), so
        neither is read.
        """
        return self.typ == NetzpunktNetzpunkttyp.HST

    @property
    def is_depot(self) -> bool:
        return self.station.kind == StationKind.DEPOT


@dataclass
class StationInfo:
    """A station of the output model, and the grid points that make it up."""

    ref: StationRef
    name: str
    name_short: str
    members: List[int]  # Netzpunkt numbers


class Network:
    """
    The resolved network of one import.

    Built once from the merged :class:`~eflips.ingest.bvgxml._read.RawCorpus`, then read
    by :mod:`._routes`. It owns no database objects.
    """

    def __init__(
        self,
        grid_points: Mapping[int, GridPoint],
        stations: Mapping[StationRef, StationInfo],
        segment_length: Mapping[Tuple[int, int], int],
    ) -> None:
        self.grid_points = grid_points
        self.stations = stations
        self.segment_length = segment_length
        self._adjacency: Optional[Dict[int, List[Tuple[int, int]]]] = None

    def station_of(self, number: int) -> StationRef:
        """The station a grid point belongs to."""
        return self.grid_points[number].station

    @property
    def adjacency(self) -> Dict[int, List[Tuple[int, int]]]:
        """The Streckennetz as ``{start: [(end, length), ...]}``, built on first use."""
        if self._adjacency is None:
            adjacency: Dict[int, List[Tuple[int, int]]] = {}
            for (start, end), length in self.segment_length.items():
                adjacency.setdefault(start, []).append((end, length))
            self._adjacency = adjacency
        return self._adjacency

    def geom_of_point(self, number: int) -> str:
        """The PostGIS geometry of one grid point."""
        point = self.grid_points[number]
        return _point_geom(point.x, point.y)

    def geom_of_station(self, ref: StationRef) -> str:
        """
        The PostGIS geometry of a station: the median of its grid points.

        The median (rather than the mean) keeps the station on top of one of its own
        platforms even when one member is an outlier.
        """
        members = [self.grid_points[number] for number in self.stations[ref].members]
        return _point_geom(
            int(statistics.median(p.x for p in members)),
            int(statistics.median(p.y for p in members)),
        )


#: Bound on the process-wide coordinate cache below. A full Berlin import has about 7,000
#: distinct coordinates, so this holds several imports' worth without growing unboundedly
#: in a long-running server.
_GEOM_CACHE_SIZE = 200_000


@lru_cache(maxsize=_GEOM_CACHE_SIZE)
def _point_geom(x: int, y: int) -> str:
    """
    Convert Soldner millimetres to a PostGIS geometry, memoised.

    Memoisation matters: the conversion may perform a network altitude lookup, and one
    import asks for the same few thousand coordinates once per route that calls at them.
    """
    return soldner_to_pointz(x, y)


def build_network(corpus: RawCorpus) -> Network:
    """
    Resolve every grid point of the corpus to a station.

    :param corpus: the merged input
    :return: the resolved network
    :raises ValueError: if a grid point has a type the format does not define
    """
    logger = logging.getLogger(__name__)

    refs: Dict[int, StationRef] = {}
    unmatched: List[Netzpunkt] = []
    for number, netzpunkt in corpus.netzpunkte.items():
        ref = _station_ref(number, netzpunkt, corpus)
        refs[number] = ref
        # GPkt lands on UNMATCHED_STOP too, but no Punktfolge ever names one, so it never
        # reaches the output and is not worth a warning.
        if ref.kind == StationKind.UNMATCHED_STOP and netzpunkt.netzpunkttyp == NetzpunktNetzpunkttyp.BPUNKT:
            unmatched.append(netzpunkt)

    if unmatched:
        logger.warning(
            "%d BPunkt grid points have no Hst twin anywhere in the input (e.g. %s). They "
            "become stations of their own, named after the BPunkt. Include the lines that "
            "serve those stops to merge them with the real stop.",
            len(unmatched),
            ", ".join(sorted(p.kurzname for p in unmatched)[:5]),
        )

    grid_points: Dict[int, GridPoint] = {}
    for number, netzpunkt in corpus.netzpunkte.items():
        grid_points[number] = GridPoint(
            number=number,
            typ=netzpunkt.netzpunkttyp,
            station=refs[number],
            x=netzpunkt.xkoordinate,
            y=netzpunkt.ykoordinate,
            name=netzpunkt.langname,
        )

    stations: Dict[StationRef, StationInfo] = {}
    for number, point in grid_points.items():
        info = stations.get(point.station)
        if info is None:
            info = _station_info(point.station, corpus)
            stations[point.station] = info
        info.members.append(number)

    return Network(grid_points=grid_points, stations=stations, segment_length=corpus.segment_length)


def _station_ref(number: int, netzpunkt: Netzpunkt, corpus: RawCorpus) -> StationRef:
    """Resolve one grid point to its station, by arithmetic on the number."""
    typ = netzpunkt.netzpunkttyp

    if typ == NetzpunktNetzpunkttyp.HST:
        if netzpunkt.haltestellenbereich is None:
            # Not observed in any export: Hst is the one type that always carries the FK.
            return StationRef(StationKind.UNMATCHED_STOP, number)
        return StationRef(StationKind.STOP, netzpunkt.haltestellenbereich)

    if typ == NetzpunktNetzpunkttyp.BPUNKT:
        twin_number = number - BPUNKT_OFFSET
        twin = corpus.netzpunkte.get(twin_number)
        if twin is not None and twin.netzpunkttyp == NetzpunktNetzpunkttyp.HST and twin.haltestellenbereich is not None:
            return StationRef(StationKind.STOP, twin.haltestellenbereich)
        # The twin is in none of the input files. Key on its number anyway: it is the same
        # stable identity the Hst would have had, so two BPunkte at one stop still merge.
        return StationRef(StationKind.UNMATCHED_STOP, twin_number)

    if typ in (NetzpunktNetzpunkttyp.EPKT, NetzpunktNetzpunkttyp.APKT):
        return StationRef(StationKind.DEPOT, depot_code(number))

    if typ == NetzpunktNetzpunkttyp.GPKT:
        # A boundary point. It appears in the Netzpunkte list but in no Punktfolge, so it
        # never reaches the output; give it an identity rather than raising.
        return StationRef(StationKind.UNMATCHED_STOP, number)

    raise ValueError(f"Netzpunkt {number} ({netzpunkt.kurzname!r}) has unsupported type {typ}.")


def depot_code(number: int) -> int:
    """
    The depot code encoded in an ``EPkt``/``APkt`` number.

    ``103`` ``1`` ``09400`` (Einsetzen) and ``103`` ``2`` ``09400`` (Aussetzen) are the two
    ends of depot ``9400``, so dropping the discriminator digit folds them together.
    """
    digits = str(number)
    return int(digits[_DEPOT_CODE_DIGIT:])


def _station_info(ref: StationRef, corpus: RawCorpus) -> StationInfo:
    """Name a station. Identity is already fixed by ``ref``; this is presentation only."""
    if ref.kind == StationKind.STOP:
        bereich = corpus.haltestellenbereiche.get(ref.key)
        if bereich is not None:
            return StationInfo(ref=ref, name=bereich.fahrplanbuchname, name_short=bereich.kurzname, members=[])
        # An Hst pointing at a Haltestellenbereich that no file lists. Fall back to the
        # point's own name below.
        return StationInfo(ref=ref, name=f"Haltestellenbereich {ref.key}", name_short=str(ref.key), members=[])

    if ref.kind == StationKind.DEPOT:
        return _depot_info(ref, corpus)

    twin = corpus.netzpunkte.get(ref.key)
    if twin is not None:
        return StationInfo(ref=ref, name=twin.langname, name_short=twin.kurzname, members=[])
    sibling = corpus.netzpunkte.get(ref.key + BPUNKT_OFFSET)
    if sibling is not None:
        return StationInfo(
            ref=ref,
            name=sibling.langname,
            name_short=sibling.kurzname.removesuffix("B"),
            members=[],
        )
    return StationInfo(ref=ref, name=f"Netzpunkt {ref.key}", name_short=str(ref.key), members=[])


def _depot_info(ref: StationRef, corpus: RawCorpus) -> StationInfo:
    """
    Name a depot from whichever of its ``EPkt``/``APkt`` the corpus has.

    Their names carry an Einsetzen/Aussetzen suffix that describes the *direction*, not
    the place: ``"Betriebshof Lichtenberg Einsetzen"`` / ``"BF L E"``. Strip it. A name
    that does not follow the convention is kept as it is — the identity is already fixed
    by the number, so an odd name only makes a worse label.
    """
    for number, netzpunkt in corpus.netzpunkte.items():
        if netzpunkt.netzpunkttyp not in (NetzpunktNetzpunkttyp.EPKT, NetzpunktNetzpunkttyp.APKT):
            continue
        if depot_code(number) != ref.key:
            continue
        name = netzpunkt.langname.removesuffix("Einsetzen").removesuffix("Aussetzen").strip()
        short = netzpunkt.kurzname.removesuffix(" E").removesuffix(" A").strip()
        return StationInfo(ref=ref, name=name or netzpunkt.langname, name_short=short or netzpunkt.kurzname, members=[])
    return StationInfo(ref=ref, name=f"Betriebshof {ref.key}", name_short=f"BF {ref.key}", members=[])
