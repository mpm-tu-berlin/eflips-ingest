"""
Reading and merging BVG-XML ``Linienfahrplan`` files.

This is the only module that touches XML. It parses each file, validates it against the
bundled ``bvg_xml.xsd``, and merges the per-file tables into corpus-wide ones. Nothing
here interprets the data — that is :mod:`._network`, :mod:`._routes` and
:mod:`._rotations`.

Merging matters because **the file is not the unit of interpretation**. The export slices
the network by ``(Linie, Stichtag)``, so a single file is an arbitrary window onto a
larger dataset:

- ~8 % of the ``BPunkt`` grid points in a file have no ``Hst`` twin *in that file*, but
  every one of them has a twin somewhere in the corpus.
- ``Strecke/ID`` is file-local (``xs:short``); the stable identity of a segment is
  ``(Startpunkt, Endpunkt)``, and its length agrees across files.
- A vehicle rotation is split across one file per line it touches, and cannot be judged
  complete from any single one of them.

Merging is therefore not an optimisation; it is what makes the downstream mappings total.

The other thing this module does is be forgiving about the input. An export dump is not a
clean set of documents: an operator who asks for every line number of the network gets the
export's "no data" answer for every number that is not in service that day (839 of the 2,205
files of the 2023 BVG dump), and files do arrive corrupt. Neither should stop the rest of the
zip from being imported, so :func:`read_files` separates both out instead of raising.
"""
import logging
import math
from collections import Counter
from dataclasses import dataclass, field, replace
from datetime import date
from functools import lru_cache
from pathlib import Path
from typing import Callable, Dict, FrozenSet, Iterable, List, Mapping, Sequence, Tuple

from lxml import etree
from xsdata.formats.dataclass.parsers import XmlParser

from eflips.ingest.bvgxml._xmldata import Linienfahrplan, ParameterName

Netzpunkt = Linienfahrplan.StreckennetzDaten.Netzpunkte.Netzpunkt
Haltestellenbereich = Linienfahrplan.StreckennetzDaten.Haltestellenbereiche.Haltestellenbereich
Fahrt = Linienfahrplan.FahrtDaten.Fahrt
Fahrzeugumlauf = Linienfahrplan.FahrzeugumlaufDaten.Fahrzeugumlauf
Route = Linienfahrplan.LinienDaten.Linie.RoutenDaten.Route

#: Identity of one route within the corpus. ``Route/LfdNr`` is renumbered between export
#: days (199 of 205 lines in the Berlin 2025-06 corpus do so), so the file index has to be
#: part of the key. The *geometric* identity of a route is its Punktfolge — see
#: :class:`eflips.ingest.bvgxml._routes.RouteShape`.
RouteId = Tuple[int, int]  # (file index, Route/LfdNr)


def parse_kalenderdatum(value: str) -> date:
    """Parse the German-format (``DD.MM.YYYY``) dates the export uses."""
    day, month, year = (int(part) for part in value.split("."))
    return date(day=day, month=month, year=year)


class EmptyResponse(ValueError):
    """
    The export answered that it has nothing for this ``(Linie, Stichtag)``.

    This is a *normal* answer, not a broken file. An operator who exports every line number
    of a network gets one of these for each number that is not in service on the day —
    839 of the 2,205 files of the 2023 BVG dump, for instance. Distinguished from a real
    parse failure so that :meth:`~eflips.ingest.bvgxml.BvgxmlIngester.prepare` can skip them
    without calling the input broken.
    """


def _first_text(doc: etree._Element, local_name: str) -> str | None:
    """
    The text of the first element with this local name, ignoring namespaces.

    Stripping the ``ns2`` prefix leaves the document in a *default* namespace, so element
    names carry a ``{uri}`` prefix that a plain path expression would not match.
    """
    for element in doc.iter():
        if isinstance(element.tag, str) and element.tag.rsplit("}", 1)[-1] == local_name:
            return (element.text or "").strip()
    return None


@lru_cache(maxsize=1)
def _schema() -> etree.XMLSchema:
    """The bundled schema, compiled once. Compiling it per file dominates a large import."""
    return etree.XMLSchema(etree.parse(Path(__file__).parent / "bvg_xml.xsd"))


def load_and_validate_xml(filename: Path) -> Linienfahrplan:
    """
    Parse one ``Linienfahrplan`` file and validate it against the bundled schema.

    :param filename: the file to load
    :return: the parsed document
    :raises EmptyResponse: if the file is the export's "no data" answer
    :raises ValueError: if the file does not parse or does not validate
    """
    # Read bytes: the document declares its own encoding and lxml honours it, whereas
    # ``read_text()`` would guess the platform default.
    raw = filename.read_bytes()

    # The ``ns2`` prefix has to go before parsing: with it, xsdata generates two separate
    # binding modules and the document no longer maps onto ``Linienfahrplan``.
    xml_string = raw.decode("utf-8").replace("ns2:", "").replace(":ns2", "")

    if "Keine gültige Linie." in xml_string:
        raise EmptyResponse(f"File {filename} is not a valid line.")
    if "Keine Umläufe vorhanden." in xml_string:
        raise EmptyResponse(f"File {filename} does not contain any rotations.")

    xml_doc = etree.fromstring(xml_string.encode("utf-8"))

    # The two messages above are the ones the reference corpora contain, but the general
    # discriminator is ``Ergebnis/ReturnCode``: 0 on all 2,804 files that carry data across
    # the three corpora and 1 on all 839 that do not. It has to be checked before validating,
    # because the schema admits ReturnCode 0 only — an unrecognised "no data" answer would
    # otherwise be reported as a corrupt file.
    return_code = _first_text(xml_doc, "ReturnCode")
    if return_code is not None and return_code != "0":
        reason = _first_text(xml_doc, "Meldungstext") or f"ReturnCode {return_code}"
        raise EmptyResponse(f"File {filename} carries no timetable data: {reason}")

    xmlschema = _schema()
    if not xmlschema.validate(xml_doc):
        raise ValueError(f"XML file {filename} is not valid: {xmlschema.error_log}")

    parser = XmlParser()
    data: Linienfahrplan = parser.from_string(xml_string, Linienfahrplan)
    return data


@dataclass(frozen=True)
class RawFile:
    """One parsed ``Linienfahrplan``, with the export parameters that describe its slice."""

    path: Path
    stichtag: date
    linie: str
    deployment: str
    release: str
    doc: Linienfahrplan

    @staticmethod
    def from_document(path: Path, doc: Linienfahrplan) -> "RawFile":
        parameters = {p.name: p.wert for p in doc.generierung.generierungs_parameter.parameter}
        stichtag = parameters.get(ParameterName.STICHTAG)
        if stichtag is None:
            raise ValueError(f"File {path} has no Stichtag in its GenerierungsParameter.")
        return RawFile(
            path=path,
            stichtag=parse_kalenderdatum(stichtag),
            linie=doc.linien_daten.linie.kurzname,
            deployment=doc.generierung.datenversion.deployment_id,
            release=doc.generierung.schnittstellenversion.release,
            doc=doc,
        )


@dataclass
class RawCorpus:
    """
    The merged view of every input file.

    The tables below are keyed by the identities that are stable *across* files. Building
    this object is also where the invariants we rely on downstream are checked, once, with
    messages that name the offending file.
    """

    files: Sequence[RawFile]

    #: Every ``Netzpunkt`` of the corpus, by ``Nummer``.
    netzpunkte: Dict[int, Netzpunkt] = field(default_factory=dict)

    #: Every ``Haltestellenbereich`` of the corpus, by ``Nummer``.
    haltestellenbereiche: Dict[int, Haltestellenbereich] = field(default_factory=dict)

    #: Road length in metres of the connection between two grid points, by
    #: ``(Startpunkt, Endpunkt)``. ``Strecke/ID`` is deliberately not a key: it is
    #: file-local and reused with different meanings between files.
    segment_length: Dict[Tuple[int, int], int] = field(default_factory=dict)

    #: Every ``Fahrt`` of the corpus, by its (globally unique) ``ID``.
    fahrten: Dict[int, Fahrt] = field(default_factory=dict)

    #: The route a ``Fahrt`` runs on, by ``Fahrt/ID``. Resolves the file-local
    #: ``LfdNrRoutenvariante`` -> ``Routenvariante`` -> ``Route`` indirection once.
    route_of_fahrt: Dict[int, RouteId] = field(default_factory=dict)

    #: Every ``Route`` of the corpus, by :data:`RouteId`.
    routen: Dict[RouteId, Route] = field(default_factory=dict)

    #: The hops that a route's own ``Streckenfolge`` accounts for, as
    #: ``(from, to)`` grid point pairs. A hop of the ``Punktfolge`` that is *not* in here
    #: is where the export collapsed the route — see
    #: :func:`eflips.ingest.bvgxml._routes.resolve_routes`.
    route_covered_hops: Dict[RouteId, FrozenSet[Tuple[int, int]]] = field(default_factory=dict)

    #: All ``Fahrzeugumlauf`` elements, paired with the index of the file they came from.
    fahrzeugumlaeufe: List[Tuple[int, Fahrzeugumlauf]] = field(default_factory=list)

    @property
    def stichtage(self) -> FrozenSet[date]:
        """The set of export dates covered by the input files."""
        return frozenset(f.stichtag for f in self.files)

    @property
    def linien(self) -> FrozenSet[str]:
        """The set of line names covered by the input files."""
        return frozenset(f.linie for f in self.files)

    @property
    def slices(self) -> FrozenSet[Tuple[str, date]]:
        """
        The ``(Linie, Stichtag)`` slices the input covers.

        This, not the set of lines, is the unit the export works in: a file describes one
        line on one day, so a rotation that runs line M29 on a day for which no M29 file was
        supplied is just as unimportable as one whose line is missing entirely.
        """
        return frozenset((f.linie, f.stichtag) for f in self.files)


@dataclass
class PreparedInput:
    """
    The outcome of reading a directory of ``Linienfahrplan`` files.

    A real export is not a clean set of documents. Files that carry no data and files that
    are outright broken both occur, and neither should stop the rest of the zip from being
    imported — so they are separated out here rather than raised.
    """

    #: The files that parsed and carry data.
    files: List[RawFile] = field(default_factory=list)

    #: Names of files that are the export's "no data" answer. Nothing is lost with these:
    #: the response says outright that the slice is empty.
    skipped_empty: List[str] = field(default_factory=list)

    #: Names of files that could not be read, and why. Data *is* lost with these, so any
    #: vehicle rotation reaching into one of them is reported as truncated further down the
    #: line rather than silently kept.
    skipped_invalid: Dict[str, str] = field(default_factory=dict)


def read_files(
    paths: Iterable[Path],
    progress_callback: None | Callable[[float], None] = None,
) -> PreparedInput:
    """
    Parse every file, separating the ones that carry no data and the ones that are broken.

    :param paths: the ``Linienfahrplan`` files to read
    :param progress_callback: called with a value in ``[0, 1]`` as the files are read
    :return: the parsed files and the two kinds of skipped ones
    """
    logger = logging.getLogger(__name__)
    ordered = sorted(paths)
    prepared = PreparedInput()

    for index, path in enumerate(ordered):
        try:
            prepared.files.append(RawFile.from_document(path, load_and_validate_xml(path)))
        except EmptyResponse:
            prepared.skipped_empty.append(path.name)
        except Exception as e:  # noqa: BLE001 — lxml, xsdata and codecs all raise their own
            prepared.skipped_invalid[path.name] = f"{type(e).__name__}: {e}"
            logger.warning("Skipping %s, which could not be read: %s", path.name, e)
        if progress_callback and ordered:
            progress_callback((index + 1) / len(ordered))

    if prepared.skipped_empty:
        logger.info(
            "%d of %d files are the export's 'no data' answer for their line and day, and " "were skipped.",
            len(prepared.skipped_empty),
            len(ordered),
        )
    if prepared.skipped_invalid:
        logger.warning(
            "%d of %d files could not be read and were skipped: %s. Vehicle rotations that "
            "reach into them will be reported as incomplete and dropped.",
            len(prepared.skipped_invalid),
            len(ordered),
            ", ".join(sorted(prepared.skipped_invalid)[:10]),
        )
    return prepared


def read_corpus(paths: Iterable[Path]) -> RawCorpus:
    """
    Parse and merge every file into a :class:`RawCorpus`, skipping unusable ones.

    :param paths: the ``Linienfahrplan`` files to read
    :return: the merged corpus
    :raises ValueError: if no file carries data, or if the files contradict each other about
        a shared identity
    """
    files = read_files(paths).files
    if not files:
        raise ValueError("No input files carry any data.")
    return merge_corpus(files)


def merge_corpus(files: Sequence[RawFile]) -> RawCorpus:
    """
    Merge already-parsed files into a :class:`RawCorpus`, checking the shared identities.

    Split out from :func:`read_corpus` so that ``prepare()`` can validate eagerly and
    ``ingest()`` can rebuild the merged view from the pickled documents without re-parsing
    XML.
    """
    logger = logging.getLogger(__name__)
    corpus = RawCorpus(files=files)

    # A segment's length is consistent across files for all but a handful of legs: 13 of the
    # 10,754 pairs in the Berlin 2025-06 corpus disagree, and where they do the spread can be
    # a quarter of the leg (the worst is 2,200 m on one of 9,100 m). Collect every reading and
    # settle on one below, rather than letting the last file read win: the route geometry has
    # to be a function of the Punktfolge alone, or the same route would come out different
    # depending on which file it was read from.
    segment_readings: Dict[Tuple[int, int], Counter[int]] = {}

    deployments = {f.deployment for f in files}
    if len(deployments) > 1:
        logger.warning(
            "The input files come from %d different IVU deployments (%s). Export quality "
            "differs markedly between deployments; expect inconsistent data.",
            len(deployments),
            ", ".join(sorted(deployments)),
        )

    # A grid point may legitimately be relocated between export days: in the Berlin 2025-06
    # corpus, Andreasstr./Lange Str. (Netzpunkt 101005166) sits 93 m further south in the
    # 21.06.2025 file than in the 20.06. one, and the Strecke leading into it shortens by
    # the same 93 m a day later. All seven files come from one export run, so this is a
    # dated change in the source's master data, not export noise. That is a new position
    # for the same point,
    # not a contradiction, so only the fields that decide which *station* the point belongs
    # to are treated as identity.
    #
    # Which of the positions to keep is a choice, and this is a planning tool: a stop that
    # moves for a construction site is noise we would rather not model. Collect every
    # reading and settle on the one most files agree on, the same rule the segment lengths
    # use below. Unlike a median it always yields a position that was actually surveyed,
    # rather than the midpoint between two of them when the readings split evenly.
    point_readings: Dict[int, Counter[Tuple[int, int]]] = {}
    renamed_bereiche = 0

    for index, raw in enumerate(files):
        doc = raw.doc

        for netzpunkt in doc.streckennetz_daten.netzpunkte.netzpunkt:
            previous = corpus.netzpunkte.get(netzpunkt.nummer)
            if previous is None:
                corpus.netzpunkte[netzpunkt.nummer] = netzpunkt
            elif (
                previous.netzpunkttyp != netzpunkt.netzpunkttyp
                or previous.haltestellenbereich != netzpunkt.haltestellenbereich
            ):
                raise ValueError(
                    f"Netzpunkt {netzpunkt.nummer} is a "
                    f"{previous.netzpunkttyp.value} belonging to Haltestellenbereich "
                    f"{previous.haltestellenbereich} in an earlier file, but a "
                    f"{netzpunkt.netzpunkttyp.value} belonging to "
                    f"{netzpunkt.haltestellenbereich} in {raw.path.name}. The input files "
                    f"contradict each other about the network; they are probably from "
                    f"different data versions."
                )
            point_readings.setdefault(netzpunkt.nummer, Counter())[(netzpunkt.xkoordinate, netzpunkt.ykoordinate)] += 1

        for bereich in doc.streckennetz_daten.haltestellenbereiche.haltestellenbereich:
            previous_bereich = corpus.haltestellenbereiche.get(bereich.nummer)
            if previous_bereich is None:
                corpus.haltestellenbereiche[bereich.nummer] = bereich
            elif previous_bereich != bereich:
                renamed_bereiche += 1

        # Strecke/ID is file-local (xs:short, reused from 1 in every file), so resolve it
        # to the (from, to) pair that identifies the same connection everywhere.
        strecken: Dict[int, Tuple[int, int]] = {}
        for strecke in doc.streckennetz_daten.strecken.strecke:
            strecken[strecke.id] = (strecke.startpunkt, strecke.endpunkt)
            segment_readings.setdefault((strecke.startpunkt, strecke.endpunkt), Counter())[strecke.streckenlaenge] += 1

        # Routenvariante is a thin indirection: a variant differs from its route only in
        # who contracts it and which points are published, neither of which affects the
        # driven geometry. Resolve it away here.
        route_of_variante = {
            variante.lfd_nr: variante.lfd_nr_route for variante in doc.linien_daten.linie.routenvarianten.routenvariante
        }
        for route in doc.linien_daten.linie.routen_daten.route:
            route_id = (index, route.lfd_nr)
            corpus.routen[route_id] = route
            corpus.route_covered_hops[route_id] = frozenset(
                strecken[s.strecken_id] for s in route.streckenfolge.strecke if s.strecken_id in strecken
            )

        for fahrt in doc.fahrt_daten.fahrt:
            if fahrt.id in corpus.fahrten:
                previous_file = _file_of_fahrt(corpus, fahrt.id, files)
                raise ValueError(
                    f"Fahrt ID {fahrt.id} is defined both in {previous_file} and in "
                    f"{raw.path.name}. Fahrt IDs are globally unique in this export "
                    f"format; the input files overlap or one of them is corrupt."
                )
            route_lfd_nr = route_of_variante.get(fahrt.lfd_nr_routenvariante)
            if route_lfd_nr is None:
                raise ValueError(
                    f"Fahrt ID {fahrt.id} in {raw.path.name} references "
                    f"Routenvariante {fahrt.lfd_nr_routenvariante}, which the file does "
                    f"not define."
                )
            corpus.fahrten[fahrt.id] = fahrt
            corpus.route_of_fahrt[fahrt.id] = (index, route_lfd_nr)

        for fahrzeugumlauf in doc.fahrzeugumlauf_daten.fahrzeugumlauf:
            corpus.fahrzeugumlaeufe.append((index, fahrzeugumlauf))

    _settle_coordinates(corpus, point_readings)

    ambiguous = 0
    for pair, readings in segment_readings.items():
        # Most common reading, ties broken towards the longer one so that an estimate is
        # never optimistic.
        best = max(readings.items(), key=lambda item: (item[1], item[0]))[0]
        if len(readings) > 1:
            ambiguous += 1
        corpus.segment_length[pair] = best
    if ambiguous:
        logger.info(
            "%d of %d Strecken have more than one length in the corpus; using the most "
            "common reading of each (ties broken towards the longer one).",
            ambiguous,
            len(segment_readings),
        )
    if renamed_bereiche:
        logger.info(
            "%d Haltestellenbereiche are named differently in different files; using the " "earliest name of each.",
            renamed_bereiche,
        )

    logger.info(
        "Read %d files (%s, %s): %d Netzpunkte, %d Haltestellenbereiche, %d Strecken, "
        "%d Fahrten, %d Fahrzeugumlauf elements.",
        len(files),
        _format_range(sorted(corpus.stichtage)),
        ", ".join(sorted(deployments)),
        len(corpus.netzpunkte),
        len(corpus.haltestellenbereiche),
        len(corpus.segment_length),
        len(corpus.fahrten),
        len(corpus.fahrzeugumlaeufe),
    )
    return corpus


def _settle_coordinates(corpus: RawCorpus, readings: Mapping[int, Counter[Tuple[int, int]]]) -> None:
    """
    Give every grid point the position most of its files agree on, and report the moves.

    Warned about rather than logged quietly: the corpus that comes out describes the network
    as it stood on *most* of the imported days, which is not the network of any one of them.
    That is the right trade for a planning tool, but it is a decision the operator should
    see, because the remedy — importing a narrower date range — is theirs to make.
    """
    logger = logging.getLogger(__name__)

    moved: List[Tuple[float, int, str]] = []
    for number, counter in readings.items():
        if len(counter) == 1:
            continue
        # Ties go to the earliest reading: ``Counter`` iterates in insertion order and
        # ``max`` returns the first of the maximal items, and files are merged in date order.
        chosen = max(counter.items(), key=lambda item: item[1])[0]
        point = corpus.netzpunkte[number]
        corpus.netzpunkte[number] = replace(point, xkoordinate=chosen[0], ykoordinate=chosen[1])
        # Soldner coordinates are in millimetres, and the projection is Cartesian, so the
        # straight-line distance is the plain Euclidean one.
        furthest = max(math.dist(chosen, other) for other in counter) / 1000.0
        moved.append((furthest, number, point.langname))

    if moved:
        moved.sort(reverse=True)
        logger.warning(
            "The input files place %d grid point%s at more than one position, having been "
            "relocated between export days (%s). Each keeps the position that most of "
            "its files agree on, so the resulting network is the one in force on most of "
            "the imported days rather than on any single one. Import a narrower date range "
            "if you need one particular day's network.",
            len(moved),
            "" if len(moved) == 1 else "s",
            "; ".join(f"{name} moves {distance:.0f} m" for distance, _, name in moved[:5]),
        )


def _file_of_fahrt(corpus: RawCorpus, fahrt_id: int, files: Sequence[RawFile]) -> str:
    index = corpus.route_of_fahrt.get(fahrt_id, (-1, -1))[0]
    return files[index].path.name if 0 <= index < len(files) else "an earlier file"


def _format_range(dates: List[date]) -> str:
    if not dates:
        return "no dates"
    if len(dates) == 1:
        return dates[0].isoformat()
    return f"{dates[0].isoformat()}–{dates[-1].isoformat()}"
