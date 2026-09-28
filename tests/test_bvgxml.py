import glob
import logging
import math
import os
from dataclasses import replace
from datetime import date, timedelta
from pathlib import Path
from typing import Dict, List
from uuid import UUID, uuid4
from zipfile import ZipFile

import eflips.model
import pytest
from eflips.model import create_engine
from sqlalchemy.orm import Session

from eflips.ingest.bvgxml import BvgxmlIngester
from eflips.ingest.bvgxml._network import BPUNKT_OFFSET, StationKind, StationRef, build_network, depot_code
from eflips.ingest.bvgxml._read import (
    EmptyResponse,
    RawCorpus,
    RawFile,
    load_and_validate_xml,
    merge_corpus,
    read_corpus,
    read_files,
)
from eflips.ingest.bvgxml._rotations import TruncationCause, build_rotations
from eflips.ingest.bvgxml._routes import (
    CROW_FLY_DETOUR_FACTOR,
    DEPOT_LEG_SPEED_KMH,
    MIN_ESTIMATED_DISTANCE_M,
    StopTiming,
    resolve_routes,
    spread_arrivals,
)
from eflips.ingest.bvgxml._schedule import DEADHEAD_FAHRTARTEN, build_schedule, first_and_last_are_depots
from eflips.ingest.bvgxml._xmldata import Linienfahrplan, NetzpunktNetzpunkttyp
from tests.base import BaseIngester

SAMPLE_DIR = Path(os.path.dirname(os.path.abspath(__file__))) / ".." / "samples" / "BVGXML"

#: A "no data" answer, as the export gives for a line that is not in service on the day.
#: 839 of the 2,205 files of the 2023 BVG dump look like this. The Meldungstext here is
#: deliberately *not* one of the two the reference corpora contain, so that what is
#: exercised is the general ``ReturnCode`` discriminator rather than a hard-coded phrase.
NO_DATA_RESPONSE = """<?xml version="1.0" encoding="UTF-8"?>
<ns2:Linienfahrplan xmlns:ns2="http://www.ivu.de/mb/intf/passengercount/remote/model/">
    <Generierung>
        <Ergebnis>
            <ReturnCode>1</ReturnCode>
            <Meldungsliste>
                <Meldung>
                    <Meldungstext>Kein Fahrplan zum Stichtag vorhanden.</Meldungstext>
                </Meldung>
            </Meldungsliste>
        </Ergebnis>
    </Generierung>
</ns2:Linienfahrplan>
"""


def sample_paths() -> List[Path]:
    return [Path(p) for p in sorted(glob.glob(str(SAMPLE_DIR / "*.xml")))]


@pytest.fixture(scope="module")
def corpus() -> RawCorpus:
    return read_corpus(sample_paths())


@pytest.fixture(scope="module")
def resolved(corpus):
    """The whole pipeline over the sample corpus, up to but excluding the database."""
    network = build_network(corpus)
    routes = resolve_routes(corpus, network)
    rotations = build_rotations(corpus)
    schedule = build_schedule(corpus, network, routes, rotations)
    return network, routes, rotations, schedule


class TestRead:
    def test_load_and_validate(self):
        loaded = load_and_validate_xml(sample_paths()[0])
        assert isinstance(loaded, Linienfahrplan)

    def test_corpus_merges_every_file(self, corpus):
        assert len(corpus.files) == len(sample_paths())
        assert len(corpus.netzpunkte) > 0
        assert len(corpus.haltestellenbereiche) > 0
        assert len(corpus.fahrten) > 0
        assert len(corpus.fahrzeugumlaeufe) > 0

    def test_slices_are_line_and_day(self, corpus):
        assert corpus.slices == {(f.linie, f.stichtag) for f in corpus.files}
        assert len(corpus.stichtage) == len(sample_paths())

    def test_segment_identity_is_the_endpoint_pair(self, corpus):
        """Strecke/ID is file-local, so the corpus keys segments by (from, to)."""
        for (start, end), length in corpus.segment_length.items():
            assert start in corpus.netzpunkte
            assert end in corpus.netzpunkte
            assert length >= 0

    def test_duplicate_fahrt_id_raises(self):
        paths = sample_paths()
        first = RawFile.from_document(paths[0], load_and_validate_xml(paths[0]))
        again = RawFile.from_document(paths[0], load_and_validate_xml(paths[0]))
        with pytest.raises(ValueError, match="globally unique"):
            merge_corpus([first, again])

    @staticmethod
    def _two_files_sharing_a_netzpunkt(**changes):
        """
        Two files that describe the same grid point, the second one with ``changes``
        applied. The samples are three unrelated lines, so the shared point is planted.
        """
        paths = sample_paths()
        first = RawFile.from_document(paths[0], load_and_validate_xml(paths[0]))
        second = RawFile.from_document(paths[1], load_and_validate_xml(paths[1]))
        original = first.doc.streckennetz_daten.netzpunkte.netzpunkt[0]
        second.doc.streckennetz_daten.netzpunkte.netzpunkt.append(replace(original, **changes))
        return first, second, original

    def test_contradicting_netzpunkt_type_raises(self):
        first, second, _ = self._two_files_sharing_a_netzpunkt(netzpunkttyp=NetzpunktNetzpunkttyp.GPKT)
        with pytest.raises(ValueError, match="contradict each other about the network"):
            merge_corpus([first, second])

    def test_no_data_answer_is_an_empty_response(self, tmp_path):
        """
        The export answers a line it has no timetable for with a non-zero ReturnCode. That
        is a normal answer, not a broken file, and it must be told apart from one — the
        schema admits ReturnCode 0 only, so validating first would call every such answer
        corrupt.
        """
        path = tmp_path / "empty.xml"
        path.write_text(NO_DATA_RESPONSE, encoding="utf-8")
        with pytest.raises(EmptyResponse, match="Kein Fahrplan zum Stichtag vorhanden."):
            load_and_validate_xml(path)

    def test_read_files_separates_no_data_answers_from_broken_ones(self, tmp_path):
        """
        A real export dump contains all three kinds. Neither of the unusable kinds may stop
        the usable files from being read.
        """
        real = tmp_path / "real.xml"
        real.write_bytes(sample_paths()[0].read_bytes())
        (tmp_path / "no_data.xml").write_text(NO_DATA_RESPONSE, encoding="utf-8")
        (tmp_path / "truncated.xml").write_text("<Linienfahrplan><Generierung>", encoding="utf-8")
        (tmp_path / "not_xml.xml").write_text("this is not xml at all", encoding="utf-8")

        prepared = read_files(tmp_path.glob("*.xml"))

        assert [f.path for f in prepared.files] == [real]
        assert prepared.skipped_empty == ["no_data.xml"]
        assert set(prepared.skipped_invalid) == {"truncated.xml", "not_xml.xml"}
        assert all(message for message in prepared.skipped_invalid.values())

    def test_read_files_reports_progress_over_every_file(self, tmp_path):
        for index in range(4):
            (tmp_path / f"{index}.xml").write_text(NO_DATA_RESPONSE, encoding="utf-8")
        seen: List[float] = []
        read_files(tmp_path.glob("*.xml"), progress_callback=seen.append)
        assert seen == [0.25, 0.5, 0.75, 1.0]

    def test_moved_netzpunkt_does_not_raise(self):
        """A stop that is relocated between export days is new data, not a conflict."""
        first, second, original = self._two_files_sharing_a_netzpunkt()
        moved = second.doc.streckennetz_daten.netzpunkte.netzpunkt[-1]
        moved.xkoordinate += 93_000  # 93 m, as observed between the 20. and 21.06.2025 files
        merged = merge_corpus([first, second])
        # One reading each, so the tie falls to the earlier file.
        assert merged.netzpunkte[original.nummer].xkoordinate == original.xkoordinate

    def test_moved_netzpunkt_takes_the_position_most_files_agree_on(self):
        """
        A relocation is settled by majority, not by which file happens to be read first.

        Andreasstr./Lange Str. sits at its old position in five of the seven Berlin 2025-06
        files and at its new one in two, so the old position wins there. Reverse the
        majority, as an import that starts after the move would, and the new one has to.
        """
        first, second, original = self._two_files_sharing_a_netzpunkt()
        third = RawFile.from_document(sample_paths()[2], load_and_validate_xml(sample_paths()[2]))
        for raw in (second, third):
            planted = replace(original, xkoordinate=original.xkoordinate + 93_000)
            if raw is third:
                raw.doc.streckennetz_daten.netzpunkte.netzpunkt.append(planted)
            else:
                raw.doc.streckennetz_daten.netzpunkte.netzpunkt[-1] = planted

        merged = merge_corpus([first, second, third])

        assert merged.netzpunkte[original.nummer].xkoordinate == original.xkoordinate + 93_000
        assert merged.netzpunkte[original.nummer].ykoordinate == original.ykoordinate

    def test_moved_netzpunkt_is_warned_about_by_name(self, caplog):
        """
        The operator has to be able to see *which* stop moved and how far: the remedy is to
        import a narrower date range, and that is their decision to make.
        """
        first, second, original = self._two_files_sharing_a_netzpunkt()
        moved = second.doc.streckennetz_daten.netzpunkte.netzpunkt[-1]
        moved.xkoordinate += 93_000

        with caplog.at_level(logging.WARNING, logger="eflips.ingest.bvgxml._read"):
            merge_corpus([first, second])

        assert original.langname in caplog.text
        assert "93 m" in caplog.text


class TestNetwork:
    def test_bpunkt_resolves_to_its_hst_twin(self, corpus, resolved):
        network, _, _, _ = resolved
        checked = 0
        for number, netzpunkt in corpus.netzpunkte.items():
            if netzpunkt.netzpunkttyp != NetzpunktNetzpunkttyp.BPUNKT:
                continue
            twin = corpus.netzpunkte.get(number - BPUNKT_OFFSET)
            if twin is None:
                continue
            assert network.station_of(number) == network.station_of(twin.nummer)
            assert network.station_of(number).kind is StationKind.STOP
            checked += 1
        assert checked > 0

    def test_bpunkt_without_a_twin_gets_its_own_station(self, corpus, resolved):
        network, _, _, _ = resolved
        orphans = [
            number
            for number, netzpunkt in corpus.netzpunkte.items()
            if netzpunkt.netzpunkttyp == NetzpunktNetzpunkttyp.BPUNKT
            and (number - BPUNKT_OFFSET) not in corpus.netzpunkte
        ]
        for number in orphans:
            ref = network.station_of(number)
            assert ref.kind is StationKind.UNMATCHED_STOP
            # Keyed on the twin's number, so a second BPunkt at the same stop still merges.
            assert ref.key == number - BPUNKT_OFFSET

    def test_depot_pull_out_and_pull_in_are_one_station(self, corpus, resolved):
        network, _, _, _ = resolved
        pairs: Dict[int, List[int]] = {}
        for number, netzpunkt in corpus.netzpunkte.items():
            if netzpunkt.netzpunkttyp in (NetzpunktNetzpunkttyp.EPKT, NetzpunktNetzpunkttyp.APKT):
                pairs.setdefault(depot_code(number), []).append(number)
        assert pairs
        for code, numbers in pairs.items():
            refs = {network.station_of(number) for number in numbers}
            assert refs == {StationRef(StationKind.DEPOT, code)}

    def test_depot_names_drop_the_direction_suffix(self, resolved):
        network, _, _, _ = resolved
        depots = [info for ref, info in network.stations.items() if ref.kind is StationKind.DEPOT]
        assert depots
        for info in depots:
            assert not info.name.endswith("Einsetzen")
            assert not info.name.endswith("Aussetzen")
            assert not info.name_short.endswith(" E")
            assert not info.name_short.endswith(" A")

    def test_only_hst_points_are_stops(self, corpus, resolved):
        network, _, _, _ = resolved
        for number, netzpunkt in corpus.netzpunkte.items():
            assert network.grid_points[number].is_stop == (netzpunkt.netzpunkttyp == NetzpunktNetzpunkttyp.HST)


class TestRoutes:
    def test_every_route_has_at_least_two_stations(self, resolved):
        _, routes, _, _ = resolved
        assert routes.routes
        for route in routes.routes.values():
            assert len(route.stops) >= 2
            assert route.name
            assert route.name_short

    def test_elapsed_distance_strictly_increases(self, resolved):
        _, routes, _, _ = resolved
        for route in routes.routes.values():
            for previous, stop in zip(route.stops, route.stops[1:]):
                assert stop.elapsed_distance_m > previous.elapsed_distance_m
            assert route.distance_m == route.stops[-1].elapsed_distance_m

    def test_bpunkt_bracket_is_folded_away(self, corpus, resolved):
        """
        An in-service route is bracketed by the layover position at each end. Because a
        BPunkt and its Hst twin are the same station, the bracket must not survive into
        the output as a station of its own.
        """
        network, routes, _, _ = resolved
        folded = 0
        for route_id, shape in routes.shape_of.items():
            if shape is None:
                continue
            points = shape.points
            if len(points) < 3:
                continue
            if corpus.netzpunkte[points[0]].netzpunkttyp != NetzpunktNetzpunkttyp.BPUNKT:
                continue
            if corpus.netzpunkte[points[1]].netzpunkttyp != NetzpunktNetzpunkttyp.HST:
                continue
            resolved_route = routes.routes[shape]
            assert len(resolved_route.stops) < len(points)
            folded += 1
        assert folded > 0

    def test_identical_shapes_share_one_route(self, resolved):
        _, routes, _, _ = resolved
        shapes = [shape for shape in routes.shape_of.values() if shape is not None]
        assert len(shapes) > len(set(shapes)), "the samples should contain a repeated route"
        for shape in set(shapes):
            assert shape in routes.routes

    def test_headsigns_are_read(self, resolved):
        """Route.headsign used to be unconditionally None; Zielanzeigen carry it."""
        _, routes, _, _ = resolved
        assert any(route.headsign for route in routes.routes.values())

    @pytest.mark.parametrize("kind", [NetzpunktNetzpunkttyp.EPKT, NetzpunktNetzpunkttyp.APKT])
    def test_zero_duration_depot_leg_gets_a_duration_from_its_length(self, kind):
        """
        The export models a depot leg it has no driving time for as instantaneous — the
        pull-out's Startzeit is the moment the service trip it feeds departs, and the
        vehicle teleports out of the depot. 838 Einsetzfahrten in Berlin 2025-06 and every
        depot leg in the 2026 export look like that; the 2023 samples do not, so the
        driving times are stripped here.

        The end pinned by the neighbouring trip must stay put and the other end must move:
        a pull-out departs earlier, a pull-in arrives later.
        """
        path = sample_paths()[0]
        doc = load_and_validate_xml(path)

        def is_target(route) -> bool:
            points = [p.netzpunkt for p in route.punktfolge.punkt]
            netzpunkte = {n.nummer: n for n in doc.streckennetz_daten.netzpunkte.netzpunkt}
            end = points[0] if kind is NetzpunktNetzpunkttyp.EPKT else points[-1]
            return netzpunkte[end].netzpunkttyp is kind

        route = next(r for r in doc.linien_daten.linie.routen_daten.route if is_target(r))
        for profil in route.fahrzeitprofile.fahrzeitprofil:
            for punkt in profil.fahrzeitprofilpunkte.punkt:
                punkt.streckenfahrzeit = 0
                punkt.wartezeit = 0

        corpus = merge_corpus([RawFile.from_document(path, doc)])
        routes = resolve_routes(corpus, build_network(corpus))
        resolved_route = routes.routes[routes.shape_of[(0, route.lfd_nr)]]
        expected = max(1, math.ceil(resolved_route.distance_m / (DEPOT_LEG_SPEED_KMH / 3.6)))
        assert expected > 0

        profiles = [p for (rid, _n), p in routes.profiles.items() if rid == (0, route.lfd_nr)]
        assert profiles
        for profile in profiles:
            if kind is NetzpunktNetzpunkttyp.EPKT:
                assert profile.departure_offset_s == -expected
                assert profile.arrival_offset_s == 0, "the pull-out's arrival is pinned by the trip it feeds"
            else:
                assert profile.departure_offset_s == 0, "the pull-in's departure is pinned by the trip that feeds it"
                assert profile.arrival_offset_s == expected

    def test_missing_strecke_is_estimated_from_the_coordinates(self, corpus):
        """A hop the corpus has no Strecke for falls back to crow-fly × detour factor."""
        network = build_network(corpus)
        before = resolve_routes(corpus, network)
        route_id, shape = next((rid, s) for rid, s in before.shape_of.items() if s is not None and len(s.points) > 4)
        original_distance = before.routes[shape].distance_m

        # A hop in the middle of the run, so that neither endpoint folds into a neighbour.
        start, end = shape.points[2], shape.points[3]
        removed = network.segment_length.pop((start, end))

        first, last = network.grid_points[start], network.grid_points[end]
        crow_fly_m = math.hypot(first.x - last.x, first.y - last.y) / 1000.0
        expected = max(CROW_FLY_DETOUR_FACTOR * crow_fly_m, 1.0)

        rebuilt = resolve_routes(corpus, network)
        route = rebuilt.routes[rebuilt.shape_of[route_id]]
        assert route.distance_estimated
        assert route.name.startswith("CHECK DISTANCE: ")
        assert route.distance_m == pytest.approx(original_distance - removed + expected, rel=1e-6)

    def test_impossibly_short_depot_leg_is_rejected_and_estimated(self, corpus):
        """
        The export records a flat 60 m for every depot connection in the 2026 UGFPL data,
        for legs whose endpoints its own coordinates place up to 21.5 km apart. Taken at
        face value that becomes a nine-second pull-out for an hour-long deadhead, with no
        CHECK DISTANCE marker to warn anyone, because a length *was* supplied.

        A road cannot be shorter than the straight line between its ends, so such a length
        is rejected and estimated like a missing one.
        """
        network = build_network(corpus)
        route_id, shape = next(
            (rid, s)
            for rid, s in resolve_routes(corpus, network).shape_of.items()
            if s is not None
            and len(s.points) == 2
            and (network.grid_points[s.points[0]].is_depot or network.grid_points[s.points[1]].is_depot)
        )
        start, end = shape.points
        crow_fly_m = (
            math.hypot(
                network.grid_points[start].x - network.grid_points[end].x,
                network.grid_points[start].y - network.grid_points[end].y,
            )
            / 1000.0
        )
        assert crow_fly_m > 1000, "need a depot leg long enough for 60 m to be absurd"

        network.segment_length[(start, end)] = 60
        route = resolve_routes(corpus, network).routes[shape]

        assert route.distance_m == pytest.approx(CROW_FLY_DETOUR_FACTOR * crow_fly_m)
        assert route.distance_estimated
        assert route.name.startswith("CHECK DISTANCE: ")

    def test_plausible_depot_leg_is_left_alone(self, corpus):
        """The rejection must not fire on a length that is merely on the short side."""
        network = build_network(corpus)
        route_id, shape = next(
            (rid, s)
            for rid, s in resolve_routes(corpus, network).shape_of.items()
            if s is not None
            and len(s.points) == 2
            and (network.grid_points[s.points[0]].is_depot or network.grid_points[s.points[1]].is_depot)
        )
        start, end = shape.points
        crow_fly_m = (
            math.hypot(
                network.grid_points[start].x - network.grid_points[end].x,
                network.grid_points[start].y - network.grid_points[end].y,
            )
            / 1000.0
        )

        # Just inside the tolerance: shorter than the straight line, but only slightly, as
        # coordinate noise on a real leg can make it.
        network.segment_length[(start, end)] = int(0.9 * crow_fly_m)
        route = resolve_routes(corpus, network).routes[shape]

        assert route.distance_m == pytest.approx(int(0.9 * crow_fly_m))
        assert not route.distance_estimated

    def test_route_with_no_distance_at_all_is_estimated_end_to_end(self, corpus):
        network = build_network(corpus)
        routes = resolve_routes(corpus, network)
        route_id, shape = next((rid, s) for rid, s in routes.shape_of.items() if s is not None and len(s.points) == 2)
        network.segment_length[(shape.points[0], shape.points[1])] = 0

        first, last = network.grid_points[shape.points[0]], network.grid_points[shape.points[1]]
        crow_fly_m = math.hypot(first.x - last.x, first.y - last.y) / 1000.0
        expected = max(CROW_FLY_DETOUR_FACTOR * crow_fly_m, MIN_ESTIMATED_DISTANCE_M)

        rebuilt = resolve_routes(corpus, network)
        route = rebuilt.routes[rebuilt.shape_of[route_id]]
        assert route.distance_m == pytest.approx(expected)
        assert route.name.startswith("CHECK DISTANCE: ")


class TestCollapsedPunktfolge:
    """
    Some exports collapse a route's Punktfolge to the endpoints of the run while the
    Fahrzeitprofil still lists every stop. 13 of 490 routes in the 2026 UGFPL export are
    like this and none at all in Berlin 2025-06, so the samples are collapsed by hand here.
    """

    @staticmethod
    def _collapse(doc: Linienfahrplan, index: int) -> int:
        strecken = {s.id: s for s in doc.streckennetz_daten.strecken.strecke}
        routes = doc.linien_daten.linie.routen_daten.route
        route = routes[index]
        points = route.punktfolge.punkt[:2] + route.punktfolge.punkt[-2:]
        kept = {
            (points[0].netzpunkt, points[1].netzpunkt),
            (points[2].netzpunkt, points[3].netzpunkt),
        }
        routes[index] = replace(
            route,
            punktfolge=replace(route.punktfolge, punkt=points),
            streckenfolge=replace(
                route.streckenfolge,
                strecke=[
                    s
                    for s in route.streckenfolge.strecke
                    if (strecken[s.strecken_id].startpunkt, strecken[s.strecken_id].endpunkt) in kept
                ],
            ),
        )
        return route.lfd_nr

    @staticmethod
    def _long_route_index(doc: Linienfahrplan) -> int:
        return next(i for i, r in enumerate(doc.linien_daten.linie.routen_daten.route) if len(r.punktfolge.punkt) > 4)

    def test_collapsed_route_is_reconstructed(self):
        path = sample_paths()[0]
        pristine_doc = load_and_validate_xml(path)
        collapsed_doc = load_and_validate_xml(path)
        index = self._long_route_index(collapsed_doc)
        expected_points = tuple(
            p.netzpunkt for p in pristine_doc.linien_daten.linie.routen_daten.route[index].punktfolge.punkt
        )
        self._collapse(collapsed_doc, index)

        corpus = merge_corpus([RawFile.from_document(path, collapsed_doc)])
        network = build_network(corpus)
        routes = resolve_routes(corpus, network)

        route_id = (0, collapsed_doc.linien_daten.linie.routen_daten.route[index].lfd_nr)
        assert routes.shape_of[route_id].points == expected_points
        assert routes.n_reconstructed >= 1

    def test_unfinished_search_does_not_reconstruct(self, monkeypatch):
        """A walk that only a truncated search found is not known to be the only one."""
        monkeypatch.setattr("eflips.ingest.bvgxml._routes.MAX_RECONSTRUCTION_SEARCH_STEPS", 10)

        path = sample_paths()[0]
        doc = load_and_validate_xml(path)
        index = self._long_route_index(doc)
        lfd_nr = self._collapse(doc, index)

        corpus = merge_corpus([RawFile.from_document(path, doc)])
        routes = resolve_routes(corpus, build_network(corpus))
        assert len(routes.shape_of[(0, lfd_nr)].points) == 4
        assert routes.n_reconstruction_failed >= 1

    def test_unreconstructable_route_still_arrives_at_the_same_time(self):
        """
        Without the network to restore the omitted stops, the collapsed leg is estimated —
        but the vehicle must still reach the end of the route at the same moment, or a 32
        minute run silently becomes a 2 minute one.

        Only the *arrival* is compared. A collapsed route may well depart earlier, because
        its first stop can be an earlier point of the same station than the pristine route's
        is: line 125 approaches Holzhauser Str./Schubartstr. via three grid points, and
        dropping the third moves the route's first stop 60 s earlier without moving the
        vehicle.
        """
        path = sample_paths()[0]
        pristine_doc = load_and_validate_xml(path)
        collapsed_doc = load_and_validate_xml(path)
        index = self._long_route_index(collapsed_doc)
        lfd_nr = self._collapse(collapsed_doc, index)

        # Take the network of the collapsed leg away, leaving only what the routes still
        # reference, so that the omitted points cannot be restored.
        kept = {
            s.strecken_id for r in collapsed_doc.linien_daten.linie.routen_daten.route for s in r.streckenfolge.strecke
        }
        collapsed_doc.streckennetz_daten.strecken.strecke = [
            s for s in collapsed_doc.streckennetz_daten.strecken.strecke if s.id in kept
        ]

        arrivals = {}
        for name, doc in (("pristine", pristine_doc), ("collapsed", collapsed_doc)):
            corpus = merge_corpus([RawFile.from_document(path, doc)])
            routes = resolve_routes(corpus, build_network(corpus))
            arrivals[name] = {
                number: profile.arrival_offset_s
                for (route_id, number), profile in routes.profiles.items()
                if route_id == (0, lfd_nr)
            }
            if name == "collapsed":
                assert len(routes.shape_of[(0, lfd_nr)].points) == 4
                assert routes.routes[routes.shape_of[(0, lfd_nr)]].distance_estimated

        assert arrivals["collapsed"] == arrivals["pristine"]
        assert min(arrivals["pristine"].values()) > 15 * 60, "the sample route is a long one"


class TestSpreadArrivals:
    """
    Every driving and waiting time in the export is a whole minute, so stops sharing an
    arrival offset are routine. They are spread across the time actually available rather
    than nudged a second apart.
    """

    def test_untouched_when_already_strictly_increasing(self):
        stops = [StopTiming(0, 0), StopTiming(60, 0), StopTiming(180, 0)]
        assert spread_arrivals(stops) == stops

    def test_run_is_spread_evenly_into_the_following_gap(self):
        stops = [StopTiming(0, 0), StopTiming(60, 0), StopTiming(60, 0), StopTiming(60, 0), StopTiming(120, 0)]
        result = spread_arrivals(stops)
        assert [s.arrival_offset_s for s in result] == [0, 60, 80, 100, 120]
        assert all(a.arrival_offset_s < b.arrival_offset_s for a, b in zip(result, result[1:]))

    def test_trailing_run_holds_the_trip_end_and_spreads_backwards(self):
        stops = [StopTiming(0, 0), StopTiming(120, 0), StopTiming(120, 0), StopTiming(120, 0)]
        result = spread_arrivals(stops)
        assert result[-1].arrival_offset_s == 120, "the trip's own arrival time must not move"
        assert result[0].arrival_offset_s == 0, "the trip's own departure time must not move"
        assert all(a.arrival_offset_s < b.arrival_offset_s for a, b in zip(result, result[1:]))

    def test_dwell_is_clamped_to_the_gap(self):
        stops = [StopTiming(0, 300), StopTiming(60, 0), StopTiming(120, 0)]
        result = spread_arrivals(stops)
        assert result[0].departure_offset_s < result[1].arrival_offset_s

    def test_impossible_run_raises(self):
        stops = [StopTiming(0, 0)] + [StopTiming(3, 0)] * 5 + [StopTiming(6, 0)]
        with pytest.raises(ValueError, match="available to spread them over"):
            spread_arrivals(stops)

    def test_a_short_multi_stop_route_with_no_driving_time_still_resolves(self):
        """
        A route the export carries no driving time for has *every* stop at the same offset,
        and the duration derived from its length has to give each of them a distinct second.
        Deriving it from the length alone does not: a metre-long route yields one second,
        and :func:`spread_arrivals` then raised — aborting the whole import and blaming the
        export for a contradiction introduced one step earlier.

        Every zero-duration route in the three reference corpora happens to have two stops
        (1,081 of them in Berlin 2025-06), so the floor was never reached there. Here the
        two conditions are forced onto one long route: no driving time, and one metre per
        hop so the length contributes nothing.
        """
        path = sample_paths()[0]
        doc = load_and_validate_xml(path)
        for strecke in doc.streckennetz_daten.strecken.strecke:
            strecke.streckenlaenge = 1
        route = max(doc.linien_daten.linie.routen_daten.route, key=lambda r: len(r.punktfolge.punkt))
        for profil in route.fahrzeitprofile.fahrzeitprofil:
            for punkt in profil.fahrzeitprofilpunkte.punkt:
                punkt.streckenfahrzeit = 0
                punkt.wartezeit = 0

        corpus = merge_corpus([RawFile.from_document(path, doc)])
        routes = resolve_routes(corpus, build_network(corpus))

        route_id = (0, route.lfd_nr)
        # resolve_routes() above must not have raised: a route the export carries no driving
        # time for is ordinary input, not the self-contradiction that aborts an import.
        resolved_route = routes.routes[routes.shape_of[route_id]]
        assert len(resolved_route.stops) > 2, "the point of this test is a route with many stops"

        profiles = [p for (rid, _n), p in routes.profiles.items() if rid == route_id]
        assert profiles
        for profile in profiles:
            offsets = [stop.arrival_offset_s for stop in profile.stops]
            assert all(b > a for a, b in zip(offsets, offsets[1:]))
            assert len(offsets) == len(resolved_route.stops)


class TestRotations:
    def test_rotations_are_keyed_by_their_umlauf_group(self, corpus, resolved):
        _, _, rotations, _ = resolved
        assert rotations.rotations
        seen = set()
        for key in rotations.rotations:
            for member in key:
                assert member not in seen, "an Umlauf may belong to only one rotation"
                seen.add(member)

    def test_complete_rotations_run_depot_to_depot(self, resolved):
        """
        The structural completeness test subsumes the depot-name heuristic it replaces:
        every rotation whose parts all carry trips starts at an EPkt and ends at an APkt.
        """
        _, _, _, schedule = resolved
        assert schedule.rotations
        for rotation in schedule.rotations:
            assert first_and_last_are_depots(rotation), rotation.name

    def test_truncation_causes_are_named(self, resolved):
        _, _, rotations, _ = resolved
        assert rotations.truncated
        for rotation in rotations.truncated:
            assert rotation.missing
            for segment in rotation.missing:
                assert segment.cause in TruncationCause
        # The samples are three unrelated lines, so most rotations reach into files that
        # were not supplied.
        assert TruncationCause.LINE_NOT_SUPPLIED in rotations.truncation_summary()
        assert rotations.missing_lines()

    def test_trips_are_ordered_by_day_then_segment_then_position(self, resolved):
        _, _, rotations, _ = resolved
        for rotation in rotations.rotations.values():
            if not rotation.segments:
                continue
            expected = [
                fahrt_id
                for segment in sorted(rotation.segments, key=lambda s: (s.day, s.beginn_s, s.key))
                for fahrt_id in segment.fahrt_ids
            ]
            assert rotation.fahrt_ids() == expected


class TestSchedule:
    def test_rotations_are_continuous_and_do_not_overlap(self, resolved):
        _, _, _, schedule = resolved
        assert schedule.report.n_discontinuities == 0, schedule.report.discontinuities
        assert schedule.report.n_overlaps == 0, schedule.report.overlaps
        for rotation in schedule.rotations:
            for current, following in zip(rotation.trips, rotation.trips[1:]):
                assert current.stops[-1].station == following.stops[0].station
                assert current.arrival <= following.departure

    def test_trip_type_comes_from_fahrtart(self, corpus, resolved):
        """
        The previous implementation derived this by looking for 'Einsetzfahrt' in a route
        name it had generated itself, which classified every Betriebsfahrt as a passenger
        trip.
        """
        _, _, _, schedule = resolved
        seen_deadhead = seen_passenger = False
        for rotation in schedule.rotations:
            for trip in rotation.trips:
                fahrtart = corpus.fahrten[trip.fahrt_id].fahrtart
                assert trip.is_deadhead == (fahrtart in DEADHEAD_FAHRTARTEN)
                seen_deadhead |= trip.is_deadhead
                seen_passenger |= not trip.is_deadhead
        assert seen_deadhead and seen_passenger

    def test_deadhead_matches_fahrgastrelevant(self, corpus, resolved):
        """Fahrtart in {E, A, B} and fahrgastrelevant == N are the same predicate."""
        for fahrt in corpus.fahrten.values():
            assert (fahrt.fahrtart in DEADHEAD_FAHRTARTEN) == (fahrt.fahrgastrelevant.value == "N")

    def test_every_trip_of_a_complete_rotation_is_imported(self, resolved):
        _, routes, rotations, schedule = resolved
        expected = sum(len(r.fahrt_ids()) for r in rotations.complete)
        assert schedule.report.n_trips == expected - schedule.report.n_trips_on_degenerate_routes

    def test_stop_times_align_with_the_route(self, resolved):
        _, _, _, schedule = resolved
        for rotation in schedule.rotations:
            for trip in rotation.trips:
                assert len(trip.stops) == len(trip.route.stops)
                for stop, route_stop in zip(trip.stops, trip.route.stops):
                    assert stop.station == route_stop.station
                assert trip.arrival > trip.departure

    def test_estimated_distance_count_matches_the_routes_written(self, resolved):
        """
        The summary tells the reader to look these up by name, so the number has to be the
        number they will find. Several input routes resolve to one written route — the
        export repeats a route once per file its line appears in — so counting input routes
        would overstate it (64 against 19 on the Berlin 2025-06 corpus).
        """
        _, _, _, schedule = resolved
        written = sum(1 for route in schedule.routes if route.distance_estimated)
        assert schedule.report.n_estimated_distance == written
        assert written == sum(1 for route in schedule.routes if route.name.startswith("CHECK DISTANCE: "))

    def test_summary_accounts_for_dropped_trips(self, resolved):
        _, _, _, schedule = resolved
        summary = schedule.report.summary()
        assert "rotations" in summary
        assert "were not imported" in summary


class TestNetworkGeoms:
    def test_prefetch_resolves_everything_in_one_batch(self, corpus, resolved, monkeypatch):
        """After prefetch_geoms, geom_of_point / geom_of_station never look a point up on its own."""
        _, _, _, schedule = resolved
        network = build_network(corpus)
        calls = []

        def fake_many(xys):
            xys = list(xys)
            calls.append(xys)
            return {xy: f"SRID=4326;POINTZ({xy[0]} {xy[1]} 0)" for xy in xys}

        def no_single_lookup(x, y):  # pragma: no cover
            raise AssertionError(f"single lookup for {(x, y)} after prefetch")

        monkeypatch.setattr("eflips.ingest.bvgxml._network.soldner_to_pointz_many", fake_many)
        monkeypatch.setattr("eflips.ingest.bvgxml._network.soldner_to_pointz", no_single_lookup)

        point_numbers = {stop.grid_point for route in schedule.routes for stop in route.stops}
        network.prefetch_geoms(point_numbers, schedule.stations)
        assert len(calls) == 1
        assert len(calls[0]) == len(set(calls[0])), "the batch must be de-duplicated"

        for number in point_numbers:
            assert network.geom_of_point(number).startswith("SRID=4326;POINTZ(")
        for ref in schedule.stations:
            assert network.geom_of_station(ref).startswith("SRID=4326;POINTZ(")
        assert len(calls) == 1

    def test_geom_without_prefetch_falls_back_to_single_lookup(self, corpus, resolved, monkeypatch):
        _, _, _, schedule = resolved
        network = build_network(corpus)
        monkeypatch.setattr(
            "eflips.ingest.bvgxml._network.soldner_to_pointz", lambda x, y: f"SRID=4326;POINTZ({x} {y} 1)"
        )
        number = next(iter(stop.grid_point for route in schedule.routes for stop in route.stops))
        assert network.geom_of_point(number).endswith(" 1)")


class TestBvgxmlIngester(BaseIngester):
    @pytest.fixture(autouse=True)
    def disable_altitude_lookups(self, monkeypatch) -> None:
        """Bypass network altitude lookups for all tests in this class."""
        monkeypatch.setenv("ELEVATION_DUMMY_MODE", "True")

    @pytest.fixture()
    def ingester(self) -> BvgxmlIngester:
        return BvgxmlIngester(self.database_url)

    @pytest.fixture()
    def sample_xml_paths(self) -> List[Path]:
        return sample_paths()

    @pytest.fixture()
    def bvg_zip_file(self, tmp_path, sample_xml_paths) -> Path:
        """Bundle every sample XML into a zip and return its path."""
        zip_path = tmp_path / f"{uuid4()}.zip"
        with ZipFile(zip_path, "w") as zf:
            for xml in sample_xml_paths:
                zf.write(xml, arcname=xml.name)
        return zip_path

    @pytest.fixture()
    def single_xml_zip(self, tmp_path, sample_xml_paths) -> Path:
        """A zip containing only one XML file, for faster end-to-end tests."""
        zip_path = tmp_path / f"{uuid4()}.zip"
        with ZipFile(zip_path, "w") as zf:
            zf.write(sample_xml_paths[0], arcname=sample_xml_paths[0].name)
        return zip_path

    def test_prepare(self, ingester, bvg_zip_file) -> None:
        progress_values: List[float] = []
        success, result = ingester.prepare(
            xml_zip_file=bvg_zip_file,
            progress_callback=progress_values.append,
        )
        assert success is True
        assert isinstance(result, UUID)
        assert (ingester.path_for_uuid(result) / "schedules.pkl").is_file()
        # The parsed documents live in the pickle and ingest() never reopens the XML, so
        # the extracted copy must not be left behind — it is 862 MB for a full-city import.
        assert not (ingester.path_for_uuid(result) / "xml").exists()
        assert progress_values
        assert progress_values[-1] == pytest.approx(1.0)
        assert all(0.0 <= p <= 1.0 for p in progress_values)

    def test_prepare_rejects_non_path(self, ingester) -> None:
        success, errors = ingester.prepare(xml_zip_file="not-a-path")  # type: ignore[arg-type]
        assert success is False
        assert isinstance(errors, dict)
        assert "xml_zip_file" in errors

    def test_prepare_rejects_missing_file(self, ingester, tmp_path) -> None:
        success, errors = ingester.prepare(xml_zip_file=tmp_path / "does_not_exist.zip")
        assert success is False
        assert isinstance(errors, dict)
        assert "xml_zip_file" in errors

    def test_prepare_rejects_wrong_extension(self, ingester, tmp_path) -> None:
        not_a_zip = tmp_path / "data.txt"
        not_a_zip.write_text("hello")
        success, errors = ingester.prepare(xml_zip_file=not_a_zip)
        assert success is False
        assert isinstance(errors, dict)
        assert "xml_zip_file" in errors

    def test_prepare_rejects_corrupt_zip(self, ingester, tmp_path) -> None:
        bad_zip = tmp_path / "bad.zip"
        bad_zip.write_bytes(b"this is not a zip file")
        success, errors = ingester.prepare(xml_zip_file=bad_zip)
        assert success is False
        assert isinstance(errors, dict)
        assert "xml_zip_file" in errors

    def test_prepare_rejects_empty_zip(self, ingester, tmp_path) -> None:
        empty_zip = tmp_path / "empty.zip"
        with ZipFile(empty_zip, "w") as zf:
            zf.writestr("readme.txt", "no xml here")
        success, errors = ingester.prepare(xml_zip_file=empty_zip)
        assert success is False
        assert isinstance(errors, dict)
        assert "xml_zip_file" in errors

    def test_prepare_names_an_inner_zip(self, ingester, tmp_path, sample_xml_paths) -> None:
        """
        Export dumps arrive as a zip of a zip (the 2023 BVG dump is one). Unpacking it for
        the user would mean extracting whatever they handed us, so the message has to be
        specific enough that they can fix it themselves.
        """
        inner = tmp_path / "inner.zip"
        with ZipFile(inner, "w") as zf:
            zf.write(sample_xml_paths[0], arcname=sample_xml_paths[0].name)
        outer = tmp_path / "outer.zip"
        with ZipFile(outer, "w") as zf:
            zf.write(inner, arcname="passengerCount_BO_2023-07-03.zip")
            zf.writestr("notes.txt", "not xml")

        success, errors = ingester.prepare(xml_zip_file=outer)
        assert success is False
        assert isinstance(errors, dict)
        assert "passengerCount_BO_2023-07-03.zip" in errors["xml_zip_file"]
        assert "Unpack the inner archive" in errors["xml_zip_file"]

    def test_prepare_reports_xml_errors(self, ingester, tmp_path) -> None:
        """A zip with nothing readable in it is an error, and names the files."""
        broken_zip = tmp_path / "broken.zip"
        with ZipFile(broken_zip, "w") as zf:
            zf.writestr("broken.xml", "<not valid xml")
        success, errors = ingester.prepare(xml_zip_file=broken_zip)
        assert success is False
        assert isinstance(errors, dict)
        assert "broken.xml" in errors

    def test_prepare_rejects_a_zip_of_only_no_data_answers(self, ingester, tmp_path) -> None:
        zip_path = tmp_path / "no_data.zip"
        with ZipFile(zip_path, "w") as zf:
            for index in range(3):
                zf.writestr(f"line_{index}.xml", NO_DATA_RESPONSE)
        success, errors = ingester.prepare(xml_zip_file=zip_path)
        assert success is False
        assert isinstance(errors, dict)
        assert "carry any data" in errors["xml_zip_file"]

    def test_prepare_skips_unusable_files_and_keeps_the_rest(self, ingester, sample_xml_paths, tmp_path) -> None:
        """
        Real export dumps contain both "no data" answers and corrupt files. Neither may cost
        the user the rest of the archive — before this, one bad file in 1,400 rejected the
        whole import.
        """
        zip_path = tmp_path / "mixed.zip"
        with ZipFile(zip_path, "w") as zf:
            zf.write(sample_xml_paths[0], arcname=sample_xml_paths[0].name)
            zf.writestr("no_data.xml", NO_DATA_RESPONSE)
            zf.writestr("corrupt.xml", "<Linienfahrplan><Generierung>")
        success, uuid = ingester.prepare(xml_zip_file=zip_path)
        assert success is True
        assert isinstance(uuid, UUID)

        BvgxmlIngester(self.database_url).ingest(uuid)
        engine = create_engine(self.database_url)
        with Session(engine) as session:
            scenario = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id == uuid).one()
            assert session.query(eflips.model.Trip).filter(eflips.model.Trip.scenario_id == scenario.id).count() > 0

    def test_ingest_report_names_the_files_it_could_not_read(self, ingester, sample_xml_paths, tmp_path) -> None:
        """Skipping a corrupt file must be loud, not silent: it is data the user has lost."""
        zip_path = tmp_path / "mixed.zip"
        with ZipFile(zip_path, "w") as zf:
            zf.write(sample_xml_paths[0], arcname=sample_xml_paths[0].name)
            zf.writestr("no_data.xml", NO_DATA_RESPONSE)
            zf.writestr("corrupt.xml", "<Linienfahrplan><Generierung>")
        success, uuid = ingester.prepare(xml_zip_file=zip_path)
        assert success is True

        messages: List[str] = []
        handler = logging.Handler()
        handler.emit = lambda record: messages.append(record.getMessage())  # type: ignore[method-assign]
        logger = logging.getLogger("eflips.ingest.bvgxml")
        logger.addHandler(handler)
        previous_level = logger.level
        logger.setLevel(logging.INFO)
        # eflips.model.setup_database() runs Alembic, whose env.py calls fileConfig() with
        # the default disable_existing_loggers=True. That sets disabled=True on every logger
        # that already exists, this package's included, so anything logged afterwards is
        # dropped. The setup_database fixture has already run by now; undo it here.
        disabled = [
            logging.getLogger(name)
            for name in list(logging.root.manager.loggerDict)
            if name.startswith("eflips.ingest.")
        ]
        for entry in disabled:
            entry.disabled = False
        try:
            BvgxmlIngester(self.database_url).ingest(uuid)
        finally:
            logger.removeHandler(handler)
            logger.setLevel(previous_level)

        summary = "\n".join(messages)
        assert "corrupt.xml" in summary
        assert "1 input files carry no timetable" in summary

    def test_prepare_reports_contradicting_files(self, ingester, tmp_path, sample_xml_paths) -> None:
        """Files that contradict each other must fail in prepare(), not half-way through a write."""
        clashing_zip = tmp_path / "clash.zip"
        with ZipFile(clashing_zip, "w") as zf:
            zf.write(sample_xml_paths[0], arcname="a.xml")
            zf.write(sample_xml_paths[0], arcname="b.xml")
        success, errors = ingester.prepare(xml_zip_file=clashing_zip)
        assert success is False
        assert isinstance(errors, dict)
        assert "globally unique" in errors["xml_zip_file"]

    def test_ingest(self, ingester, single_xml_zip) -> None:
        progress_values: List[float] = []
        success, uuid = ingester.prepare(xml_zip_file=single_xml_zip)
        assert success is True
        assert isinstance(uuid, UUID)

        # Use a fresh ingester instance as documented in BaseIngester.test_ingest.
        BvgxmlIngester(self.database_url).ingest(uuid, progress_callback=progress_values.append)

        assert progress_values
        assert progress_values[-1] == pytest.approx(1.0)

        engine = create_engine(self.database_url)
        with Session(engine) as session:
            scenario = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id == uuid).one_or_none()
            assert scenario is not None, "ingest() should create a Scenario for the UUID"
            routes = session.query(eflips.model.Route).filter(eflips.model.Route.scenario_id == scenario.id).all()
            trips = session.query(eflips.model.Trip).filter(eflips.model.Trip.scenario_id == scenario.id).all()
            assert routes
            assert trips

            for route in routes:
                assert route.name
                assert route.name_short
                assert route.distance > 0
                assert route.assoc_route_stations
                for assoc in route.assoc_route_stations:
                    assert assoc.station is not None
                    assert assoc.location is not None

            for station in session.query(eflips.model.Station).filter(eflips.model.Station.scenario_id == scenario.id):
                assert station.name
                assert station.name_short
                assert station.geom is not None

            assert any(route.headsign for route in routes), "Zielanzeigen carry a headsign"
            assert any(trip.trip_type == eflips.model.TripType.EMPTY for trip in trips)
            assert any(trip.trip_type == eflips.model.TripType.PASSENGER for trip in trips)

            for trip in trips:
                assert trip.arrival_time > trip.departure_time
                assert trip.stop_times

    def test_ingested_rotations_are_depot_to_depot_and_continuous(self, ingester, single_xml_zip) -> None:
        success, uuid = ingester.prepare(xml_zip_file=single_xml_zip)
        assert success is True
        BvgxmlIngester(self.database_url).ingest(uuid)

        engine = create_engine(self.database_url)
        with Session(engine) as session:
            scenario = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id == uuid).one()
            rotations = (
                session.query(eflips.model.Rotation).filter(eflips.model.Rotation.scenario_id == scenario.id).all()
            )
            assert rotations
            for rotation in rotations:
                trips = sorted(rotation.trips, key=lambda t: t.departure_time)
                assert trips
                for current, following in zip(trips, trips[1:]):
                    assert current.route.arrival_station_id == following.route.departure_station_id
                    assert current.arrival_time <= following.departure_time

    def test_ingest_without_prepare_raises(self, ingester) -> None:
        with pytest.raises(ValueError, match="No prepared data found"):
            ingester.ingest(uuid4())

    def test_ingest_twice_same_data_independent_scenarios(self, ingester, single_xml_zip) -> None:
        """
        Ingesters must be re-runnable any number of times, each invocation yielding a fresh,
        independent scenario without colliding with rows written by earlier runs.
        """
        success_a, uuid_a = ingester.prepare(xml_zip_file=single_xml_zip)
        assert success_a is True
        assert isinstance(uuid_a, UUID)
        BvgxmlIngester(self.database_url).ingest(uuid_a)

        success_b, uuid_b = ingester.prepare(xml_zip_file=single_xml_zip)
        assert success_b is True
        assert isinstance(uuid_b, UUID)
        assert uuid_b != uuid_a
        BvgxmlIngester(self.database_url).ingest(uuid_b)

        engine = create_engine(self.database_url)
        with Session(engine) as session:
            scenario_a = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id == uuid_a).one()
            scenario_b = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id == uuid_b).one()
            assert scenario_a.id != scenario_b.id

            counts = []
            for scenario in (scenario_a, scenario_b):
                counts.append(
                    tuple(
                        session.query(model).filter(model.scenario_id == scenario.id).count()
                        for model in (
                            eflips.model.Station,
                            eflips.model.Route,
                            eflips.model.Trip,
                            eflips.model.Rotation,
                        )
                    )
                )
            assert all(count > 0 for count in counts[0])
            assert counts[0] == counts[1], "the same input must produce the same output"

    def test_ingest_twice_different_data_independent_scenarios(self, ingester, sample_xml_paths, tmp_path) -> None:
        """
        The realistic operational case: a user ingests one timetable slice, then later a
        different one. Both scenarios must coexist without ID collisions.
        """
        if len(sample_xml_paths) < 2:
            pytest.skip("need at least two sample XML files for this test")

        uuids = []
        for index in (0, 1):
            zip_path = tmp_path / f"{index}_{uuid4()}.zip"
            with ZipFile(zip_path, "w") as zf:
                zf.write(sample_xml_paths[index], arcname=sample_xml_paths[index].name)
            success, uuid = ingester.prepare(xml_zip_file=zip_path)
            assert success is True
            assert isinstance(uuid, UUID)
            BvgxmlIngester(self.database_url).ingest(uuid)
            uuids.append(uuid)

        engine = create_engine(self.database_url)
        with Session(engine) as session:
            scenarios = session.query(eflips.model.Scenario).filter(eflips.model.Scenario.task_id.in_(uuids)).all()
            assert len(scenarios) == 2
            assert scenarios[0].id != scenarios[1].id
