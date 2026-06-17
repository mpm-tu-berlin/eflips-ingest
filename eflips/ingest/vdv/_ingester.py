import codecs
import csv
import glob
import logging
import os
import pickle
import re
from collections import defaultdict
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


def fix_identical_stop_times(stop_times: List[StopTime]) -> None:
    """
    Linearly spread groups of stop times that share an arrival time across ``[T-29s, T+29s]``.

    Two identical times → ``[T-29s, T+29s]``. Three → ``[T-29s, T, T+29s]``. And so on.

    Modifies ``stop_times`` in place. Dwell durations are left untouched.
    """
    groups: Dict[datetime, List[int]] = defaultdict(list)
    for i, st in enumerate(stop_times):
        groups[st.arrival_time].append(i)

    for indices in groups.values():
        n = len(indices)
        if n < 2:
            continue
        span = 2 * _DUPLICATE_SPREAD_HALFWIDTH
        for k, idx in enumerate(indices):
            offset = -_DUPLICATE_SPREAD_HALFWIDTH + (k / (n - 1)) * span
            stop_times[idx].arrival_time += offset


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

            # If every entry shares a single top-level folder prefix, strip it so files land at the root of out_dir.
            if names and all("/" in n for n in names) and len({n.split("/", 1)[0] for n in names}) == 1:
                cut = len(names[0].split("/", 1)[0]) + 1
                for m in members:
                    m.filename = m.filename[cut:]

            zip_file.extractall(out_dir, [m for m in members if m.filename])

        try:
            all_tables = validate_input_data_vdv_451(out_dir)
        except ValueError as e:
            return False, {"validation": str(e)}

        with open(out_dir / "all_tables.pkl", "wb") as fp:
            pickle.dump(all_tables, fp)

        return True, uuid

    def ingest(self, uuid: UUID, progress_callback: None | Callable[[float], None] = None) -> None:
        logger = logging.getLogger(__name__)

        temp_dir = self.path_for_uuid(uuid)
        all_tables_file = Path(temp_dir) / "all_tables.pkl"
        with open(all_tables_file, "rb") as fp:
            all_tables = pickle.load(fp)

        all_data: Dict[VDV_Table_Name, List[VdvBaseObject]] = {}
        for tbl in all_tables:
            all_data[tbl] = import_vdv452_table_records(all_tables[tbl])

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

                rotations_by_vdv_pk_and_date: Dict[Tuple[int, int, int, date], Rotation] = {}
                tz = pytz.timezone("Europe/Berlin")

                for rec_frt in rec_frts:
                    route_key = (rec_frt.basis_version, rec_frt.li_nr, rec_frt.str_li_var)
                    if route_key not in routes_by_vdv_pk:
                        logger.debug(f"Skipping trip {rec_frt.frt_fid}: route {route_key} was rejected.")
                        continue
                    route = routes_by_vdv_pk[route_key]
                    route_rec_sels = rec_sels_by_route[route_key]
                    if not route_rec_sels:
                        raise ValueError(f"Trip {rec_frt.frt_fid} references route {route_key} which has no segments.")

                    # REC_FRT_HZT carries per-trip dwell overrides. Real IVU.plan exports emit a row for every
                    # stop with FRT_HZT_ZEIT == 0 even when the exporter only meant to override on the
                    # non-zero ones. Treat that "all zeros" pattern as "no override" so the ORT_HZTF default
                    # applies; otherwise, zeros are taken at face value.
                    trip_rec_frt_hzts = rec_frt_hzts_by_fid.get(rec_frt.frt_fid, [])
                    if trip_rec_frt_hzts and all(h.frt_hzt_zeit == timedelta(0) for h in trip_rec_frt_hzts):
                        trip_rec_frt_hzts = []

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

                    # Walk the route, accumulating arrival times and per-stop dwells.
                    elapsed = rec_frt.frt_start
                    arrival_offsets: List[timedelta] = []
                    dwell_durations: List[timedelta] = []
                    for i, rec_sel in enumerate(route_rec_sels):
                        if i == 0:
                            first_pk = rec_sel.start_station_primary_key
                            arrival_offsets.append(elapsed)
                            first_dwell = resolve_dwell(first_pk)
                            dwell_durations.append(first_dwell)
                            elapsed += first_dwell
                        elapsed += segment_durations[i]
                        arrival_offsets.append(elapsed)
                        next_pk = rec_sel.end_station_primary_key
                        next_dwell = resolve_dwell(next_pk)
                        dwell_durations.append(next_dwell)
                        elapsed += next_dwell

                    # FRT_START and the computed end time are integer-seconds; the epsilon here just guards
                    # against floating-point drift inside timedelta arithmetic.
                    trip_duration = elapsed - rec_frt.frt_start
                    if abs(trip_duration.total_seconds()) < 1.0:
                        # Per MENGE_ONR_TYP: 1=HP (passenger stop), 2=BHOF (depot point),
                        # 6=BP (operational point). IVU.plan emits zero-duration connector trips
                        # that touch a BHOF or BP — e.g. a depot deadhead-out, or a relief handover
                        # at the same physical place under its operational identity. These are
                        # schedule plumbing, not real movements; drop them. A zero-duration trip
                        # between only-HP stops would be genuinely suspect, so still raise.
                        ONR_TYP_HP = 1
                        endpoint_types = {route_rec_sels[0].onr_typ_nr} | {s.sel_ziel_typ for s in route_rec_sels}
                        if endpoint_types - {ONR_TYP_HP}:
                            logger.info(
                                f"Skipping zero-duration non-revenue trip {rec_frt.frt_fid} "
                                f"({stations_in_order[0].name.strip()!r} -> "
                                f"{stations_in_order[-1].name.strip()!r})."
                            )
                            continue
                        raise ValueError(
                            f"Trip {rec_frt.frt_fid} has effectively zero duration "
                            f"({trip_duration.total_seconds():.3f}s)."
                        )

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

                # Drop rotations that ended up with no trips. Snapshot the collection first — deleting from
                # the live collection while iterating can confuse the SQLAlchemy unit of work.
                session.flush()
                for rotation in list(scenario.rotations):
                    if len(rotation.trips) == 0:
                        session.delete(rotation)

                session.commit()
            except Exception:
                session.rollback()
                raise

    @classmethod
    def create_dummy_vehicle_type(cls, scenario: Scenario) -> VehicleType:
        dummy_vehicle_type = VehicleType(
            scenario=scenario,
            name="Dummy Vehicle Type",
            opportunity_charging_capable=False,
            battery_capacity=10000,
            charging_curve=[[0, 1], [1000, 1000]],
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
                raise ValueError(f"The file {abs_file_path} does not contain any records.")

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
_RE_VALID_FORMAT = re.compile(r"(char\[[0-9]+\]|num\[[0-9]+\.[0-9]+\])+")
_RE_CHAR = re.compile(r"char\[[0-9]+\]")
_RE_INT = re.compile(r"num\[[0-9]+\.0\]")
_RE_FLOAT = re.compile(r"num\[[0-9]+\.[0-9]+\]")


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
        if not _RE_VALID_FORMAT.match(part):
            dtypes.append(None)
            logger.warning(
                f"Invalid datatype formatting in VDV 451 file: {part!r} does not match 'char[n]' or "
                "'num[n.n]'. Column will not be imported."
            )
            continue
        if _RE_CHAR.match(part):
            dtypes.append(VDV_Data_Type.CHAR)
        elif _RE_INT.match(part):
            dtypes.append(VDV_Data_Type.INT)
        elif _RE_FLOAT.match(part):
            dtypes.append(VDV_Data_Type.FLOAT)
        else:
            dtypes.append(None)

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
