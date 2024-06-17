import csv
import enum
import glob
import logging
import os
import pickle
import re
from dataclasses import dataclass
from datetime import date, timedelta, datetime, time
from enum import Enum
from pathlib import Path
from typing import Dict, Callable, Tuple, Optional, List
from uuid import UUID, uuid4
from zipfile import ZipFile

import pytz
from eflips.model import VehicleType, Scenario, Rotation, Station, Line, Route, Trip, TripType, StopTime
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
from tqdm.auto import tqdm

from eflips.ingest.base import AbstractIngester
from eflips.ingest.vdv452data import (
    VdvBaseObject,
    BasisVerGueltigkeit,
    Firmenkalender,
    RecOrt,
    MengeFzgTyp,
    RecSel,
    SelFztFeld,
    LidVerlauf,
    RecFrt,
    RecUmlauf,
    RecLid,
    RecFrtHzt,
    OrtHztf,
)


class VDV_Data_Type(enum.Enum):
    """
    An enum for the data types as specified in VDV 451/452. We map the different data types to two main types:
    - CHAR: Character data
    - NUM: Numeric data
    """

    CHAR = "char"
    INT = "num"
    FLOAT = enum.auto


class VDV_Table_Name(enum.Enum):
    """
    An enum for the table names as Specified in VDV 451/452. Only the strings are used here as the numbers
    have never been encountered in practice.
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
    This class serves to list the required tables for the VDV 452 data ingestion.
    """

    # Required Tables:
    # BASIS_VER_GUELTIGKEIT: Nötig, um herauszufinden welches die aktuell gültige Version ist (kein Handling, falls nicht existent und trz nur 1 Version existiert)
    # FIRMENKALENDER: Brauchen wir wegen zuordnung Betriebstage zu Tagesarten (für Umläufe)
    # REC_ORT: sprechende Namen der Haltestellen, aber auch (optional) Längen- und Breitengrade, Höhe
    # MENGE_FZG_TYP: Angaben der Fahrzeugtypen (Länge, Breite, ..., u.U. Batterieinfos, Name, Verbräuche, ..)
    # REC_SEL: Nötig, enthalten Angaben über die km-Entfernung zwischen den Haltestellen
    # SEL_FZT_FELD: Fahrzeiten zwischen zwei Haltestellen (je Fahrzeitgruppe), definitiv nötig
    # LID_VERLAUF: Für die Linienverläufe (Stationen und Reihenfolge)
    # REC_FRT: brauchen wir für die Zuordnung Fahrt->Umlauf. In der Tabelle findet die Zuordnung der Fahrt zur Linie und Tagesart.
    # REC_UMLAUF: Hier sind die eigentlichen Umläufe beschrieben

    # nicht ganz klar / Kontextabhängig:
    # Haltezeiten: ggfs. entweder-oder aus:
    #   a) ORT_HZTF: Haltezeiten je Fahrzeitgruppe und Ort/Haltestelle.
    #   b) REC_FRT_HZT: Haltezeiten je Fahrt. Also spezifischer als ORT_HZTF. Mir ist unklar, was passiert, falls beide vorhanden sind. Finde ich nichts in der Doku. Würde dann immer das hier genommen?
    #   c) oder auf 0 falls nicht existent?
    # Überläuferfahrten (VDV 452 Kap. 9.8.2 bzw. S. 65 bei "Erläuterung zur Fahrzeugumlaufbildung aus den Fahrten", demnach ist es kontextabhängig ob die folgenden Tabellen nötig sind):
    #   a) REC_UEB: Länge der Fahrt zwischen zwei Orten - bei Überläuferfahrt (Betriebshofaus- und -einfahrt, Zufahrten)
    #   b) UEB_FZT: Fahrzeiten bei Überläuferfahrten
    # !!! Anmerkung: Überläuferfahrten sind eben nicht immer so definiert, manchmal wird es wie eine "Fahrgastfahrt" behandelt, das schwankt etwas zwischen den mir vorliegenden VDV Datensätzen.
    # REC_LID: Linienvarianten. Ich würde sagen schon nötig, wegen der Linienbezeichnung; aber theoretisch könnte es ggfs. weggelassen werden (?) und mit dem PK gearbeitet werden
    # EINZELANSCHLUSS: für Anschlussdefinition, glaube mir machen keine Umstiege in eflips?
    # REC_UMS: Umsteigezeiten für Anschlusssicherung.

    # Später behandeln: 14 E-Mobilitäts Tabellen aus der VDV (Kapitel 11.6 - 11.14)

    # egal:
    # MENGE_BASIS_VERSIONEN: eigentlich nur für Textuelle Beschreibung der Basisversion nötig
    # MENGE_TAGESART: nur textuelle Beschreibung der Tagesart
    # MENGE_ONR_TYP: nur textuelle Beschreibung der funktionalen Ortstypen
    # MENGE_ORT_TYP: analog nur textuelle Beschreibung der Ortstypen
    # REC_HP: nur textuelle Beschreibung / Nummerierung der Haltepunkttypen
    # die beschreibung des Haltepunktes ist tatsächlich eig. egal, weil wir mit den REC_ORten arbeiten und nicht mit Haltepunkten
    # REC_OM: egal, weil nur nötig für Fahrzeug-Standortverfolgung im ITCS System (Ortsmarken)
    # FAHRZEUG: Machen wir im eflips-depot eh neu
    # ZUL_VERKEHRSBETRIEB: wir differenzieren nicht zwischen verschiedenen Verkehrsunternehmen, sondern simulieren alles
    # MENGE_BEREICH: wieder nur textuelle Beschreibung der Linienbereiche / Betriebszweige
    # REC_ANR: Ansagetexte sind uns egal
    # REC_ZNR: Angezeigte Fahrtziele, egal
    # REC_SEL_ZP: Geografischer Verlauf der Fahrt zwischen zwei Stationen - das betrachten wir nicht
    # MENGE_FGR: Textuelle Beschreibung von Fahrzeitgruppen, irrelevant
    # FLAECHEN_ZONE: Flächenzonen beschrieben, sollte uns egal sein
    # FL_ZONE_ORT: analog
    # MENGE_FLAECHEN_ZONE_TYP: analog
    # SEL_FZT_FELD_ZP: Fahrzeit für Zwischenpunkte. SOLLTE egal sein. todo kann es möglich sein, dass nur diese Zwischenpunkte angegeben sind oder so?!?!
    # MENGE_FAHRTART: Textuelle Beschreibung der Fahrtarten (z.B. Normalfahrt, Betriebshofausfahrt usw. siehe VDV 452 Kap. 9.6.8)

    # fahrzeug waere optional, aber machen wir nicht?

    # required tables: die Enum Datatypes als keys und das Dataclass object als value.
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
    ]  # None (optional) in the VDV_Data_Type represents "other / invalid data type" here


def fix_identical_stop_times(stop_times: List[StopTime]) -> None:
    """
    This function goes through a list of stop times and changes the arrival time of a stop time to be the same as the

    :param stop_times: A list of stop times. The list is assumed to be sorted by arrival time.
    :return: Nothing. The list is modified in place.
    """
    # First, identify the indizes of the stop times that have the same arrival time
    indizes_of_identical_arrival_times: Dict[datetime, List[int]] = {}
    for i, stop_time in enumerate(stop_times):
        if stop_time.arrival_time not in indizes_of_identical_arrival_times:
            indizes_of_identical_arrival_times[stop_time.arrival_time] = []
        indizes_of_identical_arrival_times[stop_time.arrival_time].append(i)
    indizes_of_identical_arrival_times = {k: v for k, v in indizes_of_identical_arrival_times.items() if len(v) > 1}

    # Now depending on the length of the list, we have to adjust the arrival times, so they are evenly spaced
    # throughout a minute (e.g. with 2 stops, the first one arrives at 12:00:00 and the second one at 12:00:30)
    # We do this by adding a timedelta to the arrival time of the stop time.
    # However, if the stop time is the last one in the list, we then need to *subtract* the offest, so the last
    # time stays the same

    for identical_arrival_times in indizes_of_identical_arrival_times.values():
        assert len(identical_arrival_times) > 1
        # We cannot assume the minimum resolution is one minute. So we need to check the minimum difference
        # Before and after
        if identical_arrival_times[0] != 0:
            diff_before = (
                stop_times[identical_arrival_times[0]].arrival_time
                - stop_times[identical_arrival_times[0] - 1].arrival_time
            )
        else:
            diff_before = timedelta(seconds=60)
        if identical_arrival_times[-1] != len(stop_times) - 1:
            diff_after = (
                stop_times[identical_arrival_times[-1] + 1].arrival_time
                - stop_times[identical_arrival_times[-1]].arrival_time
            )
        else:
            diff_after = timedelta(seconds=60)
        # We take the minimum of the two
        offset = min(diff_before, diff_after) / len(identical_arrival_times)
        for i, idx in enumerate(identical_arrival_times):
            stop_times[idx].arrival_time += i * offset
        if idx == len(stop_times) - 1:
            # This is how much we shifted the last stop time, so we need to subtract it again
            max_offset = (len(identical_arrival_times) - 1) * offset
            for idx in identical_arrival_times:
                stop_times[idx].arrival_time -= max_offset


class VdvIngester(AbstractIngester):
    def prepare(  # type: ignore[override]
        self,
        x10_zip_file: Path,
        progress_callback: None | Callable[[float], None] = None,
    ) -> Tuple[bool, UUID | Dict[str, str]]:
        """
        Prepare and validate the input data for ingestion.

        :param x10_zip_file: A pathlib.Path object representing the path to the x10 zip file.
        :param progress_callback: A function that accepts a float value between 0 and 1. This function will be called
                                  periodically to update the progress of the ingestion process.
        :return: A tuple containing a boolean indicating whether the input data is valid and either a UUID or a dictionary
                 containing the error message.
        """

        # Check that the zip file only contains x10 files and that they are not empty or too large
        valid_or_error = validate_zip_file(x10_zip_file)
        if valid_or_error is not True:
            assert isinstance(valid_or_error, dict)
            return False, valid_or_error

        # Generate a uuid and extract the zip file to the temporary directory
        uuid = uuid4()
        dir = self.path_for_uuid(uuid)
        os.makedirs(dir, exist_ok=False)
        with ZipFile(x10_zip_file, "r") as zip_file:
            zip_file.extractall(dir)

        # Check if all the required tables are present in the directory
        try:
            all_tables = validate_input_data_vdv_451(dir)
        except ValueError as e:
            return False, {"validation": str(e)}

        with open(dir / "all_tables.pkl", "wb") as fp:
            pickle.dump(all_tables, fp)

        # If all tables are present, return the UUID
        return True, uuid

    def ingest(self, uuid: UUID, progress_callback: None | Callable[[float], None] = None) -> None:
        logger = logging.getLogger(__name__)

        # Load the paths to the tables
        temp_dir = self.path_for_uuid(uuid)
        all_tables_file = Path(temp_dir) / "all_tables.pkl"
        with open(all_tables_file, "rb") as fp:
            all_tables = pickle.load(fp)

        # For each table, turn it into a list of VDV base objects
        all_data: Dict[VDV_Table_Name, List[VdvBaseObject]] = {}
        for tbl in all_tables:
            all_data[tbl] = import_vdv452_table_records(all_tables[tbl])

        # Now, we have all the data in the all_data dictionary. For each data piece,
        # - put it in the database in the correct object
        # - if we need to reference it later, put it into a dictionary, where we store the eflips-model object against
        #   its VDV-style primary key

        engine = create_engine(self.database_url)
        with Session(engine) as session:
            try:
                # Create the scenario, if it does not exist
                scenario_q = session.query(Scenario).filter(Scenario.task_id == str(uuid))
                if scenario_q.count() == 0:
                    scenario = Scenario(name=f"Created from VDV data with UUID {uuid}")
                    session.add(scenario)
                else:
                    scenario = scenario_q.one()

                # Vehicle Types
                vehicle_types_by_vdv_pk: Dict[Tuple[int | date | str, ...], VehicleType] = {}
                for vdv_vehicle_type in all_data[VDV_Table_Name.MENGE_FZG_TYP]:
                    assert isinstance(vdv_vehicle_type, MengeFzgTyp)
                    db_vehicle_type = vdv_vehicle_type.to_vehicle_type(scenario)
                    session.add(db_vehicle_type)
                    vehicle_types_by_vdv_pk[vdv_vehicle_type.primary_key] = db_vehicle_type

                # Rotations
                rotations_by_vdv_pk: Dict[Tuple[int | str | date, ...], Rotation] = {}

                # We may need a dummy vehicle type for rotations that do not have a vehicle type associated with them
                dummy_vehicle_type: VehicleType | None = None

                for vdv_rotation in all_data[VDV_Table_Name.REC_UMLAUF]:
                    assert isinstance(vdv_rotation, RecUmlauf)

                    if vdv_rotation.fzg_typ_nr is None and dummy_vehicle_type is None:
                        # We need to add a dummy vehicle type for the first time, create it
                        dummy_vehicle_type = self.create_dummy_vehicle_type(scenario)
                        session.add(dummy_vehicle_type)

                    db_rotation = vdv_rotation.to_rotation(
                        scenario, vehicle_types_by_vdv_pk, dummy_vehicle_type=dummy_vehicle_type
                    )
                    session.add(db_rotation)
                    rotations_by_vdv_pk[vdv_rotation.primary_key] = db_rotation

                # Stations

                # Stations we handle specially. Since the VDV RecOrt is more what we would call the
                # "coordinates of RouteStopAssociations" in eflips-model, we use a method there to extract the actual
                # Station object from it.
                assert all(isinstance(x, RecOrt) for x in all_data[VDV_Table_Name.REC_ORT])
                rec_orts: List[RecOrt] = [x for x in all_data[VDV_Table_Name.REC_ORT] if isinstance(x, RecOrt)]
                stations_by_vdv_pk: Dict[Tuple[int | date | str, ...], Station] = RecOrt.list_of_stations(
                    rec_orts, scenario
                )
                # The values of this dict are not unique, so we only add the unique ones to the database
                for station in set(stations_by_vdv_pk.values()):
                    session.add(station)

                # Lines
                # The same Line might be shared by multiple RecLid objects, but is unique by li_kuerzel
                lines_by_li_kuerzel: Dict[str, Line] = {}
                lines_by_vdv_pk: Dict[Tuple[int | date | str, ...], Line] = {}

                assert all(isinstance(x, RecLid) for x in all_data[VDV_Table_Name.REC_LID])
                rec_lids = [x for x in all_data[VDV_Table_Name.REC_LID] if isinstance(x, RecLid)]

                for rec_lid in rec_lids:
                    line_name = rec_lid.li_kuerzel
                    if line_name not in lines_by_li_kuerzel:
                        line = Line(name=line_name, scenario=scenario)
                        session.add(line)
                        lines_by_li_kuerzel[line_name] = line
                    else:
                        line = lines_by_li_kuerzel[line_name]
                    lines_by_vdv_pk[rec_lid.primary_key] = line

                # Routes
                # Those are more intricate, as we will have to compose them from the RecLid and RecSel and ??? tables
                # We need to construct quite a few helper dictionaries for this
                lines_by_basis_version_and_li_nr: Dict[Tuple[int | date | str, ...], Line] = {
                    (k[0], k[1]): v for k, v in lines_by_vdv_pk.items()
                }

                rec_orts_by_basis_version_and_onr_typ_nr_and_ort_nr = {
                    (r.basis_version, r.onr_typ_nr, r.ort_nr): r for r in rec_orts
                }

                assert all(isinstance(x, RecSel) for x in all_data[VDV_Table_Name.REC_SEL])
                rec_sels: List[RecSel] = [x for x in all_data[VDV_Table_Name.REC_SEL] if isinstance(x, RecSel)]
                rec_sel_by_basis_version_and_start_type_and_start_nr_and_end_type_and_end_nr = {
                    (r.basis_version, r.onr_typ_nr, r.ort_nr, r.sel_ziel_typ, r.sel_ziel): r for r in rec_sels
                }

                assert all(isinstance(x, LidVerlauf) for x in all_data[VDV_Table_Name.LID_VERLAUF])
                lid_verlaufs: List[LidVerlauf] = [
                    x for x in all_data[VDV_Table_Name.LID_VERLAUF] if isinstance(x, LidVerlauf)
                ]
                lid_verlaufs_by_basis_version_and_li_nr_and_str_li_var: Dict[
                    Tuple[int, int, str], List[LidVerlauf]
                ] = {}
                for lid_verlauf in lid_verlaufs:
                    key = (lid_verlauf.basis_version, lid_verlauf.li_nr, lid_verlauf.str_li_var)
                    if key not in lid_verlaufs_by_basis_version_and_li_nr_and_str_li_var:
                        lid_verlaufs_by_basis_version_and_li_nr_and_str_li_var[key] = []
                    # Insert it in the correct order, by the lid_verlauf.li_lfd_nr
                    # Runtime expensive, but works
                    lid_verlaufs_by_basis_version_and_li_nr_and_str_li_var[key].append(lid_verlauf)
                    lid_verlaufs_by_basis_version_and_li_nr_and_str_li_var[key].sort(key=lambda x: x.li_lfd_nr)

                # Now we can construct the routes
                routes_by_vdv_pk: Dict[Tuple[int | date | str, ...], Route] = {}
                assert all(isinstance(x, RecLid) for x in all_data[VDV_Table_Name.REC_LID])
                rec_lids = [x for x in all_data[VDV_Table_Name.REC_LID] if isinstance(x, RecLid)]

                rec_selss: Dict[
                    Tuple[int | date | str, ...], List[RecSel]
                ] = {}  # Later we will use this to construct the trips

                for rec_lid in rec_lids:
                    route: Route
                    route, rec_selss[(rec_lid.basis_version, rec_lid.li_nr, rec_lid.str_li_var)] = rec_lid.to_route(
                        scenario=scenario,
                        lines_by_basis_version_andli_nr=lines_by_basis_version_and_li_nr,
                        rec_orts_by_basis_version_and_onr_typ_nr_and_ort_nr=rec_orts_by_basis_version_and_onr_typ_nr_and_ort_nr,
                        lid_verlaufs_by_basis_version_and_li_nr_and_str_li_var=lid_verlaufs_by_basis_version_and_li_nr_and_str_li_var,
                        stations_by_basis_version_and_onr_typ_nr_and_ort_nr=stations_by_vdv_pk,
                        rec_sel_by_basis_version_and_start_type_and_start_nr_and_end_type_and_end_nr=rec_sel_by_basis_version_and_start_type_and_start_nr_and_end_type_and_end_nr,
                    )
                    session.add(route)
                    routes_by_vdv_pk[rec_lid.primary_key] = route

                # Now we can construct the trips

                assert all(isinstance(x, SelFztFeld) for x in all_data[VDV_Table_Name.SEL_FZT_FELD])
                sel_fzt_felds = [x for x in all_data[VDV_Table_Name.SEL_FZT_FELD] if isinstance(x, SelFztFeld)]
                sel_fzt_felds_by_pk: Dict[Tuple[int | date | str, ...], List[SelFztFeld]] = {}
                for this_sel_fzt_feld in sel_fzt_felds:
                    pk = this_sel_fzt_feld.primary_key
                    if pk not in sel_fzt_felds_by_pk:
                        sel_fzt_felds_by_pk[pk] = []
                    sel_fzt_felds_by_pk[pk].append(this_sel_fzt_feld)

                # We also need to create a slighltly relaxes sel_fzt_felds_by_pk, where we only have one entry per pk
                sel_fzt_felds_by_relaxed_pk: Dict[Tuple[int | date | str, ...], List[SelFztFeld]] = {}
                for x in sel_fzt_felds:
                    relaxed_pk = (x.basis_version, x.bereich_nr, x.onr_typ_nr, x.ort_nr, x.sel_ziel_typ, x.sel_ziel)
                    if relaxed_pk not in sel_fzt_felds_by_relaxed_pk:
                        sel_fzt_felds_by_relaxed_pk[relaxed_pk] = []
                    sel_fzt_felds_by_relaxed_pk[relaxed_pk].append(x)

                assert all(isinstance(x, RecFrt) for x in all_data[VDV_Table_Name.REC_FRT])
                rec_frts = [x for x in all_data[VDV_Table_Name.REC_FRT] if isinstance(x, RecFrt)]

                if VDV_Table_Name.ORT_HZTF in all_data.keys():
                    assert all(isinstance(x, OrtHztf) for x in all_data[VDV_Table_Name.ORT_HZTF])
                    ort_hztfs = [x for x in all_data[VDV_Table_Name.ORT_HZTF] if isinstance(x, OrtHztf)]
                else:
                    ort_hztfs = []

                if VDV_Table_Name.REC_FRT_HZT in all_data.keys():
                    assert all(isinstance(x, RecFrtHzt) for x in all_data[VDV_Table_Name.REC_FRT_HZT])
                    rec_frt_hzts = [x for x in all_data[VDV_Table_Name.REC_FRT_HZT] if isinstance(x, RecFrtHzt)]
                else:
                    rec_frt_hzts = []

                # It only makes sense to have one of the two
                assert bool(len(rec_frt_hzts) > 0) ^ bool(len(ort_hztfs) > 0)

                assert all(isinstance(x, Firmenkalender) for x in all_data[VDV_Table_Name.FIRMENKALENDER])
                firmenkalenders = [x for x in all_data[VDV_Table_Name.FIRMENKALENDER] if isinstance(x, Firmenkalender)]

                rotations_by_vdv_pk_and_date: Dict[Tuple[int, int, int, date], Rotation] = dict()

                for rec_frt in tqdm(rec_frts):
                    # Load the dwell durations rec-frt-hzt that belong to this trip
                    this_trip_rec_frt_hzts = [x for x in rec_frt_hzts if x.frt_fid == rec_frt.frt_fid]

                    # Load the corresponding Route object
                    route = routes_by_vdv_pk[(rec_frt.basis_version, rec_frt.li_nr, rec_frt.str_li_var)]

                    # Also, load the corresponding rec_sels
                    this_route_rec_sels: List[RecSel] = rec_selss[
                        (rec_frt.basis_version, rec_frt.li_nr, rec_frt.str_li_var)
                    ]
                    this_route_sel_fzt_felds = []
                    for rec_sel in this_route_rec_sels:
                        sel_fzt_feld_pk = (
                            rec_sel.basis_version,
                            rec_sel.bereich_nr,
                            rec_frt.fgr_nr,
                            rec_sel.onr_typ_nr,
                            rec_sel.ort_nr,
                            rec_sel.sel_ziel_typ,
                            rec_sel.sel_ziel,
                        )

                        if sel_fzt_feld_pk in sel_fzt_felds_by_pk.keys():
                            sel_fzt_feld = sel_fzt_felds_by_pk[sel_fzt_feld_pk]
                        else:
                            logger.debug(f"Could not find SelFztFeld for {sel_fzt_feld_pk}")
                            # Find one by relaxing the constraints
                            sel_fzt_feld = sel_fzt_felds_by_relaxed_pk[
                                (
                                    rec_sel.basis_version,
                                    rec_sel.bereich_nr,
                                    rec_sel.onr_typ_nr,
                                    rec_sel.ort_nr,
                                    rec_sel.sel_ziel_typ,
                                    rec_sel.sel_ziel,
                                )
                            ]
                            sel_fzt_feld = [sel_fzt_feld[0]]

                        if len(sel_fzt_feld) != 1:
                            logger.info(f"Could not find exactly one SelFztFeld for {sel_fzt_feld_pk}")
                            # If there are more than one, make sure the durations are the same
                            if len(sel_fzt_feld) > 1:
                                durations = [x.sel_fzt for x in sel_fzt_feld]
                                if len(set(durations)) != 1:
                                    logger.warning(
                                        f"Multiple SelFztFelds for {sel_fzt_feld_pk} have different durations: {durations}"
                                    )
                                    raise ValueError(
                                        f"Multiple SelFztFelds for {sel_fzt_feld_pk} have different durations: {durations}"
                                    )
                                else:
                                    duration = durations[0]
                            else:
                                # Length is 0 -- create a zero duration
                                duration = timedelta(minutes=0)
                            # For now, create a dummy one
                            sel_fzt_feld = [
                                SelFztFeld(
                                    basis_version=rec_sel.basis_version,
                                    bereich_nr=rec_sel.bereich_nr,
                                    fgr_nr=rec_frt.fgr_nr,
                                    onr_typ_nr=rec_sel.onr_typ_nr,
                                    ort_nr=rec_sel.ort_nr,
                                    sel_ziel_typ=rec_sel.sel_ziel_typ,
                                    sel_ziel=rec_sel.sel_ziel,
                                    sel_fzt=duration,
                                )
                            ]
                        this_route_sel_fzt_felds.append(sel_fzt_feld[0])

                    # Calculate the dwell durations and driving durations for each station
                    elapsed_duration = rec_frt.frt_start
                    arrival_time_from_start: List[timedelta] = []
                    dwell_durations: List[timedelta] = []
                    for i in range(len(this_route_rec_sels)):
                        cur_rec_sel = this_route_rec_sels[i]
                        # Add the first station
                        if i == 0:
                            # Check the rec_frt_hzts for the first station
                            first_station_pk = (cur_rec_sel.basis_version, cur_rec_sel.onr_typ_nr, cur_rec_sel.ort_nr)
                            first_station_rec_frt_hzts = [
                                x for x in this_trip_rec_frt_hzts if x.position_key == first_station_pk
                            ]

                            # Check the ort_hztfs for the first station
                            first_station_ort_hztfs = [x for x in ort_hztfs if x.position_key == first_station_pk]

                            if len(first_station_rec_frt_hzts) == 1:
                                dwell_duration = first_station_rec_frt_hzts[0].frt_hzt_zeit
                            elif len(first_station_ort_hztfs) == 1:
                                dwell_duration = first_station_ort_hztfs[0].hp_hzt
                            else:
                                logger.debug(
                                    f"Could not find any dwell duration for the station {first_station_pk}. Adding 0s."
                                )
                                # For now, create a dummy one
                                dwell_duration = timedelta(seconds=0)

                            arrival_time_from_start.append(
                                elapsed_duration
                            )  # For the first station, the arrival time is the start of the trip
                            dwell_durations.append(dwell_duration)  # dwell duration for the first station
                            elapsed_duration += dwell_duration

                        # Now, always add the driving duration and the dwell duration at the destination of the rec_sel
                        # First, add the driving duration
                        cur_sel_fzt_feld = this_route_sel_fzt_felds[i]
                        driving_duration = cur_sel_fzt_feld.sel_fzt
                        elapsed_duration += driving_duration
                        arrival_time_from_start.append(elapsed_duration)

                        # Load the dwell duration for the destination station of this segment
                        next_station_pk = (cur_rec_sel.basis_version, cur_rec_sel.sel_ziel_typ, cur_rec_sel.sel_ziel)
                        next_station_rec_frt_hzts = [
                            x for x in this_trip_rec_frt_hzts if x.position_key == next_station_pk
                        ]

                        next_station_ort_hztfs = [x for x in ort_hztfs if x.position_key == next_station_pk]

                        if len(next_station_rec_frt_hzts) == 1:
                            dwell_duration = next_station_rec_frt_hzts[0].frt_hzt_zeit
                        elif len(next_station_ort_hztfs) == 1:
                            dwell_duration = next_station_ort_hztfs[0].hp_hzt
                        else:
                            logger.debug(
                                f"Could not find any dwell duration for the station {next_station_pk}. Adding 0s."
                            )
                            # For now, create a dummy one
                            dwell_duration = timedelta(seconds=0)

                        dwell_durations.append(dwell_duration)
                        elapsed_duration += dwell_duration

                    ### CREATE THE TRIP
                    # We need to do this on all days that have the same tagesart as the rec_frt
                    # We need to find the tagesart of the rec_frt
                    for firmenkalender in firmenkalenders:
                        if firmenkalender.tagesart_nr == rec_frt.tagesart_nr:
                            the_date = firmenkalender.betriebstag

                            # Check if a specific rotation for this day exists
                            vdv_pk_and_date = (rec_frt.basis_version, rec_frt.tagesart_nr, rec_frt.um_uid, the_date)
                            if vdv_pk_and_date in rotations_by_vdv_pk_and_date:
                                rotation = rotations_by_vdv_pk_and_date[vdv_pk_and_date]
                            else:
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
                                rotations_by_vdv_pk_and_date[vdv_pk_and_date] = rotation
                                session.add(rotation)

                            # Create a local midnight datetime object in the "Europe/Berlin" timezone
                            tz = pytz.timezone("Europe/Berlin")
                            local_midnight = tz.localize(datetime.combine(the_date, time(0, 0)))

                            # Create the trip, if it is a valid trip
                            if rec_frt.frt_start.total_seconds() != elapsed_duration.total_seconds():
                                trip = Trip(
                                    scenario=scenario,
                                    route=route,
                                    departure_time=local_midnight + rec_frt.frt_start,
                                    arrival_time=local_midnight + elapsed_duration,
                                    trip_type=TripType.PASSENGER if rec_frt.fahrtart_nr == 1 else TripType.EMPTY,
                                )

                                # Create the stop times
                                stop_times = []
                                for i in range(len(route.assoc_route_stations)):
                                    station = route.assoc_route_stations[i].station
                                    arrival_time = local_midnight + arrival_time_from_start[i]
                                    dwell_duration = dwell_durations[i]
                                    stop_time = StopTime(
                                        scenario=scenario,
                                        trip=trip,
                                        station=station,
                                        arrival_time=arrival_time,
                                        dwell_duration=dwell_duration,
                                    )
                                    stop_times.append(stop_time)

                                # Fix identical stop times
                                fix_identical_stop_times(stop_times)

                                # Look up the rotation using the basis_version and um_uid
                                trip.rotation = rotation
                                session.add(trip)
                            else:
                                raise ValueError(f"Trip {rec_frt.frt_fid} has a duration of 0 seconds. Skipping.")

                # Delete all rotations in this scenario with no trips
                session.flush()
                for rotation in scenario.rotations:
                    if len(rotation.trips) == 0:
                        session.delete(rotation)

            except Exception as e:
                session.rollback()
                raise e
            finally:
                session.commit()

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
        A dictionary containing the parameter names for :meth:`prepare`.

        These should be short, descriptive names for the parameters of the :meth:`prepare` method. The keys must be the
        names of the parameters, and the values should be strings describing the parameter. If the keyword argument is
        an enumerated type, the value should be a dictionary with the keys being the members.

        This method can then be used to generate a help text for the user.

        :return: A dictionary containing the parameter hints for the prepare method.
        """
        return {
            "x10_zip_file": "VDV data archive",
        }

    @classmethod
    def prepare_param_description(cls) -> Dict[str, str | Dict[Enum, str]]:
        """
        A dictionary containing the parameter descriptions for :meth:`prepare`.

        These should be longer, more detailed descriptions of the parameters of the :meth:`prepare`
        method. The keys must be the names of the parameters, and the values should be strings describing the parameter.

        This method can then be used to generate a help text for the user.

        :return: A dictionary containing the parameter hints for the prepare method.
        """
        return {
            "x10_zip_file": "A zip file containing the VDV data. The zip file must contain all necessary tables for the"
            " VDV 451/452 format. These should be in the form of .x10 files in the top level of the zip"
            " file.",
        }


def validate_zip_file(zipfile: Path) -> bool | Dict[str, str]:
    """
    Validate the zip file.

    :param zipfile: A pathlib.Path object representing the path to the zip file.
    :return: A boolean indicating whether the zip file is valid and either a dictionary containing the error message(s).
    """
    try:
        error_messages = {}
        zip_file = ZipFile(zipfile)
        valid = True
        for entry in zip_file.infolist():
            if (entry.filename.endswith(".x10") or entry.filename.endswith(".X10")) and not entry.is_dir():
                if entry.file_size == 0:
                    valid = False
                    error_messages[entry.filename] = "Empty file"
                elif entry.file_size > 100 * 1024 * 1024:
                    valid = False
                    error_messages[entry.filename] = "File size exceeds 100 MiB"
            else:
                valid = False
                error_messages[entry.filename] = "Invalid file extension or is a directory"
    except Exception as e:
        valid = False
        error_messages["zipfile"] = str(e)

    if not valid:
        return error_messages
    return True


def validate_input_data_vdv_451(
    abs_path_to_folder_with_vdv_files: str | Path,
) -> dict[VDV_Table_Name, VDVTable]:
    """
    Checks if the given directory contains all necessary .x10 files (necessary as in VDV 451/452 specified). Will raise
    an exception if not all necessary tables are present.

    :param abs_path_to_folder_with_vdv_files: The ABSOLUTE path to the directory containing the VDV 451 files
    :return: A dictionary containing the table names as keys and the Table objects as values.
    """
    logger = logging.getLogger(__name__)

    # Create a Pattern to find all .x10 Files in this directory
    # in macOS (unlike windows) searching for *.x10 wiles will NOT find files with the extension .X10.
    # Therefore, we need to search for both, but also filter out the duplicates later as we would otherwise have duplicates in windows
    search_pattern_lowercase = os.path.join(abs_path_to_folder_with_vdv_files, "*.x10")
    search_pattern_uppercase = os.path.join(abs_path_to_folder_with_vdv_files, "*.X10")

    # Find all files that match this pattern.
    x10_files = glob.glob(search_pattern_lowercase) + glob.glob(search_pattern_uppercase)
    x10_files_unique = list(set(x10_files))

    # Iterate through the files, checking whether the neccessary tables are present

    # VDV 451 has two naming schemes. One is the name of the table directly as the name, the other is some number combination,
    # see VDV 451 Chapter 3.1 and 3.2. However, as the name of the table is also included in the file contents, we instead
    # check the contents of each file to determine to which table it belongs.

    all_tables: dict[VDV_Table_Name, VDVTable] = {}
    for abs_file_path in x10_files_unique:
        try:
            eingangsdatentable: VDVTable = check_vdv451_file_header(abs_file_path)

            # Check if the table name is already present in the dictionary (would mean duplicate, two times the same table in the files)
            if eingangsdatentable.table_name in all_tables.keys():
                raise ValueError(
                    "The table " + eingangsdatentable.table_name.value + " is present in multiple files. Aborting."
                )

            else:
                all_tables[eingangsdatentable.table_name] = eingangsdatentable

        except (ValueError, UnicodeDecodeError) as e:
            msg = "While processing " + abs_file_path + " the following exception occurred: "
            logger.debug(msg, exc_info=e)
            continue

    # Required Tables:
    # siehe ganz oben in der File fuer explanation

    if not set(VdvRequiredTables.required_tables.keys()) <= set(all_tables.keys()):
        # Compute all tables that are required but not in the tables in the files, to display them to the user
        missing_tables = set(VdvRequiredTables.required_tables.keys()) - set(all_tables.keys())
        missing_tables_str = " ".join([x.value + ", " for x in missing_tables])
        raise ValueError(
            "Not all necessary tables are present in the directory (or present, but empty). Missing tables are: "
            + missing_tables_str
            + " aborting.",
        )

    # Either REC_FRT_HZT or ORT_HZTF must be present, not both(?)

    if (VDV_Table_Name.REC_FRT_HZT in all_tables.keys()) and (VDV_Table_Name.ORT_HZTF in all_tables.keys()):
        # Both tables present...
        raise ValueError(
            "Either REC_FRT_HZT or ORT_HZTF must be present in the dataset, but both are present. Aborting."
        )

    if (VDV_Table_Name.REC_FRT_HZT not in all_tables.keys()) and (VDV_Table_Name.ORT_HZTF not in all_tables.keys()):
        # Gar keine Haltezeiten dabei
        raise ValueError("Neither REC_FRT_HZT nor ORT_HZTF present in the directory. Aborting.")

    logger.info("All necessary tables are present in the directory.")
    return all_tables


def check_vdv451_file_header(abs_file_path: str) -> VDVTable:
    """
    Checks the contents of a VDV 451 (.x10) file, extracting the table name, character set, column names and data types.
    :param file_path: The ABSOLUTE path to the VDV 451 file
    :return: A VDVTable object containing the table name, character set, column names and data types.

    """

    # 1. Open file and recognize the encoding
    # For VDV 451, either ASCII or ISO8859-1 is allowed as encoding for the table datasets. However, the header is always ASCII (see Ch. 4.1 of VDV 451).
    # Therefore, we open the file with ISO8859-1
    # (and return an error if it is not ASCII or ISO8859-1).
    logger = logging.getLogger(__name__)

    table_name_str = None
    character_set = None
    datatypes = None
    column_names = None

    valid_character_sets = ["ASCII", "ISO8859-1"]

    try:
        with open(abs_file_path, "r", encoding="ISO8859-1") as f:
            for line in f:
                if line.strip().split(";")[0] == "chs":
                    # For these modes, we need to utilize the CSV reader here in order to get rid of the double quote marks enclosing the strings (otherwise, we would have e.g. '"Templin, ZOB"') etc.
                    parts_csvrdr = csv.reader([line], delimiter=";", skipinitialspace=True)
                    parts = list(parts_csvrdr)[0]

                else:
                    # The other modes should be uncritical as they do not contain those double quotes or we dont need the info in them
                    parts = line.strip().split(";")

                # Handling of the line based on the specific "command" (see VDV 451 documentation)
                command = parts[0]

                if command == "tbl":
                    # Get the table name (e.g. 'MENGE_BASIS_VERSIONEN')
                    table_name_str = parts[1].upper().strip()

                elif command == "chs":
                    # Get the character set used in the file
                    character_set = parts[1].upper().strip()

                    # Oftentimes, the Character set is accidentally named as ISO-8859-1 (additional dash).
                    # Fix the character set if it is ISO-8859-1 to the correct form
                    if character_set == "ISO-8859-1":
                        character_set = "ISO8859-1"

                    if character_set not in valid_character_sets:
                        raise ValueError(
                            "The file",
                            abs_file_path,
                            " uses an encoding that is not allowed according to the VDV 451 specification:"
                            + character_set
                            + " does not match 'ASCII' or 'ISO8859-1'.",
                        )

                elif command == "frm":
                    # todo (also for charset) check for double entries of frm, chs, tbl, ..?
                    # Get data formats of the columns (this will be something like ['num[9.0]', 'num[8.0]', 'char[40]', 'num[2.4]'])
                    formats = parts[1:]

                    try:
                        datatypes = parse_datatypes(formats)
                    except ValueError as e:
                        e.add_note(
                            "The file"
                            + str(abs_file_path)
                            + " contains invalid column data types. Please check the formatting of the data types in the file."
                        )
                        raise e

                elif command == "atr":
                    # Get the column names
                    cx = parts[1:]
                    column_names = [x.upper().strip() for x in cx]
                elif command == "rec":
                    if table_name_str is not None and character_set is not None:
                        # We have all necessary information (and it contains at least one record)
                        break

                elif command == "eof":
                    # We reached the end of the file without seeing any records
                    raise ValueError("The file" + str(abs_file_path) + " does not contain any records.")

    except UnicodeDecodeError as e:
        e.add_note(
            "The header of the file"
            + str(abs_file_path)
            + " is using an encoding that contains non-ASCII characters. This is not allowed according to the VDV 451 specification.",
        )
        raise e

    # Raise an error if table name or encoding is not found
    if table_name_str is None:
        msg = f"The file {abs_file_path} does not contain a table name in the header."
        logger.info(msg)
        raise ValueError(msg)
    if character_set is None:
        msg = f"The file {abs_file_path} does not contain a character set in the header."
        logger.info(msg)
        raise ValueError(msg)

    if datatypes is None:
        msg = f"The file {abs_file_path} does not contain the data types of the columns in the header."
        logger.info(msg)
        raise ValueError(msg)

    if table_name_str not in [x.value for x in VDV_Table_Name]:
        raise ValueError(
            "The file" + str(abs_file_path) + " contains an unknown table name: " + table_name_str + " Skipping it."
        )

    if column_names is None:
        raise ValueError(
            "The file"
            + str(abs_file_path)
            + " does not contain the column names in the header. Please check the file and try again."
        )

    if len(column_names) != len(datatypes):
        raise ValueError(
            "The file"
            + str(abs_file_path)
            + " contains an unequal number of column names and column data types in the header: "
            + str(len(column_names))
            + " column names, but "
            + str(len(datatypes))
            + " column data types."
        )

    return VDVTable(
        abs_file_path=abs_file_path,
        character_set=character_set,
        table_name=VDV_Table_Name[table_name_str],
        column_names_and_data_types=list(zip(column_names, datatypes)),
    )


def parse_datatypes(datatype_str: list[str]) -> list[Optional[VDV_Data_Type]]:
    """
    Converts a list of datatype strings in VDV 451 format to a list of Python/Numpy datatypes
    e.g., turn something like ['num[9.0]', 'char[40]', 'num[2.0]'] into ['int', 'string', 'int']

    (We do this as we will later convert the column datatypes to the correct Python/Numpy datatypes)
    So for every column in the VDV 451 file, check if 'num', 'int' or 'float'.

    :param datatype_str: a list with the datatypes from the VDV 451 file, (with each datatype as a string, e.g. 'char[40]')
    :return: a list of python datatypes, but as strings
    """

    # todo add logger?
    logger = logging.getLogger(__name__)

    dtypes: list[Optional[VDV_Data_Type]] = []
    for part in datatype_str:
        part = part.lstrip()  # remove leading spaces

        # check if the datatype is valid (e.g. 'num[9.0]' or 'char[40]' etc.)
        # according to the VDV 451 specification, only 'char[n]' and 'num[n.0]' are allowed

        regex = r"(char\[[0-9]+\]|num\[[0-9]+.[0-9]+\])+"

        if not re.match(regex, part):
            # Avoid the program to crash if the datatype is invalid, but still log a warning
            # Sometimes, there are floats used for additional columns (columns not formally included the VDV 452 specification)
            dtypes.append(None)
            # todo genauere angabe, in welcher Datei / Spalte es auftrat?
            msg = f"Invalid datatype formatting in VDV 451 file: {part} does not match 'char[n]' or 'num[n.n]'. Column will not be imported."
            logger.warning(msg)
            continue

        regex_for_char = r"char\[[0-9]+\]"
        regex_for_int = r"num\[[0-9]+.0\]"
        regex_for_float = r"num\[[0-9]+.[0-9]+\]"

        if re.match(regex_for_char, part):
            dtypes.append(VDV_Data_Type.CHAR)
        elif re.match(regex_for_int, part):
            dtypes.append(VDV_Data_Type.INT)
        elif re.match(regex_for_float, part):
            dtypes.append(VDV_Data_Type.FLOAT)

    return dtypes


def import_vdv452_table_records(EingangsdatenTabelle: VDVTable) -> list[VdvBaseObject]:
    """
    Imports the records of a VDV 451 table into the database.
    :param EingangsdatenTabelle: The EingangsdatenTabelle object containing the table name and the path to the file
    :return: None
    """
    logger = logging.getLogger(__name__)

    # Open the file
    with open(EingangsdatenTabelle.abs_file_path, "r", encoding=EingangsdatenTabelle.character_set) as f:
        reader = csv.reader(f, delimiter=";", skipinitialspace=True)
        dict_list = []
        for row in reader:
            if len(row) == 0 or row[0].strip() != "rec":
                logger.debug("Skipping line: " + str(row))
                continue

            # Remove the 'rec' from the row
            row_data = row[1:]

            # create the json obj and give every column value the correct datatype
            e_data: Dict[str, str | int | float | None] = {}

            if len(row_data) != len(EingangsdatenTabelle.column_names_and_data_types):
                raise ValueError(
                    "The file"
                    + str(EingangsdatenTabelle.abs_file_path)
                    + " contains an record that has more or less columns than the header specifies. "
                    + "The record contains "
                    + str(row_data)
                    + ", aborting."
                )

            for i_col in range(0, len(row_data)):
                column_name = EingangsdatenTabelle.column_names_and_data_types[i_col][0]
                column_data_type = EingangsdatenTabelle.column_names_and_data_types[i_col][1]

                if row_data[i_col].strip() == "":
                    # Everything that has "no" value in the VDV 451 file is turned into a None
                    # NULL Entry (Also possible for numbers - thats why it is done BEFORE the Int conversion!)
                    e_data[column_name] = None

                elif column_data_type is None:
                    # Skip the column as it has an invalid data type.
                    continue

                elif column_data_type == VDV_Data_Type.INT:
                    try:
                        e_data[column_name] = int(row_data[i_col])
                    except ValueError as e:
                        e.add_note(
                            "The file"
                            + str(EingangsdatenTabelle.abs_file_path)
                            + " contains a non-numeric value in a column that is specified as numeric. Aborting."
                        )
                        raise e
                elif column_data_type == VDV_Data_Type.FLOAT:
                    try:
                        e_data[column_name] = float(row_data[i_col])
                    except ValueError as e:
                        e.add_note(
                            "The file"
                            + str(EingangsdatenTabelle.abs_file_path)
                            + " contains a non-numeric value in a column that is specified as numeric. Aborting."
                        )
                        raise e
                elif column_data_type == VDV_Data_Type.CHAR:
                    e_data[column_name] = row_data[i_col]
                else:
                    raise ValueError(
                        "The file"
                        + str(EingangsdatenTabelle.abs_file_path)
                        + " contains a column with an invalid data type: "
                        + str(column_data_type)
                        + ". Aborting."
                    )
            dict_list.append(e_data)

        # Now that we have created a nice dictionary, turn it into an object of the corresponding dataclass
        match EingangsdatenTabelle.table_name:
            case VDV_Table_Name.BASIS_VER_GUELTIGKEIT:
                # At the current time, we only support one distinct entry for this table.
                # If there are multiple. raise an error.
                # However, if the same entry is present multiple times, we do not raise an error.
                # So we need to turn the list of dictionaries inso a set of objects and check if the length is 1.
                objects: List[VdvBaseObject] = []
                for d in dict_list:
                    objects.append(BasisVerGueltigkeit.from_dict(d))
                if len(set(objects)) != 1:
                    raise ValueError(
                        "The table"
                        + str(EingangsdatenTabelle.table_name)
                        + " contains multiple distinct entries. Only one entry is allowed. Aborting."
                    )
                return objects

            case VDV_Table_Name.FIRMENKALENDER:
                return [Firmenkalender.from_dict(d) for d in dict_list]
            case VDV_Table_Name.REC_ORT:
                return [RecOrt.from_dict(d) for d in dict_list]
            case VDV_Table_Name.MENGE_FZG_TYP:
                return [MengeFzgTyp.from_dict(d) for d in dict_list]
            case VDV_Table_Name.REC_SEL:
                return [RecSel.from_dict(d) for d in dict_list]
            case VDV_Table_Name.SEL_FZT_FELD:
                return [SelFztFeld.from_dict(d) for d in dict_list]
            case VDV_Table_Name.LID_VERLAUF:
                return [LidVerlauf.from_dict(d) for d in dict_list]
            case VDV_Table_Name.REC_FRT:
                return [RecFrt.from_dict(d) for d in dict_list]
            case VDV_Table_Name.REC_UMLAUF:
                return [RecUmlauf.from_dict(d) for d in dict_list]
            case VDV_Table_Name.REC_LID:
                return [RecLid.from_dict(d) for d in dict_list]
            case VDV_Table_Name.REC_FRT_HZT:
                return [RecFrtHzt.from_dict(d) for d in dict_list]
            case VDV_Table_Name.ORT_HZTF:
                return [OrtHztf.from_dict(d) for d in dict_list]
            case _:  # default case
                return []
