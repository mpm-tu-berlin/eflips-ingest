import csv
import glob
import os
from datetime import datetime

import sqlalchemy
from sqlalchemy.orm import declarative_base

from typing import List

from sqlalchemy import create_engine, Column, Integer, DECIMAL


import pandas as pd
from dataclasses import dataclass

import vdv_schema

from eflips.model import (
    Area, AreaType, AssocPlanProcess, AssocRouteStation, Base, BatteryType,
    Depot, Event, EventType, Line, Plan, Process, Rotation, Route, Scenario,
    Station, StopTime, Trip, TripType, Vehicle, VehicleClass, VehicleType
)


#@dataclass
#class ParsedTable:
#    """Class for keeping track of xyz."""
#    table_name: str
#    df: pd.DataFrame

def parse_datatypes(datatype_str):
    #dtype_map = {
    #    'num': 'float',  # oder 'int' falls keine Nachkommastellen benötigt werden
    #    'char': 'object'
    #}

    # Haut uns 'num' oder 'char' für jede spalte raus

    dtypes = []
    for part in datatype_str:
        part = part.lstrip() # remove leading spaces
        type_info, size_info = part.split('[')
        size = size_info[:-1]  # Entferne die schließende Klammer
        if type_info == 'num':
            vorkomma, nachkomma = size.split('.')
            if nachkomma == '0':
                dtypes.append('int')
            else:
                dtypes.append('float')
        else:
            dtypes.append('string')

        #dtype = dtype_map[type_info]
        #dtypes.append(type_info)#dtype)
    return dtypes
def import_vdv451_file(file_path):
    encoding = 'ISO-8859-1'
    date_format = time_format = None
    columns = []
    formats = []

    e_data = []
    table_name = ""


    with open(file_path, 'r', encoding=encoding) as f:
        for line in f:
            #parts = line.strip().split(';')
            if line.strip().split(';')[0] in ('atr', 'frm', 'rec', 'mod'):
                parts_csvrdr = csv.reader([line], delimiter=";", skipinitialspace=True)
                parts = list(parts_csvrdr)[0]

            else:
                parts = line.strip().split(';')

            command = parts[0]


            if command == 'mod':
                # Extrahiere das Datum- und Zeitformat
                date_format = parts[1]
                time_format = parts[2]

            elif command == 'tbl':
                table_name = parts[1].upper().strip()

            elif command == 'atr':
                # Spaltennamen definieren
                cx = parts[1:]
                columns = [x.upper() for x in cx]

            elif command == 'frm':
                # Spalten-Datenformate definieren
                formats = parts[1:]


            elif command == 'rec':
                # Datenzeilen verarbeiten
                row_data = parts[1:]

                e_data.append(row_data)

    # Parsen des Strings und Erstellen des dtype-Objekts
    datatypes = parse_datatypes(formats)

    # Zahlen müssen zu integern gewandelt werden
    # gleichzeitig alles was leer ist bei string mal zu "None" machen
    for row in range(0,len(e_data)):
        for col in range(0, len(e_data[row])):
            if e_data[row][col].strip() == "":
                # NULL Eintrag (Auch bei Zahlen möglich - deshalb VOR der Int conversion zu nachen!)
                e_data[row][col] = None

            elif datatypes[col] == 'int':
                e_data[row][col] = int(e_data[row][col])
            elif datatypes[col] == 'float':
                e_data[row][col] = float(e_data[row][col])


    # Erstelle ein DataFrame, wenn Spalten und Daten vorhanden sind
    df = pd.DataFrame(e_data, columns=columns) if columns and e_data else pd.DataFrame()

    return table_name, df


def create_list_of_the_parsed_tables(ordnerpfad_mit_den_vdv_files):
    # Erstelle
    # den absoluten Pfad zum Zielverzeichnis
    abs_directory_path = ordnerpfad_mit_den_vdv_files
    # Erstelle ein Pattern, um alle .x10 Dateien in diesem Verzeichnis zu finden
    search_pattern = os.path.join(abs_directory_path, '*.x10')
    # Finde alle Dateien, die dem Pattern entsprechen
    x10_files = glob.glob(search_pattern)


    alle_tabellen = {}
    for datei in x10_files:
        table_name, df = import_vdv451_file(datei)
        alle_tabellen[table_name] = df
        print("Parsed file ", datei)

    print("fertschhh")
    return alle_tabellen


def pandas_magic_versuch2():

    # TODO ich ignoriere überall den ONR_TYP_NR - ich weiß noch nicht, ob wir ihn brauchen (also er wird halt für die Indizierung berücksichtigt aber für beide Typen generiere ich Stations halt)


    scenario = Scenario(name="VDV Import Scenario")

    # TODO wie soll ich das in die Sessions bekommen???
    #
    #  session.add(scenario)

    path = os.path.abspath('UVG')
    alle_tabellen_dict = create_list_of_the_parsed_tables(path)

    # Teil 1: Erstmal die aktuelle Version rausziehen
    today_int = int(
        datetime.today().strftime('%Y%m%d'))  # Today's date as integer, eg. 20220301 for the 1st of March 2022

    versions_df = alle_tabellen_dict['BASIS_VER_GUELTIGKEIT']

    # # HH
    # versions_df.loc[len(versions_df.index)] = [20240322, 1234]
    # versions_df.loc[len(versions_df.index)] = [20240130, 1237]
    # versions_df.loc[len(versions_df.index)] = [20240321, 1235]
    # versions_df.loc[len(versions_df.index)] = [20240323, 1236]
    #
    # fufu = versions_df.sort_values('VER_GUELTIGKEIT', ascending=False)
    # print(fufu)

    filtered_df = versions_df[versions_df['VER_GUELTIGKEIT'] <= today_int].sort_values('VER_GUELTIGKEIT', ascending=False)

    gueltige_version = filtered_df.iloc[0]['BASIS_VERSION']
    print("Current valid version used for this import: ", gueltige_version)


    # Teil 2: MENGE_FZG_TYP durchgehen und, with respect to the current valid BASIS_VERSION, create the eflips Vehicle Types.
    fahrzeug_typen = alle_tabellen_dict['MENGE_FZG_TYP']
    fahrzeug_typen = fahrzeug_typen[fahrzeug_typen['BASIS_VERSION'] == gueltige_version]

    assignments_fzg_typ_nr_to_eflips_vehicletype = {}
    all_vehicle_types = []

    for index, row in fahrzeug_typen.iterrows():
        vehicle_type = VehicleType(
            scenario=scenario,
            name=row['FZG_TYP_TEXT'],
            name_short=row['STR_FZG_TYP'],

            # Die scheinen NICHT bspw. im UVG Datensatz enthalten zu sein, ergo entweder weg lassen, oder nur übernehmen, falls sie im Dataframe vorhanden sind
            battery_capacity=200, # TODO derzeit noch random
            charging_curve=[[0, 200], [1, 150]], #TODO derzeit noch random
            opportunity_charging_capable=True, # TODO derzeit noch random

            # todo Die scheinen NICHT bspw. im UVG Datensatz enthalten zu sein, ergo entweder weg lassen, oder nur übernehmen, falls sie im Dataframe vorhanden sind
            # consumption = row['VERBRAUCH_DISTANZ'] / 1000, # in VDV 452, the consumption is in Wh/km, but in eflips, it is in kWh/km
        )

        # Although these are not marked as optional in VDV 452, they are not present in some cases, so we only add them if present
        # TODO laenge manchmal 0, das abfedern, weil laenge so keinen sinn macht!?
        if 'FZG_LAENGE' in fahrzeug_typen.columns:
            vehicle_type.length = row['FZG_LAENGE']
        if 'FZG_BREITE' in fahrzeug_typen.columns:
            vehicle_type.width = row['FZG_BREITE'] / 100 # in VDV 452, the width is in cm, but in eflips, it is in meters
        if 'FZG_HOEHE' in fahrzeug_typen.columns:
            vehicle_type.height = row['FZG_HOEHE'] / 100 # in VDV 452, the height is in cm, but in eflips, it is in meters

        all_vehicle_types.append(vehicle_type)
        assignments_fzg_typ_nr_to_eflips_vehicletype[row['FZG_TYP_NR']] = vehicle_type

    print(all_vehicle_types)
    print(assignments_fzg_typ_nr_to_eflips_vehicletype)

    # TODO e mobility oben ergänzen (BAtterie bspw.)

    # Part 3: Create the eflips Vehicles (Fahrzeuge) with respect to the current valid BASIS_VERSION
    fahrzeuge = alle_tabellen_dict['FAHRZEUG']
    fahrzeuge = fahrzeuge[fahrzeuge['BASIS_VERSION'] == gueltige_version]

    assignments_fzg_nr_to_eflips_vehicle = {}
    all_vehicles = []

    for index, row in fahrzeuge.iterrows():
        vehicle = Vehicle(
            scenario=scenario,
            name=str(row['FZG_NR']), # eig ist das nur ein Dezimaler ID Bezeichner, rihtige "namen" haben wir im FAHRZEUG Table nicht, höchstens POLKENN oder FIN könnten wir sonst nehmen?!
            vehicle_type=assignments_fzg_typ_nr_to_eflips_vehicletype[row['FZG_TYP_NR']]
        )
        all_vehicles.append(vehicle)
        assignments_fzg_nr_to_eflips_vehicle[row['FZG_NR']] = vehicle

    # Part 4: Create Stations
    orte = alle_tabellen_dict['REC_ORT']
    orte = orte[orte['BASIS_VERSION'] == gueltige_version]

    assignments_onr_typ_ort_nr_to_eflips_station = {}
    all_stations = []

    # TODO Hier gibt es die geschichte mit den "doppelten Orten", also bspw. wenn in der DB eine haltestelle 2x enthalten ist (vermutlich Hin/Rück usw. die dann verschiedene Lat/Long angaben ja haben, ..)
    # TODO Geom KÖNNTE man hier evtl . auch noch ziehen; manchmal ist es angegeben, manchmal aber auch einfach 0.0 und nutzlos; oder es fehlt die höhe, dann kann ich ja kein 3D-Punkt angeben.. lassen wir es weg?

    for index, row in orte.iterrows():
        station = Station(
            scenario=scenario,
            name=row['ORT_NAME'],
            #geom=...,
            #TODO is_electrified=...,
        )

        all_stations.append(station)
        assignments_onr_typ_ort_nr_to_eflips_station[(row['ONR_TYP_NR'], row['ORT_NR'])] = station



    # Part 5 & 6: Create Lines, Routes and Station assignments
    routes = alle_tabellen_dict['REC_LID']
    routes = routes[routes['BASIS_VERSION'] == gueltige_version]

    assignments_li_nr_to_eflips_line = {}
    all_lines = []

    assignments_li_nr_str_li_var_to_eflips_route = {}
    all_routes = []

    # Note: There may be many different Names for a line, due to different directions, etc. so the given name here is just the line number as string.
    # however, as REC_LID actually contains the Routes Rather than the lines. (Route = Variant of a line)

    lines_array = routes['LI_NR'].unique()

    for li_nr in lines_array:
        line = Line(
            scenario=scenario,
            name=str(li_nr),
        )

        all_lines.append(line)
        assignments_li_nr_to_eflips_line[li_nr] = line



    for index, row in routes.iterrows():



        # a) Routenverlauf zusammenstellen

        dfx = alle_tabellen_dict['LID_VERLAUF'].copy(deep=True)  # need a deep copy as we will alter th
        lid_verlauf_route_df = dfx[(dfx['BASIS_VERSION'] == gueltige_version) &
                                   (dfx['LI_NR'] == row['LI_NR']) &
                                   (dfx['STR_LI_VAR'] == row['STR_LI_VAR'])].sort_values('LI_LFD_NR', ascending=True)

        elapsed_distance = 0
        assocs_pre = []

        # Get elapsed distances & create the assocs:
        # 0. Pre-filter the dataframe of the connections between two stations for faster calculations
        # 1. for the first station, the distance is 0
        # 2. for all other stations, the distance is the sum of the elapsed distance to the previous station and the distance between the previous and the current station

        first_stop = assignments_onr_typ_ort_nr_to_eflips_station[
            (lid_verlauf_route_df.iloc[0]['ONR_TYP_NR'], lid_verlauf_route_df.iloc[0]['ORT_NR'])]
        assocs_pre.append((first_stop, 0))

        dfy = alle_tabellen_dict['REC_SEL']
        dfy = dfy[(dfy['BASIS_VERSION'] == gueltige_version) &
                  (dfy['BEREICH_NR'] == row[
                      'BEREICH_NR'])]  # BEREICH_NR as part of the primary key of the Segment and Route needs to be considered as the route & segments belong to a certain type of transportation (bus, tram, etc.)

        # TODO mit den Überlauferfahrten, die hab ich derzeit gar nicht, brauchen wir die? dz nur REC_SEL aber gibt auch UEB_SEL oder so
        for i in range(1, len(lid_verlauf_route_df)):
            start_ort_nr = lid_verlauf_route_df.iloc[i - 1]['ORT_NR']
            start_onr_typ_nr = lid_verlauf_route_df.iloc[i - 1]['ONR_TYP_NR']

            ziel_ort_nr = lid_verlauf_route_df.iloc[i]['ORT_NR']
            ziel_onr_typ_nr = lid_verlauf_route_df.iloc[i]['ONR_TYP_NR']

            laenge = dfy[(dfy['ONR_TYP_NR'] == start_onr_typ_nr) &
                         (dfy['ORT_NR'] == start_ort_nr) &
                         (dfy['SEL_ZIEL_TYP'] == ziel_onr_typ_nr) &
                         (dfy['SEL_ZIEL'] == ziel_ort_nr)]['SEL_LAENGE'].values[0]

            elapsed_distance = elapsed_distance + laenge
            assocs_pre.append((assignments_onr_typ_ort_nr_to_eflips_station[(ziel_onr_typ_nr, ziel_ort_nr)], elapsed_distance))


        last_stop = assocs_pre[-1][0]
        total_distance = assocs_pre[-1][1]

        # a) das Routen Objekt erstellen

        route = Route(
            scenario=scenario,
            name=row['LIDNAME'],
            departure_station=first_stop,
            arrival_station=last_stop,
            line=assignments_li_nr_to_eflips_line[row['LI_NR']],
            distance=total_distance
        )

        all_routes.append(route)
        assignments_li_nr_str_li_var_to_eflips_route[(row['LI_NR'], row['STR_LI_VAR'])] = route


        # Finally, create the assocs & add them to the route
        assocs = []
        for station, elapsed_distance in assocs_pre:
            assoc = AssocRouteStation(
                scenario=scenario,
                route=route,
                station=station,
                elapsed_distance=elapsed_distance
            )
            assocs.append(assoc)

        route.assoc_route_stations = assocs
        print("Done importing route ", row['LIDNAME'])


    # Part 7: Create the Rotations

    all_rotations = []
    assignments_um_uid_betriebstag_to_eflips_rotation = {} # here we need to map um_uid not with TAGESART_NR, but with the BETRIEBSTAG, as we need to duplicate the rotation for each BETRIEBSTAG associated to the TAGESART_NR (see below..)
    rotations = alle_tabellen_dict['REC_UMLAUF']
    rotations = rotations[rotations['BASIS_VERSION'] == gueltige_version]

    alle_trips = [] # Unabhängig von ihrer Rotation..

    for index, row in rotations.iterrows():

        # Get trip info PER UM_UID & TAGESART_NR combination, ...
        # but the actual trips & stop time Object are created per Rotation object.. TODO ist das so erforderlich oder kann ich quasi das selbe Trip objekt mehrfach verwenden.

        # TODO 1 hier alle Arrivals an den Stops und die Dwell Duration ermitteln.


        # and now duplicate it for every BETRIEBSTAG that belongs texceo the TAGESART_NR:
        # as the rotation is bound to a TAGESART_NR (day type number), we need to duplicate the rotation for each BETRIEBSTAG (service day) in the FIRMENKALENDER Table belonging to this TAGESART_NR

        vehicle_type = assignments_fzg_typ_nr_to_eflips_vehicletype[row['FZG_TYP_NR']]

        days_df = alle_tabellen_dict['FIRMENKALENDER']
        days_df = days_df[(days_df['BASIS_VERSION'] == gueltige_version) &
                  (days_df['TAGESART_NR'] == row[
                      'TAGESART_NR'])]  # BEREICH_NR as part of the primary key of the Segment and Route needs to be considered as the route & segments belong to a certain type of transportation (bus, tram, etc.)

        betriebstage = days_df['BETRIEBSTAG'].unique()
        for betriebstag in betriebstage:
            rotation = Rotation(
                scenario=scenario,
                name=str(row['UM_UID']) + "_" + str(betriebstag),
                vehicle_type=vehicle_type,
                trips = [], # Zunächst leer lassen, "relationships" werden automatisch synchroniseirt
                allow_opportunity_charging=False, #TODO hmm kriegen wir irgendwo die info
            )

            all_rotations.append(rotation)
            assignments_um_uid_betriebstag_to_eflips_rotation[(row['UM_UID'], betriebstag)] = rotation # todo hope we never need the UM_UID<->TAGESART_NR bound

            # TODO 2 Hier die Trips und Stop Times erstellen als Objekte



    # We need to get the arrival and dwell times at the stations (as seconds since route start)
    # these depend on the Fahrzeitgruppe (FGR_NR)

    # TODO im VDV haben die Umläufe ein ausgezeichnetes Start- und Endstation. Eigentlich sollte es aber über die Trips sich ergeben, denke / hoffe ich.
    # TODO  insofern denke ich, dass wir diese angaben nicht brauchen? ich weiß auch nicht wie ich sie überhaupt im Rotation Objekt eintragen sollte.

    # Trips:
    # so VDV 452 knows 4 different types of FAHRTART_NRn:
    # 1: Normalfahrt (Normal trip)
    # 2: Betriebshofausfahrt (Departure from the depot)
    # 3: Betriebshofeinfahrt (Arrival at the depot)
    # 4: Zufahrt (a Route used for Line changes and empty runs)

    # as eFLIPS do only know passenger and empty trips, only the 1st type (Normalfahrt) is considered as PASSENGER, the other types as EMPTY




    # TODO im SQLalchemy dann alles zu die sessions packen?!?!? (habe nirgendwo session add gemacht!!!)
    #TODO umgehen mit files, die ein anderes namens schema haben! Siehe VDV 451(?)






if __name__ == '__main__':
    print("SCHLONG")

    #run_sqlalchemy_magic(vdv_schema.metadata) # TODO schlechter stil, wenn es aus dem externen vdv_schema.py eingebunden?
    pandas_magic_versuch2()



# ==================================================================================







## ALTER CODE::::
def run_sqlalchemy_magic(metadata):

    engine = create_engine('sqlite:///temp_database_somerandomnumberss893434.db')
    metadata.bind = engine # TODO muss das VOR dem anlegen der SQLAlchemy Table objekte passieren?
    metadata.create_all(engine)

    conn = engine.connect()

    # TEST 1
    # Base = declarative_base()
    #
    # class BACHFIS(Base):
    #     __tablename__ = 'BACHFIS'
    #     id = Column(Integer, primary_key=True)
    #     KIRK = Column(DECIMAL(precision=9))
    #
    # # Erstelle ein leeres Dictionary
    # data_types_dict = {}
    #
    # # Iteriere über die Spaltenattribute der Klasse BACHFIS
    # for column in BACHFIS.__table__.columns:
    #     # Füge den Namen der Spalte und den entsprechenden Datentyp dem Dictionary hinzu
    #     data_types_dict[column.name] = column.type
    #
    # print(data_types_dict)

    # END TEST 1

    path = os.path.abspath('UVG')
    alle_tabellen = create_list_of_the_parsed_tables(path)

    wichtige_schemata = {
        'BASIS_VER_GUELTIGKEIT': vdv_schema.sqt_basis_ver_gueltigkeit,
        # 'MENGE_BASIS_VERSIONEN': vdv_schema.sqal_table_menge_basis_versionen,
        'FIRMENKALENDER': vdv_schema.sqal_table_firmenkalender,

        'MENGE_ONR_TYP': vdv_schema.sqal_table_menge_onr_typ,
        'MENGE_ORT_TYP': vdv_schema.sqal_table_menge_ort_typ,
        'REC_HP': vdv_schema.sqal_table_rec_hp,
        'REC_ORT': vdv_schema.sqal_table_rec_ort,

        'FAHRZEUG': vdv_schema.sqal_table_rec_hp,
        'MENGE_BEREICH': vdv_schema.sqal_table_menge_bereich,
        'MENGE_FZG_TYP': vdv_schema.sqal_table_menge_fzg_typ,

        'REC_SEL': vdv_schema.sqal_table_rec_sel,
        'MENGE_FGR': vdv_schema.sqal_table_menge_fgr,
        'ORT_HZTF': vdv_schema.sqt_ort_hztf,
        'SEL_FZT_FELD': vdv_schema.sqt_sel_fzt_feld,
        'REC_UEB': vdv_schema.sqt_rec_ueb,
        'UEB_FZT': vdv_schema.sqt_ueb_fzt,

        'LID_VERLAUF': vdv_schema.sqt_lid_verlauf,
        'REC_LID': vdv_schema.sqt_rec_lid,
        'REC_FRT': vdv_schema.sqt_rec_frt,
        'REC_FRT_HZT': vdv_schema.sqt_rec_frt_hzt,
        'REC_UMLAUF': vdv_schema.sqt_rec_umlauf,



    }



    for tabelle in alle_tabellen:

        tabellenname = tabelle.table_name
        if tabellenname in wichtige_schemata.keys(): #FUND
            sqal_table_schema = wichtige_schemata[tabellenname]


        else:
            continue

        # Schema ziehen:
        # DataFrame-Spalten automatisch aus dem Schema extrahieren
        inspector = sqlalchemy.inspect(sqal_table_schema)
        columns_info = inspector.columns

        colnames_extra = [] # will hold the column names as a list, as using column_keys directly yields an Index.
        the_schema = {}
        for key in columns_info.keys():
            type = columns_info[key].type
            the_schema[key] = type

            colnames_extra.append(key)


        print(the_schema)
        print(colnames_extra)

        # Inserts the Dataframe into the SQLite Database, using the defined schema for the table.
        # TODO Columns that are in the dataframe (respective, in the input data), but not in the schema, will NOT be discarded beforehand, but appear in the SQLite file. Ignore this (currenty) or somehow discard them (seems to be bit tricky)
        tabelle.df.to_sql(tabelle.table_name, engine, index=False, if_exists='replace', dtype=the_schema)






    #os.remove('temp_database_somerandomnumberss893434.db')
    today_int = int(datetime.today().strftime('%Y%m%d')) # Today's date as integer, eg. 20220301 for the 1st of March 2022
    stmt1 = vdv_schema.sqt_basis_ver_gueltigkeit.select().where(vdv_schema.sqt_basis_ver_gueltigkeit.c.ver_gueltigkeit <= today_int)
    erg = conn.execute(stmt1).fetchall()

    print(erg)