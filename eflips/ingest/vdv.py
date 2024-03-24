import csv
import glob
import os
from datetime import datetime, timedelta
import time

from sqlalchemy.orm import declarative_base
from sqlalchemy import create_engine, Column, Integer, DECIMAL

import pandas as pd


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

    # todo evtl möglichkeit einbauen, manuell die
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
    # TODO this code technically works; however, we dont neccessarily have a "FAHRZEUG" table, so should we leave this out?
    # fahrzeuge = alle_tabellen_dict['FAHRZEUG']
    # fahrzeuge = fahrzeuge[fahrzeuge['BASIS_VERSION'] == gueltige_version]
    #
    # assignments_fzg_nr_to_eflips_vehicle = {}
    # all_vehicles = []
    #
    # for index, row in fahrzeuge.iterrows():
    #     vehicle = Vehicle(
    #         scenario=scenario,
    #         name=str(row['FZG_NR']), # eig ist das nur ein Dezimaler ID Bezeichner, rihtige "namen" haben wir im FAHRZEUG Table nicht, höchstens POLKENN oder FIN könnten wir sonst nehmen?!
    #         vehicle_type=assignments_fzg_typ_nr_to_eflips_vehicletype[row['FZG_TYP_NR']]
    #     )
    #     all_vehicles.append(vehicle)
    #     assignments_fzg_nr_to_eflips_vehicle[row['FZG_NR']] = vehicle

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

    # Prepare REC_SEL Table
    # BEREICH_NR as part of the primary key of the Segment and Route needs to be considered as the route & segments belong to a certain type of transportation (bus, tram, etc.)
    dfy = alle_tabellen_dict['REC_SEL'].set_index(['BASIS_VERSION', 'BEREICH_NR', 'ONR_TYP_NR', 'ORT_NR', 'SEL_ZIEL_TYP', 'SEL_ZIEL']).sort_index()

    # fürn linienverlauf:
    # (sorting the index will also already gives us "ascending LFD_NR" in the trip order)
    dfx = alle_tabellen_dict['LID_VERLAUF'].set_index(
        ['BASIS_VERSION', 'LI_NR', 'STR_LI_VAR', 'LI_LFD_NR']).sort_index()

    for index, row in routes.iterrows():

        # a) Routenverlauf zusammenstellen

        lid_verlauf_route_df = dfx.loc[gueltige_version, row['LI_NR'], row['STR_LI_VAR']].copy(deep=True)

        elapsed_distance = 0
        assocs_pre = []

        # Get elapsed distances & create the assocs:
        # 0. Pre-filter the dataframe of the connections between two stations for faster calculations
        # 1. for the first station, the distance is 0
        # 2. for all other stations, the distance is the sum of the elapsed distance to the previous station and the distance between the previous and the current station

        first_stop = assignments_onr_typ_ort_nr_to_eflips_station[
            (lid_verlauf_route_df.iloc[0]['ONR_TYP_NR'], lid_verlauf_route_df.iloc[0]['ORT_NR'])]
        assocs_pre.append((first_stop, 0))


        # TODO mit den Überlauferfahrten, die hab ich derzeit gar nicht, brauchen wir die? dz nur REC_SEL aber gibt auch UEB_SEL oder so
        for i in range(1, len(lid_verlauf_route_df)):
            start_ort_nr = lid_verlauf_route_df.iloc[i - 1]['ORT_NR']
            start_onr_typ_nr = lid_verlauf_route_df.iloc[i - 1]['ONR_TYP_NR']

            ziel_ort_nr = lid_verlauf_route_df.iloc[i]['ORT_NR']
            ziel_onr_typ_nr = lid_verlauf_route_df.iloc[i]['ONR_TYP_NR']

            laenge = dfy.loc[(gueltige_version, row['BEREICH_NR'], start_onr_typ_nr, start_ort_nr,
                        ziel_onr_typ_nr, ziel_ort_nr), 'SEL_LAENGE']

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

    alle_trips = [] # Unabhängig von ihrer Rotation gesammelt hier

    all_rotations_eflips_obj = []

    # Bereichsnummer nötig für die Selektion der Fahrzeiten; die kriegen wir aus der Linien-Tabelle REC_LID
    dfa = alle_tabellen_dict['REC_LID'].set_index(['BASIS_VERSION', 'LI_NR', 'STR_LI_VAR']).sort_index()

    # Analog für Fahrzeug das vorbereiten
    dfz = alle_tabellen_dict['SEL_FZT_FELD'].set_index(
        ['BASIS_VERSION', 'BEREICH_NR', 'FGR_NR', 'ONR_TYP_NR', 'ORT_NR', 'SEL_ZIEL_TYP', 'SEL_ZIEL']).sort_index()

    # und fürn linienverlauf:
    # (sorting the index will also already gives us "ascending LFD_NR" in the trip order)
    dfx = alle_tabellen_dict['LID_VERLAUF'].set_index(['BASIS_VERSION', 'LI_NR', 'STR_LI_VAR', 'LI_LFD_NR']).sort_index()

    # fuer die Dwell Times:

    # 1. Dwell times per FAHRT
    if 'REC_FRT_HZT' in alle_tabellen_dict.keys():
        df_frt_hzt = alle_tabellen_dict['REC_FRT_HZT'].set_index(['BASIS_VERSION', 'FRT_FID', 'ONR_TYP_NR', 'ORT_NR']).sort_index()
    else:
        df_frt_hzt = None

    # 2. Dwell times per Fahrzeitgruppe (FGR_NR)
    if 'ORT_HZTF' in alle_tabellen_dict.keys():
        df_ort_hztf = alle_tabellen_dict['ORT_HZTF'].set_index(
            ['BASIS_VERSION', 'FGR_NR', 'ONR_TYP_NR', 'ORT_NR']).sort_index()
    else:
        df_ort_hztf = None

    for index, row in rotations.iterrows():

        zeitanfang = time.time()  # mal laufzeit analyse für performance verbesserung ziel

        # Get trip info PER UM_UID & TAGESART_NR combination, ...
        # but the actual trips & stop time Object are created per Rotation object.. TODO ist das so erforderlich oder kann ich quasi das selbe Trip objekt mehrfach verwenden.


        # Alle Fahrten ermitteln, die zu dieser Rotation gehören
        df_frt = alle_tabellen_dict['REC_FRT']
        df_frt = df_frt[(df_frt['BASIS_VERSION'] == gueltige_version) &
                        (df_frt['UM_UID'] == row['UM_UID']) &
                        (df_frt['TAGESART_NR'] == row['TAGESART_NR'])]

        trips_pre = []

        for _, row_frt in df_frt.iterrows():
            lid_verlauf_route_df = dfx.loc[gueltige_version, row_frt['LI_NR'], row_frt['STR_LI_VAR']].copy(deep=True)
            trip_start_seconds_since_midnight = row_frt['FRT_START']
            stop_times_trip_pre = [] # collect them as list of dicts as I need to crete multiple StopTimes object with different arrival_times later; so I will have no problems with "copying/altering" the objects .

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

            # In VDV, there are two possibilities for Stop Times:
            # 1: In the ORT_HZTF: Stop Times for a FGR_NR (Fahrtzeitgruppe)
            # 2: In the REC_FRT_HZT: Stop Times for a FRT_FID (Fahrt)
            # I have no concrete info what happens if both are given, but currently I saw always only one of those Tables existent or filled with info..
            # Therefore I assume (TODO check if that is correct?) to first look in the REC_FRT_HZT and if nothing is found, then look in the ORT_HZTF, so that
            # a Trip-Specific Stop Time overwrites the general Stop Time for a FGR_NR.?

            arrival_time = trip_start_seconds_since_midnight # We need this to keep track of "time elapsed", so that we can calculate arrival time at every station.

            # Add the first stop time
            first_stop = assignments_onr_typ_ort_nr_to_eflips_station[
                (lid_verlauf_route_df.iloc[0]['ONR_TYP_NR'], lid_verlauf_route_df.iloc[0]['ORT_NR'])]
            stop_times_trip_pre.append({'station': first_stop, 'arrival_time': arrival_time, 'dwell_time': 0})

            # Bereichsnummer nötig für die Selektion der Fahrzeiten; die kriegen wir aus der Linien-Tabelle REC_LID
            bereichsnr = dfa.loc[(gueltige_version, row_frt['LI_NR'], row_frt['STR_LI_VAR']), 'BEREICH_NR']

            # get the stop times for the other stops
            for i in range(1, len(lid_verlauf_route_df)):
                start_ort_nr = lid_verlauf_route_df.iloc[i - 1]['ORT_NR']
                start_onr_typ_nr = lid_verlauf_route_df.iloc[i - 1]['ONR_TYP_NR']

                ziel_ort_nr = lid_verlauf_route_df.iloc[i]['ORT_NR']
                ziel_onr_typ_nr = lid_verlauf_route_df.iloc[i]['ONR_TYP_NR']

                fahrzeit = dfz.loc[(gueltige_version, bereichsnr, row_frt['FGR_NR'], start_onr_typ_nr, start_ort_nr, ziel_onr_typ_nr, ziel_ort_nr)]['SEL_FZT'].iloc[0]

                arrival_time = arrival_time + fahrzeit

                # Check for dwell time
                # I assume to first look if there is a specific dwell time for the trip (FRT_FID) and if not, then look for the general dwell time for the FGR_NR
                dwell_time = 0

                # TODO wenn nur eine Tabelle vorhanden ist in der kram drin steht, aber ich finde meinen Ort da nicht, ist die Haltezeit dann 0?!? Habe ich im UVG Beispiel so..
                if df_frt_hzt is not None:
                    if (gueltige_version, row_frt['FRT_FID'], ziel_onr_typ_nr, ziel_ort_nr) in df_frt_hzt.index:
                        dwell_time = df_frt_hzt.loc[(gueltige_version, row_frt['FRT_FID'], ziel_onr_typ_nr, ziel_ort_nr)]['FRT_HZT_ZEIT']


                elif df_ort_hztf is not None:
                    if ((gueltige_version, row_frt['FGR_NR'], ziel_onr_typ_nr, ziel_ort_nr)) in df_ort_hztf.index:
                        dwell_time = df_ort_hztf.loc[(gueltige_version, row_frt['FGR_NR'], ziel_onr_typ_nr, ziel_ort_nr)]['HP_HZT']


                stop_times_trip_pre.append({'station': assignments_onr_typ_ort_nr_to_eflips_station[(ziel_onr_typ_nr, ziel_ort_nr)], 'arrival_time': arrival_time, 'dwell_time': dwell_time})
                arrival_time = arrival_time + dwell_time # important for the next iteration ("start time at last station = arrival time at last + dwell time there"), very important to do this AFTER putting the StopTime in the list

            triptype = TripType.PASSENGER if row_frt['FAHRTART_NR'] == 1 else TripType.EMPTY
            trip_route = assignments_li_nr_str_li_var_to_eflips_route[(row_frt['LI_NR'], row_frt['STR_LI_VAR'])]

            trips_pre.append({'trip_type': triptype,
                              'departure_time': trip_start_seconds_since_midnight,
                              'arrival_time': arrival_time,
                              'stop_times': stop_times_trip_pre,
                              'route': trip_route})

        # and now duplicate it for every BETRIEBSTAG that belongs texceo the TAGESART_NR:
        # as the rotation is bound to a TAGESART_NR (day type number), we need to duplicate the rotation for each BETRIEBSTAG (service day) in the FIRMENKALENDER Table belonging to this TAGESART_NR

        #print("verlauf gen: sec ", time.time() - zeitanfang)
        vehicle_type = assignments_fzg_typ_nr_to_eflips_vehicletype[row['FZG_TYP_NR']]

        days_df = alle_tabellen_dict['FIRMENKALENDER']
        days_df = days_df[(days_df['BASIS_VERSION'] == gueltige_version) &
                  (days_df['TAGESART_NR'] == row[
                      'TAGESART_NR'])]  # BEREICH_NR as part of the primary key of the Segment and Route needs to be considered as the route & segments belong to a certain type of transportation (bus, tram, etc.)

        betriebstage = days_df['BETRIEBSTAG'].unique() # alle Betriebstage, die DIESE TAGESART_NR haben, ..
        for betriebstag in betriebstage:
            rotation = Rotation(
                scenario=scenario,
                name=str(row['UM_UID']) + "_" + str(betriebstag),
                vehicle_type=vehicle_type,
                trips = [], # Zunächst leer lassen, "relationships" werden automatisch synchroniseirt TODO stimmt es ? war so aus dem tutorial!"
                allow_opportunity_charging=False, #TODO hmm kriegen wir irgendwo die info
            )

            all_rotations.append(rotation)
            assignments_um_uid_betriebstag_to_eflips_rotation[(row['UM_UID'], betriebstag)] = rotation # todo hope we never need the UM_UID<->TAGESART_NR bound

            # TODO 2 Hier die Trips und Stop Times erstellen als Objekte
            trips = []
            betriebstag_date_midnight = datetime.strptime(str(betriebstag), "%Y%m%d")
            for trip_pre in trips_pre:
                trips.append(
                    Trip(
                        scenario=scenario,
                        route=trip_pre['route'],
                        trip_type=trip_pre['trip_type'],
                        departure_time = betriebstag_date_midnight + timedelta(seconds=int(trip_pre['departure_time'])),
                        arrival_time = betriebstag_date_midnight + timedelta(seconds=int(trip_pre['arrival_time'])),
                        rotation=rotation,
                    )
                )

                stop_times = []
                for stop_time_pre in trip_pre['stop_times']:
                    stop_times.append(
                        StopTime(
                            scenario=scenario,
                            station=stop_time_pre['station'],
                            dwell_duration=timedelta(seconds=int(stop_time_pre['dwell_time'])),
                            arrival_time = betriebstag_date_midnight + timedelta(seconds=int(stop_time_pre['arrival_time'])),
                        )
                    )
                trips[-1].stop_times = stop_times

            rotation.trips = trips

            all_rotations_eflips_obj.append(rotation)

        zeitende = time.time()
        print("Done importing rotation ", row['UM_UID'], " ", row['TAGESART_NR'], " ,took ", zeitende - zeitanfang, " seconds")

    #TODO 3 wir müssen noch die Trips für Betriebshofzu- und abfahrten usw. verarbeiten, siehe S. 65 VDV 452 Doku



    # TODO im SQLalchemy dann alles zu die sessions packen?!?!? (habe nirgendwo session add gemacht!!!)
    #TODO umgehen mit files, die ein anderes namens schema haben! Siehe VDV 451(?)






if __name__ == '__main__':
    print("SCHLONG")

    #run_sqlalchemy_magic(vdv_schema.metadata) # TODO schlechter stil, wenn es aus dem externen vdv_schema.py eingebunden?
    pandas_magic_versuch2()



# ==================================================================================

