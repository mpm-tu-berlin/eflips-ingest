# Anpassung des Skripts zur Berücksichtigung der spezifischen Anforderungen:
import csv
import glob
import os
from datetime import datetime

import sqlalchemy

import vdv452data
from typing import List

from sqlalchemy import create_engine
import xsdata.formats.dataclass.parsers.json #todo ist derzeit nur ne entwickler dependency
import xsdata.formats.dataclass.parsers.config # todo AGAIN!!! s.a.


import pandas as pd
from dataclasses import dataclass

import vdv_schema

@dataclass
class ParsedTable:
    """Class for keeping track of xyz."""
    table_name: str
    df: pd.DataFrame

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

    return ParsedTable(df=df, table_name=table_name)


def create_list_of_the_parsed_tables(ordnerpfad_mit_den_vdv_files):
    # Erstelle
    # den absoluten Pfad zum Zielverzeichnis
    abs_directory_path = ordnerpfad_mit_den_vdv_files
    # Erstelle ein Pattern, um alle .x10 Dateien in diesem Verzeichnis zu finden
    search_pattern = os.path.join(abs_directory_path, '*.x10')
    # Finde alle Dateien, die dem Pattern entsprechen
    x10_files = glob.glob(search_pattern)


    alle_tabellen = []
    for datei in x10_files:
        alle_tabellen.append(import_vdv451_file(datei))
        print("Parsed file ", datei)

    print("fertschhh")
    return alle_tabellen

def run_sqlalchemy_magic(metadata):

    engine = create_engine('sqlite:///temp_database_somerandomnumberss893434.db')
    metadata.bind = engine # TODO muss das VOR dem anlegen der SQLAlchemy Table objekte passieren?
    metadata.create_all(engine)

    # conn = engine.connect()

    path = os.path.abspath('UVG')
    alle_tabellen = create_list_of_the_parsed_tables(path)

    wichtige_schemata = {
        'BASIS_VER_GUELTIGKEIT': vdv_schema.sqal_table_basis_ver_gueltigkeit,
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




if __name__ == '__main__':
    print("SCHLONG")

    run_sqlalchemy_magic(vdv_schema.metadata) # TODO schlechter stil, wenn es aus dem externen vdv_schema.py eingebunden?


def ALT_ansatz_xsdata():
    path = os.path.abspath('UVG')
    alle_tabellen = create_list_of_the_parsed_tables(path)

    # overwrite default behavior of failing on (additional) non-vdv452 properties that possibly where added by the data provider
    config = xsdata.formats.dataclass.parsers.config.ParserConfig(fail_on_unknown_properties=False)


    parser = xsdata.formats.dataclass.parsers.JsonParser(config=config)
    wichtigste_tabellen_klassennamen = [vdv452data.RecLid, vdv452data.RecFrt, vdv452data.RecFrtHzt, vdv452data.RecUmlauf, vdv452data.LidVerlauf,
                                        vdv452data.Fahrzeug]
    wichtigste_tabellen_tabellennamen = [x.Meta.name for x in wichtigste_tabellen_klassennamen]
    #mappings_tables_classes = [(x, x.Meta.name) for x in wichtigen_objekte]
    # the above is like:
    #[
    #    (RecLid, 'REC_LID')
    #    (RecFrt, 'REC_FRT')
    #    (RecFrtHzt, 'REC_FRT_HZT')

    #]

    for tabelle in alle_tabellen:
        if wichtigste_tabellen_tabellennamen.count(tabelle.table_name) > 0: #FUND
            index = wichtigste_tabellen_tabellennamen.index(tabelle.table_name)
            klasse = wichtigste_tabellen_klassennamen[index]
            #if tabelle.table_name == 'REC_LID':
                #print(tabelle.df.head())
            json = tabelle.df.to_json(orient='records')
            #print(json)

            # parse mit xsdata das json
            parsed = parser.from_string(json, List[klasse])
            print(parsed[:5]) #max print first 5