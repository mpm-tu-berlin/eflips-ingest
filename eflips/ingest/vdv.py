# Anpassung des Skripts zur Berücksichtigung der spezifischen Anforderungen:
from datetime import datetime

import pandas as pd

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
        #dtype = dtype_map[type_info]
        dtypes.append(type_info)#dtype)
    return dtypes
def import_vdv451_file(file_path):
    encoding = 'ISO-8859-1'
    date_format = time_format = None
    columns = []
    formats = []

    e_data = []

    with open(file_path, 'r', encoding=encoding) as f:
        for line in f:
            parts = line.strip().split(';')
            command = parts[0]

            if command == 'mod':
                # Extrahiere das Datum- und Zeitformat
                date_format = parts[1]
                time_format = parts[2]

            elif command == 'atr':
                # Spaltennamen definieren
                columns = parts[1:]

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
    for row in range(0,len(e_data)):
        for col in range(0, len(e_data[row])):
            if datatypes[col] == 'num':
                e_data[row][col] = int(e_data[row][col])

    # Erstelle ein DataFrame, wenn Spalten und Daten vorhanden sind
    df = pd.DataFrame(e_data, columns=columns) if columns and e_data else pd.DataFrame()
    return df


# Pfad zur Beispiel-Datei
file_path_example = 'firmenkalender.x10'

# Importiere die Datei und erstelle das DataFrame
df_imported = import_vdv451_file(file_path_example)

# Zeige die ersten Zeilen des DataFrame
print(df_imported.head())