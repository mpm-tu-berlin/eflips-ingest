import glob
import os
import csv
import enum
from dataclasses import dataclass
from typing import Dict


class VDV_Table_Name(enum.Enum):
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


# TODO problem mit den enum ansatz war, dass ich nicht mehr prüfen kann, ob eine Tabelle überhaupt in diesem Enum ist, weil ich nicht auf etwas zugreifen kann, was es nicht gibt (per key), außerdem inconvenient.

# VDV_Table_Names = [
#     "MENGE_BASIS_VERSIONEN",
#     "BASIS_VER_GUELTIGKEIT",
#     "FIRMENKALENDER",
#     "MENGE_TAGESART",
#     "MENGE_ONR_TYP",
#     "MENGE_ORT_TYP",
#     "REC_HP",
#     "REC_OM",
#     "REC_ORT",
#     "FAHRZEUG",
#     "ZUL_VERKEHRSBETRIEB",
#     "MENGE_BEREICH",
#     "MENGE_FZG_TYP",
#     "REC_ANR",
#     "REC_ZNR",
#     "REC_SEL",
#     "REC_SEL_ZP",
#     "MENGE_FGR",
#     "ORT_HZTF",
#     "SEL_FZT_FELD",
#     "REC_UEB",
#     "UEB_FZT",
#     "MENGE_FAHRTART",
#     "LID_VERLAUF",
#     "REC_LID",
#     "REC_FRT",
#     "REC_FRT_HZT",
#     "REC_UMLAUF",
# ]


@dataclass
class EingangsdatenTabelle:
    abs_file_path: str
    character_set: str
    table_name: VDV_Table_Name


def check_vdv451_file_header(abs_file_path: str) -> EingangsdatenTabelle:
    """
    Checks the contents of a VDV 451 (.x10) file, TODO
    :param file_path: The ABSOLUTE path to the VDV 451 file
    :return: TODO

    """

    # 1. Open file and recognize the encoding
    # For VDV 451, either ASCII or ISO8859-1 is allowed as encoding for the table datasets. However, the header is always ASCII (see Ch. 4.1 of VDV 451).
    # Therefore, we open the file with ASCII TODO ja?!? encoding and check which of the two encodings is used for the table datasets
    # (and return an error if it is not ASCII or ISO8859-1).

    table_name = None
    character_set = None

    valid_character_sets = ["ASCII", "ISO8859-1"]

    try:
        with open(
            abs_file_path, "r", encoding="ISO8859-1"
        ) as f:  # TODO @Besprechen mit LH Eigentlich ASCII- problem: die Unicode Decoding Error soll eig. nur kommen, wenn im HEADER was non ascii steht, aber wie krieg ich das bitte raus.
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
                    table_name = parts[1].upper().strip()

                elif command == "chs":
                    # Get the character set used in the file
                    character_set = parts[1].upper().strip()
                    if character_set not in valid_character_sets:
                        raise ValueError(
                            "The file",
                            abs_file_path,
                            " uses an encoding that is not allowed according to the VDV 451 specification:"
                            + character_set
                            + " does not match 'ASCII' or 'ISO8859-1'.",
                        )

                # TODO: wenn tbl; kommt, evtl. Zeilennummer merken & direkt abbrechen

    except UnicodeDecodeError as e:
        e.add_note("The header of the file",
            abs_file_path,
            " is using an encoding that contains non-ASCII characters. This is not allowed according to the VDV 451 specification.",)
        raise e

    # Raise an error if table name or encoding is not found
    if table_name is None:
        print("The file", abs_file_path, " does not contain a table name in the header.")
        raise ValueError("The file", abs_file_path, " does not contain a table name in the header.")
    if character_set is None:
        print("The file", abs_file_path, " does not contain a character set in the header.")
        raise ValueError("The file", abs_file_path, " does not contain a character set in the header.")

    if table_name not in [x.value for x in VDV_Table_Name]:
        # todo erstmal valueEror fuer alles - wir können überlegen, ob wir danach nur alle syntaktisch korrekten "Tables" weiterverarbeiten oder ob wir die gültigen, aber
        raise ValueError("The file", abs_file_path, " contains an unknown table name: ", table_name, " Skipping it.")

    # TODO @LH der header ist tatsächlich immer ASCII. Deswegen muss ich kein "erneut öffnen" machen,
    return EingangsdatenTabelle(abs_file_path=abs_file_path, character_set=character_set, table_name=table_name)


def validate_input_data_vdv_451(abs_path_to_folder_with_vdv_files: str) -> dict[VDV_Table_Name, EingangsdatenTabelle]:
    """
    Checks if the given directory contains all necessary .x10 files (necessary as in VDV 451/452 specified)
    and xxx.
    :param abs_path_to_folder_with_vdv_files: The ABSOLUTE path to the directory containing the VDV 451 files
    :return: TBC TODO
    """

    # Create a Pattern to find all .x10 Files in this directory
    search_pattern = os.path.join(abs_path_to_folder_with_vdv_files, "*.x10")

    # Find all files that match this pattern.
    x10_files = glob.glob(search_pattern)

    # Iterate through the files, checking whether the neccessary tables are present

    # VDV 451 has two naming schemes. One is the name of the table directly as the name, the other is some number combination,
    # see VDV 451 Chapter 3.1 and 3.2. However, as the name of the table is also included in the file contents, we instead
    # check the contents of each file to determine to which table it belongs.

    all_tables: dict[VDV_Table_Name, EingangsdatenTabelle] = {}
    for abs_file_path in x10_files:
        try:
            eingangsdatentable: EingangsdatenTabelle = check_vdv451_file_header(abs_file_path)

            # Check if the table name is already present in the dictionary (would mean duplicate, two times the same table in the files)
            if eingangsdatentable.table_name in all_tables.keys():
                raise ValueError(
                    "The table ", eingangsdatentable.table_name, " is present in multiple files. Aborting."
                )

            else:
                all_tables[eingangsdatentable.table_name] = eingangsdatentable

        except (ValueError, UnicodeDecodeError) as e:
            print(
                "While processing ", abs_file_path, " the following exception occurred:", e
            )  # todo aufsplitten.. ?!
            continue

    # Now check if we have all necessary tables
    # @Besprechen mit LH TODO beschraenken wir uns auf die Tables, die wie für unser eflips brauchen, oder ALLE, die gem. VDV451 notwendig sind?
    # Weil das PRoblem ist, angeblich ist ein VDV Datensatz bspw. auch ohne Umlauf grundsätzlich zulässig (S.67 der VDV 452 doku ganz unten)

    # required_tables = [x for x in VDV_Table_Names] # todo erstmal ALLE notwendig, schmeissen sie gleich raus

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
    # TODO: was machen wir mit den 14 E-Mobilitäts Tabellen aus der VDV (Kapitel 11.6 - 11.14)

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

    # fahrzeug waere optional, aber machen wir nicht? TODO @LH
    required_tables = [
        "BASIS_VER_GUELTIGKEIT",
        "FIRMENKALENDER",
        "REC_ORT",
        "MENGE_FZG_TYP",
        "REC_SEL",
        "SEL_FZT_FELD",
        "LID_VERLAUF",
        "REC_FRT",
        "REC_UMLAUF",
        "REC_LID", # hmm

    ]


    if not set(required_tables) <= set(all_tables.keys()):
        # Compute all tables that are required but not in the tables in the files, to display them to the user
        missing_tables = set(required_tables) - set(all_tables.keys())
        missing_tables_str = " ".join([x + ", " for x in missing_tables])
        raise ValueError(
            "Not all necessary tables are present in the directory. Missing tables are: ",
            missing_tables_str,
            " Aborting.",
        )

    # Either REC_FRT_HZT or ORT_HZTF must be present, not both(?)

    if ('REC_FRT_HZT' in all_tables.keys()) and ('ORT_HZTF' in all_tables.keys()):
        # Both tables present...
        raise ValueError("Either REC_FRT_HZT or ORT_HZTF must be present in the dataset, but both are present. Aborting.")

    if ('REC_FRT_HZT' not in all_tables.keys()) and ('ORT_HZTF' not in all_tables.keys()):
        # Gar keine Haltezeiten dabei
        raise ValueError("Neither REC_FRT_HZT nor ORT_HZTF present in the directory. Aborting.")

    print("All necessary tables are present in the directory.")
    return all_tables


if __name__ == "__main__":
    # TODO UVG example geht NICHT, weil bereits dort 'ISO-8859-1' steht statt 'ISO8859-1'
    all_tables = validate_input_data_vdv_451("C:\\Users\\Studium\\PycharmProjects\\eflips-ingest\\eflips\\ingest\\Vogtland")




def stuff(a: int) -> int:
    if a != 42:
        raise ValueError("a must be 42")
    return 42


if __name__ == "__main__":
    stuff(42)
