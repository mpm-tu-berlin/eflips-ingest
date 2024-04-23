import csv
import enum
import glob
import logging
import os
from dataclasses import dataclass
from vdv452data import vdv452_v1_5


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


class VDV_Util:
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
        VDV_Table_Name.BASIS_VER_GUELTIGKEIT: vdv452_v1_5.BasisVerGueltigkeit,
        VDV_Table_Name.FIRMENKALENDER: vdv452_v1_5.Firmenkalender,
        VDV_Table_Name.REC_ORT: vdv452_v1_5.RecOrt,
        VDV_Table_Name.MENGE_FZG_TYP: vdv452_v1_5.MengeFzgTyp,
        VDV_Table_Name.REC_SEL: vdv452_v1_5.RecSel,
        VDV_Table_Name.SEL_FZT_FELD: vdv452_v1_5.SelFztFeld,
        VDV_Table_Name.LID_VERLAUF: vdv452_v1_5.LidVerlauf,
        VDV_Table_Name.REC_FRT: vdv452_v1_5.RecFrt,
        VDV_Table_Name.REC_UMLAUF: vdv452_v1_5.RecUmlauf,
        VDV_Table_Name.REC_LID: vdv452_v1_5.RecLid,  # hmm
    }


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
    # Therefore, we open the file with ISO8859-1
    # (and return an error if it is not ASCII or ISO8859-1).
    logger = logging.getLogger(__name__)

    table_name_str = None
    character_set = None

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

    if table_name_str not in [x.value for x in VDV_Table_Name]:
        raise ValueError(
            "The file" + str(abs_file_path) + " contains an unknown table name: " + table_name_str + " Skipping it."
        )

    return EingangsdatenTabelle(
        abs_file_path=abs_file_path, character_set=character_set, table_name=VDV_Table_Name[table_name_str]
    )


def import_vdv452_table_records(EingangsdatenTabelle: EingangsdatenTabelle) -> None:
    """
    Imports the records of a VDV 451 table into the database.
    :param EingangsdatenTabelle: The EingangsdatenTabelle object containing the table name and the path to the file
    :return: None
    """
    logger = logging.getLogger(__name__)

    # Open the file
    try:
        with open(EingangsdatenTabelle.abs_file_path, "r", encoding=EingangsdatenTabelle.character_set) as f:
            for line in f:
                if line.strip().split(";")[0] == "rec":
                    # For this mode, we need to utilize the CSV reader here in order to get rid of the double quote marks enclosing the strings (otherwise, we would have e.g. '"Templin, ZOB"') etc.
                    parts_csvrdr = csv.reader([line], delimiter=";", skipinitialspace=True)
                    parts = list(parts_csvrdr)[0]

                    row_data = parts[1:]

    except UnicodeDecodeError as e:
        # todo specify more in detail where exactly the unicode error occurred?
        e.add_note(
            "The file"
            + str(EingangsdatenTabelle.abs_file_path)
            + " is using an encoding that contains non-"
            + EingangsdatenTabelle.character_set
            + " characters, thus not matching the specified encoding. Aborting.",
        )
        raise e


def validate_input_data_vdv_451(abs_path_to_folder_with_vdv_files: str) -> dict[VDV_Table_Name, EingangsdatenTabelle]:
    """
    Checks if the given directory contains all necessary .x10 files (necessary as in VDV 451/452 specified)
    and xxx.
    :param abs_path_to_folder_with_vdv_files: The ABSOLUTE path to the directory containing the VDV 451 files
    :return: TBC TODO
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

    all_tables: dict[VDV_Table_Name, EingangsdatenTabelle] = {}
    for abs_file_path in x10_files_unique:
        try:
            eingangsdatentable: EingangsdatenTabelle = check_vdv451_file_header(abs_file_path)

            # Check if the table name is already present in the dictionary (would mean duplicate, two times the same table in the files)
            if eingangsdatentable.table_name in all_tables.keys():
                raise ValueError(
                    "The table " + eingangsdatentable.table_name.value + " is present in multiple files. Aborting."
                )

            else:
                all_tables[eingangsdatentable.table_name] = eingangsdatentable

        except (ValueError, UnicodeDecodeError) as e:
            msg = "While processing " + abs_file_path + " the following exception occurred: "
            logger.warning(msg, exc_info=e)
            continue

    # Required Tables:
    # siehe ganz oben in der File fuer explanation

    if not set(VDV_Util.required_tables.keys()) <= set(all_tables.keys()):
        # Compute all tables that are required but not in the tables in the files, to display them to the user
        missing_tables = set(VDV_Util.required_tables.keys()) - set(all_tables.keys())
        missing_tables_str = " ".join([x.value + ", " for x in missing_tables])
        raise ValueError(
            "Not all necessary tables are present in the directory (or present, but empty). Missing tables are: "
            + missing_tables_str
            + " Aborting.",
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


if __name__ == "__main__":
    path_to_this_file = os.path.dirname(os.path.abspath(__file__))
    sample_files_dir = os.path.join(path_to_this_file, "..", "..", "samples", "VDV", "Trier")
    all_tables = validate_input_data_vdv_451(sample_files_dir)
