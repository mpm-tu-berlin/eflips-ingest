import os.path
from pathlib import Path

import pytest

from eflips.ingest.vdv2 import check_vdv451_file_header, EingangsdatenTabelle, validate_input_data_vdv_451


def abspath_to_testfile(testfile_name: str) -> Path:
    # Erstelle einen Pfad zum aktuellen Arbeitsverzeichnis
    path_of_this_file = Path(os.path.dirname(__file__))

    # Pfad zum untergeordneten Ordner "test_vdv2_files"
    testfile_directory = path_of_this_file / "test_vdv2_files"

    # Pfad zur Datei testfile_name im untergeordneten Ordner "test_vdv2_files"
    x10_file_path = testfile_directory / testfile_name

    # Erhalte den absoluten Pfad
    absolute_path = x10_file_path.resolve()

    return absolute_path


def test_valid_file():
    absolute_path = abspath_to_testfile("FAHRZEUG_valid.X10")
    erg = check_vdv451_file_header(absolute_path)
    assert type(erg) == EingangsdatenTabelle


def test_wrong_encoding():
    absolute_path = abspath_to_testfile("invalid_charset.X10")
    with pytest.raises(ValueError):
        check_vdv451_file_header(absolute_path)


def test_missing_charset():
    absolute_path = abspath_to_testfile("missing_charset.X10")
    with pytest.raises(ValueError):
        check_vdv451_file_header(absolute_path)


def test_missing_table_name():
    absolute_path = abspath_to_testfile("missing_table_name.X10")
    with pytest.raises(ValueError):
        check_vdv451_file_header(absolute_path)


def test_completely_different_content():
    absolute_path = abspath_to_testfile("komplett_anderer_inhalt.X10")
    with pytest.raises(ValueError):
        check_vdv451_file_header(absolute_path)


def test_load_for_sample_data() -> None:
    """
    Take the sample data from the PROJECT_ROOT/samples/VDV subfolders and test the load function.
    :return: None
    """
    path_of_this_file = Path(os.path.dirname(__file__))
    sample_data_directory = path_of_this_file / ".." / "samples" / "VDV"

    for directory in sample_data_directory.iterdir():
        if directory.is_dir():
            validate_input_data_vdv_451(directory)


# todo für die anderen Tests ergänzen.
