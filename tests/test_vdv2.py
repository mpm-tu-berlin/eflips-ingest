from pathlib import Path

import pytest

from eflips.ingest.vdv2 import stuff, check_vdv451_file_header, EingangsdatenTabelle


def abspath_to_testfile(testfile_name: str) -> Path:
    # Erstelle einen Pfad zum aktuellen Arbeitsverzeichnis
    current_directory = Path.cwd()

    # Pfad zum untergeordneten Ordner "test_vdv2_files"
    testfile_directory = current_directory / "test_vdv2_files"

    # Pfad zur Datei testfile_name im untergeordneten Ordner "test_vdv2_files"
    x10_file_path = testfile_directory / testfile_name

    # Erhalte den absoluten Pfad
    absolute_path = x10_file_path.resolve()

    return absolute_path


def test_stuff():
    with pytest.raises(ValueError):
        assert stuff(43) == 42
    with pytest.raises(ValueError):
        assert stuff("lshfrsdjkh<fakj") == 42

    assert stuff(42) == 42


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

# todo für die anderen Tests ergänzen.