import glob
import os.path
from pathlib import Path
from typing import List, Dict, Tuple
from uuid import uuid4, UUID
from zipfile import ZipFile

import eflips.model
import pytest
from eflips.model import Scenario, VehicleType
from eflips.model import create_engine
from sqlalchemy.orm import Session

from eflips.ingest.vdv import (
    validate_input_data_vdv_451,
    VDVTable,
    check_vdv451_file_header,
    VdvIngester,
    import_vdv452_table_records,
)
from tests.base import BaseIngester


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


class TestVdvIngester(BaseIngester):
    @pytest.fixture()
    def ingester(self) -> VdvIngester:
        return VdvIngester(self.database_url)

    @pytest.fixture()
    def vdv_zip_files(self, tmp_path) -> List[Path]:
        """
        Take the $PROJECT_ROOT/samples/VDV subfolders and for each one, create a zip file containing the contents of the
        folder. Return a list of the paths to the created zip files.
        :return: A list of paths to the created zip files.
        """

        paths = []

        # For each of the sample files in the $PROJECT_ROOT$/samples/VDV directory, test the prepare method.
        path_of_this_file = Path(os.path.dirname(__file__))
        sample_data_directory = path_of_this_file / ".." / "samples" / "VDV"
        for directory in sample_data_directory.iterdir():
            if os.path.isfile(directory):
                continue

            # Get all .x10 / .X10 files in the directory and add them to a zip
            all_files = glob.glob(str(directory / "*.X10")) + glob.glob(str(directory / "*.x10"))
            zip_file_name = os.path.join(tmp_path, (str(uuid4()) + ".zip"))
            with ZipFile(zip_file_name, "w") as zf:
                for file in all_files:
                    zf.write(file, arcname=os.path.basename(file))

            paths.append(Path(zip_file_name))

        return paths

    def test_vehicle_type(self, ingester) -> None:
        # Load the vehicle type data from our sample data
        path_of_this_file = Path(os.path.dirname(__file__))
        VEHICLE_TYPE_FILE_NAME = "menge_fzg_typ.x10"
        sample_data_directory = path_of_this_file / ".." / "samples" / "VDV"
        for directory in sample_data_directory.iterdir():
            if os.path.isfile(directory):
                continue

            if os.path.exists(directory / VEHICLE_TYPE_FILE_NAME) or os.path.exists(
                directory / VEHICLE_TYPE_FILE_NAME.upper()
            ):
                if os.path.exists(directory / VEHICLE_TYPE_FILE_NAME):
                    vehicle_type_file = directory / VEHICLE_TYPE_FILE_NAME
                else:
                    vehicle_type_file = directory / VEHICLE_TYPE_FILE_NAME.upper()

                vdv_table = check_vdv451_file_header(vehicle_type_file)
                vehicle_types = import_vdv452_table_records(vdv_table)

                engine = create_engine(ingester.database_url)
                eflips.model.setup_database(engine)
                with Session(engine) as session:
                    scenario = Scenario(
                        name="Test Scenario",
                    )

                    for vehicle_type in vehicle_types:
                        db_vehicle_type = vehicle_type.to_vehicle_type(scenario)
                        session.add(db_vehicle_type)
                        session.flush()

    def test_rotation(self, ingester) -> None:
        # First, do the same as test_vehicle_type
        path_of_this_file = Path(os.path.dirname(__file__))
        VEHICLE_TYPE_FILE_NAME = "menge_fzg_typ.x10"
        ROTATION_FILE_NAME = "rec_umlauf.x10"
        sample_data_directory = path_of_this_file / ".." / "samples" / "VDV"
        for directory in sample_data_directory.iterdir():
            if os.path.isfile(directory):
                continue

            if os.path.exists(directory / VEHICLE_TYPE_FILE_NAME) or os.path.exists(
                directory / VEHICLE_TYPE_FILE_NAME.upper()
            ):
                if os.path.exists(directory / VEHICLE_TYPE_FILE_NAME):
                    vehicle_type_file = directory / VEHICLE_TYPE_FILE_NAME
                else:
                    vehicle_type_file = directory / VEHICLE_TYPE_FILE_NAME.upper()

                vdv_table = check_vdv451_file_header(vehicle_type_file)
                vehicle_types = import_vdv452_table_records(vdv_table)

                engine = create_engine(ingester.database_url)
                eflips.model.setup_database(engine)
                with Session(engine) as session:
                    scenario = Scenario(
                        name="Test Scenario",
                    )

                    vehicle_types_by_pk: Dict[Tuple[int, int], VehicleType] = {}

                    for vehicle_type in vehicle_types:
                        db_vehicle_type = vehicle_type.to_vehicle_type(scenario)
                        vehicle_types_by_pk[vehicle_type.primary_key] = db_vehicle_type
                        session.add(db_vehicle_type)
                    session.flush()

                    if os.path.exists(directory / ROTATION_FILE_NAME) or os.path.exists(
                        directory / ROTATION_FILE_NAME.upper()
                    ):
                        if os.path.exists(directory / ROTATION_FILE_NAME):
                            rotation_file = directory / ROTATION_FILE_NAME
                        else:
                            rotation_file = directory / ROTATION_FILE_NAME.upper()
                        vdv_table = check_vdv451_file_header(rotation_file)
                        try:
                            rotations = import_vdv452_table_records(vdv_table)
                        except ValueError as e:
                            if "The REC_UMLAUF does not have a FZG_TYP associated with it. Cannot continue." in str(e):
                                continue

                        for rotation in rotations:
                            dummy_vehicle_type = VdvIngester.create_dummy_vehicle_type(scenario)
                            db_rotation = rotation.to_rotation(
                                scenario, vehicle_types_by_pk, dummy_vehicle_type=dummy_vehicle_type
                            )
                            session.add(db_rotation)
                        session.flush()

                    else:
                        raise ValueError(f"Could not find {ROTATION_FILE_NAME} in {directory}")

    def test_prepare(self, ingester, vdv_zip_files) -> None:
        for zip_file_name in vdv_zip_files:
            ingester.prepare(progress_callback=None, x10_zip_file=zip_file_name)

    @pytest.mark.skip("This test takes way too long")
    def test_ingest(self, ingester, vdv_zip_files) -> None:
        for zip_file_name in vdv_zip_files:
            success, uuid = ingester.prepare(progress_callback=None, x10_zip_file=zip_file_name)
            assert success
            assert isinstance(uuid, UUID)
            ingester.ingest(uuid)

    def test_valid_file(self):
        absolute_path = abspath_to_testfile("FAHRZEUG_valid.X10")
        erg = check_vdv451_file_header(absolute_path)
        assert type(erg) == VDVTable

    def test_wrong_encoding(self):
        absolute_path = abspath_to_testfile("invalid_charset.X10")
        with pytest.raises(ValueError):
            check_vdv451_file_header(absolute_path)

    def test_missing_charset(self):
        absolute_path = abspath_to_testfile("missing_charset.X10")
        with pytest.raises(ValueError):
            check_vdv451_file_header(absolute_path)

    def test_missing_table_name(self):
        absolute_path = abspath_to_testfile("missing_table_name.X10")
        with pytest.raises(ValueError):
            check_vdv451_file_header(absolute_path)

    def test_completely_different_content(self):
        absolute_path = abspath_to_testfile("komplett_anderer_inhalt.X10")
        with pytest.raises(ValueError):
            check_vdv451_file_header(absolute_path)

    def test_load_for_sample_data(self) -> None:
        """
        Take the sample data from the PROJECT_ROOT/samples/VDV subfolders and test the load function.
        :return: None
        """
        path_of_this_file = Path(os.path.dirname(__file__))
        sample_data_directory = path_of_this_file / ".." / "samples" / "VDV"

        for directory in sample_data_directory.iterdir():
            if directory.is_dir():
                validate_input_data_vdv_451(directory)
