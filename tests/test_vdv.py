import glob
import os.path
from dataclasses import dataclass
from datetime import datetime, timedelta
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
from eflips.ingest.vdv._ingester import (
    _apportion_seconds,
    fix_identical_stop_times,
    normalize_trip_offsets,
)
from tests.base import BaseIngester


@dataclass
class _MockStop:
    arrival_time: datetime
    dwell_duration: timedelta = timedelta(0)


_T0 = datetime(2023, 1, 1, 8, 0, 0)


class TestApportionSeconds:
    def test_no_compression_needed(self) -> None:
        assert _apportion_seconds([10, 20, 30], 60) == [10, 20, 30]

    def test_exact_budget(self) -> None:
        assert _apportion_seconds([10, 20, 30], 60) == [10, 20, 30]

    def test_compression_sums_to_budget(self) -> None:
        result = _apportion_seconds([10, 20, 30], 30)
        assert sum(result) == 30

    def test_compression_preserves_zero_deltas(self) -> None:
        result = _apportion_seconds([0, 10, 0], 5)
        assert sum(result) == 5

    def test_empty_list(self) -> None:
        assert _apportion_seconds([], 100) == []

    def test_zero_total_returns_unchanged(self) -> None:
        assert _apportion_seconds([0, 0], 10) == [0, 0]

    def test_largest_remainder_rounding(self) -> None:
        # 3 equal segments (total 12) compressed to 10: result must sum exactly to 10
        result = _apportion_seconds([4, 4, 4], 10)
        assert sum(result) == 10

    def test_single_element(self) -> None:
        assert _apportion_seconds([100], 50) == [50]


class TestNormalizeTripOffsets:
    def _td(self, seconds: int) -> timedelta:
        return timedelta(seconds=seconds)

    def test_single_stop_returns_unchanged(self) -> None:
        offsets = [self._td(0)]
        dwells = [self._td(0)]
        new_off, new_dw = normalize_trip_offsets(offsets, dwells, self._td(0), None)
        assert new_off == offsets
        assert new_dw == dwells

    def test_floor_zero_segment_to_one_second(self) -> None:
        offsets = [self._td(0), self._td(0), self._td(10)]
        dwells = [self._td(0), self._td(0), self._td(0)]
        new_off, _ = normalize_trip_offsets(offsets, dwells, self._td(0), None)
        assert new_off[1] == self._td(1)

    def test_terminus_dwell_zeroed(self) -> None:
        offsets = [self._td(0), self._td(60)]
        dwells = [self._td(0), self._td(30)]
        _, new_dw = normalize_trip_offsets(offsets, dwells, self._td(0), None)
        assert new_dw[-1] == timedelta(0)

    def test_clamp_to_budget(self) -> None:
        offsets = [self._td(0), self._td(30), self._td(60)]
        dwells = [self._td(0), self._td(0), self._td(0)]
        new_off, _ = normalize_trip_offsets(offsets, dwells, self._td(0), self._td(40))
        assert new_off[-1] == self._td(40)

    def test_no_clamp_when_within_budget(self) -> None:
        offsets = [self._td(0), self._td(30), self._td(60)]
        dwells = [self._td(0), self._td(0), self._td(0)]
        new_off, _ = normalize_trip_offsets(offsets, dwells, self._td(0), self._td(120))
        assert new_off[-1] == self._td(60)

    def test_dwell_capped_by_outgoing_step(self) -> None:
        offsets = [self._td(0), self._td(10)]
        dwells = [self._td(30), self._td(0)]
        _, new_dw = normalize_trip_offsets(offsets, dwells, self._td(0), None)
        assert new_dw[0] <= self._td(10)


class TestFixIdenticalStopTimes:
    def _stops(self, seconds: List[int]) -> List[_MockStop]:
        return [_MockStop(_T0 + timedelta(seconds=s)) for s in seconds]

    def test_fewer_than_three_stops_unchanged(self) -> None:
        stops = self._stops([0, 0])
        fix_identical_stop_times(stops)
        assert stops[0].arrival_time == _T0
        assert stops[1].arrival_time == _T0

    def test_no_ties_unchanged(self) -> None:
        stops = self._stops([0, 10, 20])
        fix_identical_stop_times(stops)
        assert [s.arrival_time for s in stops] == [_T0 + timedelta(seconds=s) for s in [0, 10, 20]]

    def test_mid_route_ties_spread(self) -> None:
        stops = self._stops([0, 60, 60, 120])
        fix_identical_stop_times(stops)
        t = [s.arrival_time for s in stops]
        assert t[0] < t[1] < t[2] < t[3]

    def test_first_stop_pinned_when_tied(self) -> None:
        stops = self._stops([0, 0, 0, 120])
        fix_identical_stop_times(stops)
        assert stops[0].arrival_time == _T0

    def test_last_stop_pinned_when_tied(self) -> None:
        stops = self._stops([0, 120, 120, 120])
        fix_identical_stop_times(stops)
        assert stops[-1].arrival_time == _T0 + timedelta(seconds=120)

    def test_spread_within_halfwidth(self) -> None:
        stops = self._stops([0, 60, 60, 120])
        fix_identical_stop_times(stops)
        t = [s.arrival_time for s in stops]
        assert t[1] >= _T0 + timedelta(seconds=60 - 29)
        assert t[2] <= _T0 + timedelta(seconds=60 + 29)

    def test_no_room_keeps_equal_times(self) -> None:
        # Neighbours are only 1s apart — no room to spread, so times stay equal
        stops = self._stops([0, 5, 5, 6])
        fix_identical_stop_times(stops)
        assert stops[1].arrival_time <= stops[2].arrival_time


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

    def _sample_dir(self) -> Path:
        path_of_this_file = Path(os.path.dirname(__file__))
        sample_data_directory = path_of_this_file / ".." / "samples" / "VDV"
        return next(d for d in sorted(sample_data_directory.iterdir()) if d.is_dir())

    def _zip_sample_dir(self, source_dir: Path, zip_path: Path, prefix: str = "") -> None:
        all_files = glob.glob(str(source_dir / "*.x10")) + glob.glob(str(source_dir / "*.X10"))
        with ZipFile(zip_path, "w") as zf:
            for f in all_files:
                zf.write(f, arcname=prefix + os.path.basename(f))

    def test_prepare_nested_zip(self, ingester, tmp_path) -> None:
        inner = tmp_path / "inner.zip"
        self._zip_sample_dir(self._sample_dir(), inner)
        outer = tmp_path / "outer.zip"
        with ZipFile(outer, "w") as zf:
            zf.write(inner, arcname="inner.zip")
        success, result = ingester.prepare(progress_callback=None, x10_zip_file=outer)
        assert success is True

    def test_prepare_folder_prefix(self, ingester, tmp_path) -> None:
        prefixed = tmp_path / "prefixed.zip"
        self._zip_sample_dir(self._sample_dir(), prefixed, prefix="vdv_export/")
        success, result = ingester.prepare(progress_callback=None, x10_zip_file=prefixed)
        assert success is True

    def test_prepare_zip_slip_rejected(self, ingester, tmp_path) -> None:
        malicious = tmp_path / "malicious.zip"
        with ZipFile(malicious, "w") as zf:
            zf.writestr("../evil.x10", "x" * 10)
        with pytest.raises(ValueError, match="[Zz]ip [Ss]lip"):
            ingester.prepare(progress_callback=None, x10_zip_file=malicious)

    def test_prepare_validation_failure_returns_false(self, ingester, tmp_path) -> None:
        # A zip that passes zip-level validation but fails VDV 451 content check
        # (FAHRZEUG_valid.X10 has a valid header but is not a required table).
        fahrzeug = Path(os.path.dirname(__file__)) / "test_vdv2_files" / "FAHRZEUG_valid.X10"
        bad = tmp_path / "bad.zip"
        with ZipFile(bad, "w") as zf:
            zf.write(fahrzeug, arcname="FAHRZEUG.X10")
        success, result = ingester.prepare(progress_callback=None, x10_zip_file=bad)
        assert success is False
        assert "validation" in result

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
