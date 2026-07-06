"""Unit tests for the VDV importer that do not require a database.

Covers _DebugSink, validate_zip_file, parse_datatypes, check_vdv451_file_header,
import_vdv452_table_records (all table branches + error paths), and every from_dict
method in _xmldata.py.
"""
import io
import json
import os
from datetime import date, datetime, timedelta
from pathlib import Path
from zipfile import ZipFile, ZipInfo

import pytest

from eflips.ingest.vdv import (
    VDV_Data_Type,
    VDV_Table_Name,
    VDVTable,
    check_vdv451_file_header,
    import_vdv452_table_records,
    parse_datatypes,
    validate_input_data_vdv_451,
    validate_zip_file,
)
from eflips.ingest.vdv._ingester import _DebugSink
from eflips.ingest.vdv._xmldata import (
    BasisVerGueltigkeit,
    Firmenkalender,
    LidVerlauf,
    MengeFzgTyp,
    OrtHztf,
    RecFrt,
    RecFrtHzt,
    RecLid,
    RecOrt,
    RecSel,
    RecUmlauf,
    RoutenArt,
    SelFztFeld,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_x10(
    path: Path,
    table_name_str: str,
    col_names: list,
    col_formats: list,
    records: list,
) -> None:
    """Write a minimal valid VDV 451 x10 file to *path*."""
    lines = [
        "mod; DD.MM.YYYY; HH:MM:SS; free",
        'chs; "ISO8859-1"',
        f"tbl; {table_name_str}",
        "atr; " + "; ".join(col_names),
        "frm; " + "; ".join(col_formats),
    ]
    for rec in records:
        lines.append("rec; " + "; ".join(str(v) for v in rec))
    lines.append(f"end; {len(records)}")
    lines.append("eof; 1")
    path.write_text("\n".join(lines), encoding="iso-8859-1")


def _x10_table(
    path: Path,
    table_name_str: str,
    col_names: list,
    col_formats: list,
    records: list,
) -> VDVTable:
    _make_x10(path, table_name_str, col_names, col_formats, records)
    return check_vdv451_file_header(str(path))


# ---------------------------------------------------------------------------
# _DebugSink
# ---------------------------------------------------------------------------

class TestDebugSink:
    def test_disabled_when_no_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.delenv("EFLIPS_VDV_DEBUG_LOG", raising=False)
        sink = _DebugSink()
        assert not sink.enabled
        sink.log("should_be_noop")
        sink.close()

    def test_enabled_when_env_set(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        log_path = tmp_path / "debug.jsonl"
        monkeypatch.setenv("EFLIPS_VDV_DEBUG_LOG", str(log_path))
        sink = _DebugSink()
        assert sink.enabled
        sink.close()
        assert log_path.exists()

    def test_log_writes_jsonl(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        log_path = tmp_path / "debug.jsonl"
        monkeypatch.setenv("EFLIPS_VDV_DEBUG_LOG", str(log_path))
        sink = _DebugSink()
        sink.log("test_event", value=42, name="hello")
        sink.close()
        lines = log_path.read_text().splitlines()
        # first line is debug_session_start, second is our event
        assert len(lines) == 2
        evt = json.loads(lines[1])
        assert evt["event"] == "test_event"
        assert evt["value"] == 42
        assert evt["name"] == "hello"

    def test_log_coerces_datetime(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        log_path = tmp_path / "debug.jsonl"
        monkeypatch.setenv("EFLIPS_VDV_DEBUG_LOG", str(log_path))
        sink = _DebugSink()
        dt = datetime(2023, 4, 18, 12, 0, 0)
        sink.log("evt", ts=dt)
        sink.close()
        lines = log_path.read_text().splitlines()
        evt = json.loads(lines[1])
        assert evt["ts"] == "2023-04-18T12:00:00"

    def test_log_coerces_timedelta(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        log_path = tmp_path / "debug.jsonl"
        monkeypatch.setenv("EFLIPS_VDV_DEBUG_LOG", str(log_path))
        sink = _DebugSink()
        sink.log("evt", dur=timedelta(seconds=90))
        sink.close()
        lines = log_path.read_text().splitlines()
        evt = json.loads(lines[1])
        assert evt["dur"] == 90.0

    def test_log_coerces_tuple(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        log_path = tmp_path / "debug.jsonl"
        monkeypatch.setenv("EFLIPS_VDV_DEBUG_LOG", str(log_path))
        sink = _DebugSink()
        sink.log("evt", key=(1, 2, 3))
        sink.close()
        lines = log_path.read_text().splitlines()
        evt = json.loads(lines[1])
        assert evt["key"] == [1, 2, 3]

    def test_close_is_idempotent(self, monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
        log_path = tmp_path / "debug.jsonl"
        monkeypatch.setenv("EFLIPS_VDV_DEBUG_LOG", str(log_path))
        sink = _DebugSink()
        sink.close()
        sink.close()  # second close must not raise
        assert not sink.enabled


# ---------------------------------------------------------------------------
# validate_zip_file
# ---------------------------------------------------------------------------

class TestValidateZipFileExtra:
    def _make_zip(self, buf: io.BytesIO, entries: list) -> io.BytesIO:
        """Write entries as (name, data) pairs into a new ZipFile in *buf*."""
        with ZipFile(buf, "w") as zf:
            for name, data in entries:
                zf.writestr(name, data)
        buf.seek(0)
        return buf

    def test_dir_entry_skipped(self) -> None:
        buf = io.BytesIO()
        with ZipFile(buf, "w") as zf:
            # A directory-only entry — ZipFile won't create a ZipInfo for a
            # real dir via writestr, so we add a member with a trailing slash.
            zi = ZipInfo("somedir/")
            zf.writestr(zi, "")
            # Also add a valid x10 so the whole archive would be accepted
            zf.writestr("valid.x10", "x" * 10)
        buf.seek(0)
        result = validate_zip_file(buf)
        # The directory entry is skipped; the x10 content is arbitrary (not
        # really parsed here), so only the zero-length check matters.
        # Our x10 has 10 bytes so it should not trigger "Empty file".
        assert result is True

    def test_empty_x10_rejected(self) -> None:
        buf = self._make_zip(io.BytesIO(), [("empty.x10", "")])
        result = validate_zip_file(buf)
        assert isinstance(result, dict)
        assert "empty.x10" in result

    def test_txt_file_skipped(self) -> None:
        buf = self._make_zip(io.BytesIO(), [("readme.txt", "some text"), ("data.x10", "x" * 10)])
        result = validate_zip_file(buf)
        assert result is True

    def test_wrong_extension_rejected(self) -> None:
        buf = self._make_zip(io.BytesIO(), [("file.pdf", "data")])
        result = validate_zip_file(buf)
        assert isinstance(result, dict)
        assert "file.pdf" in result

    def test_corrupt_zip_error(self) -> None:
        garbage = io.BytesIO(b"this is not a zip file at all")
        result = validate_zip_file(garbage)
        assert isinstance(result, dict)
        assert "zipfile" in result

    def test_nested_zip_recursion(self) -> None:
        # Inner zip with a valid x10
        inner = io.BytesIO()
        self._make_zip(inner, [("inner.x10", "x" * 10)])

        # Outer zip containing the inner zip
        outer = io.BytesIO()
        with ZipFile(outer, "w") as zf:
            zf.writestr("inner.zip", inner.getvalue())
        outer.seek(0)

        result = validate_zip_file(outer)
        assert result is True

    def test_nested_zip_error_propagated(self) -> None:
        # Inner zip with a bad extension
        inner = io.BytesIO()
        self._make_zip(inner, [("bad.csv", "data")])

        outer = io.BytesIO()
        with ZipFile(outer, "w") as zf:
            zf.writestr("inner.zip", inner.getvalue())
        outer.seek(0)

        result = validate_zip_file(outer)
        assert isinstance(result, dict)
        assert "bad.csv" in result


# ---------------------------------------------------------------------------
# parse_datatypes
# ---------------------------------------------------------------------------

class TestParseDatatypesExtra:
    def test_float_format(self) -> None:
        result = parse_datatypes(["num[9.4]"])
        assert result == [VDV_Data_Type.FLOAT]

    def test_unknown_format_returns_none(self) -> None:
        result = parse_datatypes(["garbage_type"])
        assert result == [None]

    def test_empty_input(self) -> None:
        assert parse_datatypes([]) == []


# ---------------------------------------------------------------------------
# check_vdv451_file_header — additional error paths
# ---------------------------------------------------------------------------

class TestCheckVdv451HeaderExtra:
    def test_disallowed_charset(self, tmp_path: Path) -> None:
        # UTF-16 is a valid codec but not in the VDV-allowed set.
        f = tmp_path / "disallowed.x10"
        f.write_text(
            'mod; DD.MM.YYYY; HH:MM:SS; free\nchs; "UTF-16"\ntbl; REC_SEL\neof; 0\n',
            encoding="ascii",
        )
        with pytest.raises(ValueError, match="not allowed"):
            check_vdv451_file_header(str(f))

    def test_no_datatypes(self, tmp_path: Path) -> None:
        f = tmp_path / "no_frm.x10"
        f.write_text(
            'mod; DD.MM.YYYY; HH:MM:SS; free\n'
            'chs; "ISO8859-1"\n'
            "tbl; REC_SEL\n"
            "atr; BASIS_VERSION; BEREICH_NR\n"
            "eof; 0\n",
            encoding="iso-8859-1",
        )
        with pytest.raises(ValueError, match="data types"):
            check_vdv451_file_header(str(f))

    def test_no_column_names(self, tmp_path: Path) -> None:
        f = tmp_path / "no_atr.x10"
        f.write_text(
            'mod; DD.MM.YYYY; HH:MM:SS; free\n'
            'chs; "ISO8859-1"\n'
            "tbl; REC_SEL\n"
            "frm; num[9.0]; num[3.0]\n"
            "eof; 0\n",
            encoding="iso-8859-1",
        )
        with pytest.raises(ValueError, match="column names"):
            check_vdv451_file_header(str(f))

    def test_unknown_table_name(self) -> None:
        # This fixture has tbl; MÜLLABFUHRSPASS which is not in VDV_Table_Name.
        fixture = Path(__file__).parent / "test_vdv2_files" / "non_ascii_zeichen_header_ISO8859-1.X10"
        with pytest.raises(ValueError, match="unknown table name"):
            check_vdv451_file_header(str(fixture))

    def test_mismatched_column_count(self, tmp_path: Path) -> None:
        f = tmp_path / "mismatch.x10"
        f.write_text(
            'mod; DD.MM.YYYY; HH:MM:SS; free\n'
            'chs; "ISO8859-1"\n'
            "tbl; REC_SEL\n"
            "atr; A; B; C\n"       # 3 names
            "frm; num[9.0]; char[4]\n"  # 2 formats
            "eof; 0\n",
            encoding="iso-8859-1",
        )
        with pytest.raises(ValueError, match="unequal number"):
            check_vdv451_file_header(str(f))


# ---------------------------------------------------------------------------
# validate_input_data_vdv_451 — error paths
# ---------------------------------------------------------------------------

# Minimal column/format specs per required table (no records needed).
_REQUIRED_TABLE_SPECS: dict = {
    "BASIS_VER_GUELTIGKEIT": (
        ["BASIS_VERSION", "VER_GUELTIGKEIT"],
        ["num[9.0]", "num[8.0]"],
    ),
    "FIRMENKALENDER": (
        ["BASIS_VERSION", "BETRIEBSTAG", "TAGESART_NR"],
        ["num[9.0]", "num[8.0]", "num[2.0]"],
    ),
    "REC_ORT": (
        ["BASIS_VERSION", "ONR_TYP_NR", "ORT_NR", "ORT_NAME",
         "ORT_REF_ORT", "ORT_REF_ORT_TYP", "ORT_REF_ORT_KUERZEL", "ORT_REF_ORT_NAME"],
        ["num[9.0]", "num[2.0]", "num[6.0]", "char[40]",
         "num[6.0]", "num[2.0]", "char[20]", "char[40]"],
    ),
    "MENGE_FZG_TYP": (
        ["BASIS_VERSION", "FZG_TYP_NR", "FZG_TYP_TEXT", "STR_FZG_TYP"],
        ["num[9.0]", "num[3.0]", "char[40]", "char[6]"],
    ),
    "REC_SEL": (
        ["BASIS_VERSION", "BEREICH_NR", "ONR_TYP_NR", "ORT_NR",
         "SEL_ZIEL_TYP", "SEL_ZIEL", "SEL_LAENGE"],
        ["num[9.0]", "num[3.0]", "num[2.0]", "num[10.0]",
         "num[2.0]", "num[10.0]", "num[6.0]"],
    ),
    "SEL_FZT_FELD": (
        ["BASIS_VERSION", "BEREICH_NR", "ONR_TYP_NR", "ORT_NR",
         "SEL_ZIEL_TYP", "SEL_ZIEL", "FGR_NR", "SEL_FZT"],
        ["num[9.0]", "num[3.0]", "num[2.0]", "num[10.0]",
         "num[2.0]", "num[10.0]", "num[5.0]", "num[5.0]"],
    ),
    "LID_VERLAUF": (
        ["BASIS_VERSION", "LI_NR", "STR_LI_VAR", "LI_LFD_NR", "ONR_TYP_NR", "ORT_NR"],
        ["num[9.0]", "num[6.0]", "char[6]", "num[3.0]", "num[2.0]", "num[10.0]"],
    ),
    "REC_FRT": (
        ["BASIS_VERSION", "FRT_FID", "LI_NR", "STR_LI_VAR",
         "TAGESART_NR", "FAHRTART_NR", "FGR_NR", "FRT_START", "UM_UID"],
        ["num[9.0]", "num[8.0]", "num[6.0]", "char[6]",
         "num[5.0]", "num[2.0]", "num[5.0]", "num[6.0]", "num[8.0]"],
    ),
    "REC_UMLAUF": (
        ["BASIS_VERSION", "TAGESART_NR", "UM_UID",
         "ANF_ORT", "ANF_ONR_TYP", "END_ORT", "END_ONR_TYP"],
        ["num[9.0]", "num[2.0]", "num[8.0]",
         "num[6.0]", "num[2.0]", "num[6.0]", "num[2.0]"],
    ),
    "REC_LID": (
        ["BASIS_VERSION", "LI_NR", "STR_LI_VAR", "BEREICH_NR",
         "LI_KUERZEL", "LIDNAME", "ROUTEN_ART"],
        ["num[9.0]", "num[6.0]", "char[6]", "num[3.0]",
         "char[7]", "char[30]", "num[2.0]"],
    ),
}


def _populate_required_tables(d: Path, exclude: set | None = None) -> None:
    """Write all required tables (except those in *exclude*) into directory *d*."""
    if exclude is None:
        exclude = set()
    for tbl_name, (cols, fmts) in _REQUIRED_TABLE_SPECS.items():
        if tbl_name in exclude:
            continue
        _make_x10(d / f"{tbl_name.lower()}.x10", tbl_name, cols, fmts, [])


class TestValidateInputDataVdv451:
    def test_empty_dir_raises_missing_tables(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Missing tables"):
            validate_input_data_vdv_451(tmp_path)

    def test_duplicate_table_silently_ignored_then_missing_raises(self, tmp_path: Path) -> None:
        # Two files both claiming BASIS_VER_GUELTIGKEIT → second is silently
        # skipped, but other required tables are still absent → ValueError.
        cols, fmts = _REQUIRED_TABLE_SPECS["BASIS_VER_GUELTIGKEIT"]
        _make_x10(tmp_path / "a.x10", "BASIS_VER_GUELTIGKEIT", cols, fmts, [])
        _make_x10(tmp_path / "b.x10", "BASIS_VER_GUELTIGKEIT", cols, fmts, [])
        with pytest.raises(ValueError, match="Missing tables"):
            validate_input_data_vdv_451(tmp_path)

    def test_neither_hzt_nor_hztf_raises(self, tmp_path: Path) -> None:
        _populate_required_tables(tmp_path)
        # No REC_FRT_HZT and no ORT_HZTF → should raise
        with pytest.raises(ValueError, match="Neither REC_FRT_HZT nor ORT_HZTF"):
            validate_input_data_vdv_451(tmp_path)

    def test_with_ort_hztf_passes(self, tmp_path: Path) -> None:
        _populate_required_tables(tmp_path)
        _make_x10(
            tmp_path / "ort_hztf.x10",
            "ORT_HZTF",
            ["BASIS_VERSION", "ONR_TYP_NR", "ORT_NR", "FGR_NR", "HP_HZT"],
            ["num[9.0]", "num[2.0]", "num[10.0]", "num[5.0]", "num[6.0]"],
            [],
        )
        # Should not raise
        validate_input_data_vdv_451(tmp_path)


# ---------------------------------------------------------------------------
# import_vdv452_table_records — error paths + all table type branches
# ---------------------------------------------------------------------------

class TestImportVdv452Extra:
    def test_wrong_column_count_raises(self, tmp_path: Path) -> None:
        # frm declares 2 columns but the rec line has 3 values.
        f = tmp_path / "bad_count.x10"
        f.write_text(
            'chs; "ISO8859-1"\ntbl; REC_SEL\n'
            "atr; BASIS_VERSION; BEREICH_NR\n"
            "frm; num[9.0]; num[3.0]\n"
            "rec; 369; 10; EXTRA_FIELD\n"
            "eof; 1\n",
            encoding="iso-8859-1",
        )
        tbl = check_vdv451_file_header(str(f))
        with pytest.raises(ValueError):
            import_vdv452_table_records(tbl)

    def test_skip_none_column_type(self, tmp_path: Path) -> None:
        # frm has an unrecognised format for the second column → it gets
        # VDV_Data_Type=None and should be skipped during import.
        f = tmp_path / "unknown_fmt.x10"
        f.write_text(
            'chs; "ISO8859-1"\ntbl; REC_SEL\n'
            "atr; BASIS_VERSION; BEREICH_NR\n"
            "frm; num[9.0]; NOPE_FMT\n"
            "rec; 369; 10\n"
            "eof; 1\n",
            encoding="iso-8859-1",
        )
        tbl = check_vdv451_file_header(str(f))
        # import_vdv452_table_records for an unrecognised table name (REC_SEL is
        # known, but the second column is skipped; only BASIS_VERSION remains in dict).
        # REC_SEL.from_dict will then fail because other required keys are missing.
        # We just verify the skip path is reached without crashing on column parsing.
        # (A KeyError from from_dict is fine here.)
        try:
            import_vdv452_table_records(tbl)
        except (KeyError, AssertionError):
            pass  # Expected: from_dict fails because required keys are missing

    def test_non_numeric_int_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_int.x10"
        f.write_text(
            'chs; "ISO8859-1"\ntbl; REC_SEL\n'
            "atr; BASIS_VERSION; BEREICH_NR\n"
            "frm; num[9.0]; num[3.0]\n"
            "rec; 369; NOTANINT\n"
            "eof; 1\n",
            encoding="iso-8859-1",
        )
        tbl = check_vdv451_file_header(str(f))
        with pytest.raises(ValueError):
            import_vdv452_table_records(tbl)

    def test_non_numeric_float_raises(self, tmp_path: Path) -> None:
        f = tmp_path / "bad_float.x10"
        f.write_text(
            'chs; "ISO8859-1"\ntbl; REC_SEL\n'
            "atr; BASIS_VERSION; BEREICH_NR\n"
            "frm; num[9.0]; num[3.2]\n"
            "rec; 369; NOTAFLOAT\n"
            "eof; 1\n",
            encoding="iso-8859-1",
        )
        tbl = check_vdv451_file_header(str(f))
        with pytest.raises(ValueError):
            import_vdv452_table_records(tbl)

    def test_basis_ver_gueltigkeit_single(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "bvg.x10",
            "BASIS_VER_GUELTIGKEIT",
            ["BASIS_VERSION", "VER_GUELTIGKEIT"],
            ["num[9.0]", "num[8.0]"],
            [[369, 20230418]],
        )
        result = import_vdv452_table_records(tbl)
        assert len(result) == 1
        assert isinstance(result[0], BasisVerGueltigkeit)
        assert result[0].ver_gueltigkeit == date(2023, 4, 18)

    def test_basis_ver_gueltigkeit_multiple_distinct_raises(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "bvg2.x10",
            "BASIS_VER_GUELTIGKEIT",
            ["BASIS_VERSION", "VER_GUELTIGKEIT"],
            ["num[9.0]", "num[8.0]"],
            [[369, 20230418], [369, 20230419]],
        )
        with pytest.raises(ValueError, match="multiple distinct"):
            import_vdv452_table_records(tbl)

    def test_firmenkalender(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "fk.x10",
            "FIRMENKALENDER",
            ["BASIS_VERSION", "BETRIEBSTAG", "TAGESART_NR"],
            ["num[9.0]", "num[8.0]", "num[2.0]"],
            [[369, 20230418, 1]],
        )
        result = import_vdv452_table_records(tbl)
        assert len(result) == 1
        assert isinstance(result[0], Firmenkalender)
        assert result[0].betriebstag == date(2023, 4, 18)
        assert result[0].tagesart_nr == 1

    def test_rec_ort(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "ro.x10",
            "REC_ORT",
            ["BASIS_VERSION", "ONR_TYP_NR", "ORT_NR", "ORT_NAME",
             "ORT_REF_ORT", "ORT_REF_ORT_TYP", "ORT_REF_ORT_KUERZEL",
             "ORT_REF_ORT_NAME", "WGS_XKOOR", "WGS_YKOOR", "ORT_POS_HOEHE"],
            ["num[9.0]", "num[2.0]", "num[6.0]", "char[40]",
             "num[6.0]", "num[2.0]", "char[20]", "char[40]",
             "num[9.6]", "num[9.6]", "num[5.0]"],
            [[369, 1, 500, "Hauptbahnhof", 50, 1, "HBF", "Hauptbahnhof", 13.351, 52.519, 100]],
        )
        result = import_vdv452_table_records(tbl)
        assert len(result) == 1
        obj = result[0]
        assert isinstance(obj, RecOrt)
        assert obj.ort_name == "Hauptbahnhof"
        assert abs(obj.longitude - 13.351) < 0.001  # type: ignore[operator]
        assert abs(obj.latitude - 52.519) < 0.001   # type: ignore[operator]
        assert obj.altitude == 100

    def test_rec_sel(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "rs.x10",
            "REC_SEL",
            ["BASIS_VERSION", "BEREICH_NR", "ONR_TYP_NR", "ORT_NR",
             "SEL_ZIEL_TYP", "SEL_ZIEL", "SEL_LAENGE"],
            ["num[9.0]", "num[3.0]", "num[2.0]", "num[10.0]",
             "num[2.0]", "num[10.0]", "num[6.0]"],
            [[369, 10, 1, 500, 1, 501, 400]],
        )
        result = import_vdv452_table_records(tbl)
        assert len(result) == 1
        obj = result[0]
        assert isinstance(obj, RecSel)
        assert obj.sel_laenge == 400
        assert obj.start_station_primary_key == (369, 1, 500)
        assert obj.end_station_primary_key == (369, 1, 501)

    def test_sel_fzt_feld(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "sfz.x10",
            "SEL_FZT_FELD",
            ["BASIS_VERSION", "BEREICH_NR", "ONR_TYP_NR", "ORT_NR",
             "SEL_ZIEL_TYP", "SEL_ZIEL", "FGR_NR", "SEL_FZT"],
            ["num[9.0]", "num[3.0]", "num[2.0]", "num[10.0]",
             "num[2.0]", "num[10.0]", "num[5.0]", "num[5.0]"],
            [[369, 10, 1, 500, 1, 501, 1, 120]],
        )
        result = import_vdv452_table_records(tbl)
        assert len(result) == 1
        obj = result[0]
        assert isinstance(obj, SelFztFeld)
        assert obj.sel_fzt == timedelta(seconds=120)

    def test_lid_verlauf(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "lv.x10",
            "LID_VERLAUF",
            ["BASIS_VERSION", "LI_NR", "STR_LI_VAR", "LI_LFD_NR", "ONR_TYP_NR", "ORT_NR"],
            ["num[9.0]", "num[6.0]", "char[6]", "num[3.0]", "num[2.0]", "num[10.0]"],
            [[369, 100, "H", 1, 1, 500]],
        )
        result = import_vdv452_table_records(tbl)
        assert len(result) == 1
        obj = result[0]
        assert isinstance(obj, LidVerlauf)
        assert obj.li_lfd_nr == 1
        assert obj.position_key == (369, 1, 500)

    def test_rec_frt(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "rf.x10",
            "REC_FRT",
            ["BASIS_VERSION", "FRT_FID", "LI_NR", "STR_LI_VAR",
             "TAGESART_NR", "FAHRTART_NR", "FGR_NR", "FRT_START", "UM_UID"],
            ["num[9.0]", "num[8.0]", "num[6.0]", "char[6]",
             "num[5.0]", "num[2.0]", "num[5.0]", "num[6.0]", "num[8.0]"],
            [[369, 1001, 100, "H", 1, 1, 1, 28800, 200]],
        )
        result = import_vdv452_table_records(tbl)
        assert len(result) == 1
        obj = result[0]
        assert isinstance(obj, RecFrt)
        assert obj.frt_start == timedelta(seconds=28800)
        assert obj.primary_key == (369, 1001)

    def test_rec_lid(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "rl.x10",
            "REC_LID",
            ["BASIS_VERSION", "LI_NR", "STR_LI_VAR", "BEREICH_NR",
             "LI_KUERZEL", "LIDNAME", "ROUTEN_ART"],
            ["num[9.0]", "num[6.0]", "char[6]", "num[3.0]",
             "char[7]", "char[30]", "num[2.0]"],
            [[369, 100, "H", 10, "42", "Linie 42", 1]],
        )
        result = import_vdv452_table_records(tbl)
        assert len(result) == 1
        obj = result[0]
        assert isinstance(obj, RecLid)
        assert obj.li_kuerzel == "42"
        assert obj.routen_art == RoutenArt.NORMAL
        assert obj.primary_key == (369, 100, "H")

    def test_rec_frt_hzt(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "rfh.x10",
            "REC_FRT_HZT",
            ["BASIS_VERSION", "FRT_FID", "ONR_TYP_NR", "ORT_NR", "FRT_HZT_ZEIT"],
            ["num[9.0]", "num[8.0]", "num[2.0]", "num[10.0]", "num[6.0]"],
            [[369, 1001, 1, 500, 60]],
        )
        result = import_vdv452_table_records(tbl)
        assert len(result) == 1
        obj = result[0]
        assert isinstance(obj, RecFrtHzt)
        assert obj.frt_hzt_zeit == timedelta(seconds=60)
        assert obj.position_key == (369, 1, 500)

    def test_ort_hztf(self, tmp_path: Path) -> None:
        tbl = _x10_table(
            tmp_path / "oh.x10",
            "ORT_HZTF",
            ["BASIS_VERSION", "ONR_TYP_NR", "ORT_NR", "FGR_NR", "HP_HZT"],
            ["num[9.0]", "num[2.0]", "num[10.0]", "num[5.0]", "num[6.0]"],
            [[369, 1, 500, 1, 30]],
        )
        result = import_vdv452_table_records(tbl)
        assert len(result) == 1
        obj = result[0]
        assert isinstance(obj, OrtHztf)
        assert obj.hp_hzt == timedelta(seconds=30)

    def test_unknown_table_returns_empty(self, tmp_path: Path) -> None:
        # FAHRZEUG is a known VDV_Table_Name but has no handler → returns [].
        tbl = _x10_table(
            tmp_path / "fzg.x10",
            "FAHRZEUG",
            ["BASIS_VERSION", "FZG_NR"],
            ["num[9.0]", "num[4.0]"],
            [],
        )
        result = import_vdv452_table_records(tbl)
        assert result == []


# ---------------------------------------------------------------------------
# from_dict methods — all classes in _xmldata.py
# ---------------------------------------------------------------------------

class TestFromDictMethods:
    # -- BasisVerGueltigkeit --------------------------------------------------

    def test_basis_ver_gueltigkeit(self) -> None:
        d = {"VER_GUELTIGKEIT": 20230418, "BASIS_VERSION": 369}
        obj = BasisVerGueltigkeit.from_dict(d)
        assert obj.ver_gueltigkeit == date(2023, 4, 18)
        assert obj.basis_version == 369
        assert obj.primary_key == (date(2023, 4, 18),)

    # -- Firmenkalender -------------------------------------------------------

    def test_firmenkalender(self) -> None:
        d = {"BASIS_VERSION": 369, "BETRIEBSTAG": 20230418, "TAGESART_NR": 2}
        obj = Firmenkalender.from_dict(d)
        assert obj.betriebstag == date(2023, 4, 18)
        assert obj.tagesart_nr == 2
        assert obj.primary_key == (369, date(2023, 4, 18))

    # -- LidVerlauf -----------------------------------------------------------

    def test_lid_verlauf(self) -> None:
        d = {
            "BASIS_VERSION": 369, "LI_LFD_NR": 3, "LI_NR": 100,
            "STR_LI_VAR": "H", "ONR_TYP_NR": 1, "ORT_NR": 500,
        }
        obj = LidVerlauf.from_dict(d)
        assert obj.li_lfd_nr == 3
        assert obj.position_key == (369, 1, 500)
        assert obj.primary_key == (369, 100, "H", 3)

    # -- OrtHztf --------------------------------------------------------------

    def test_ort_hztf(self) -> None:
        d = {
            "BASIS_VERSION": 369, "ONR_TYP_NR": 1, "ORT_NR": 500,
            "FGR_NR": 2, "HP_HZT": 45,
        }
        obj = OrtHztf.from_dict(d)
        assert obj.hp_hzt == timedelta(seconds=45)
        assert obj.position_key == (369, 1, 500)
        assert obj.primary_key == (369, 2, 1, 500)

    # -- RecFrtHzt ------------------------------------------------------------

    def test_rec_frt_hzt(self) -> None:
        d = {
            "BASIS_VERSION": 369, "ONR_TYP_NR": 1, "ORT_NR": 500,
            "FRT_FID": 1001, "FRT_HZT_ZEIT": 120,
        }
        obj = RecFrtHzt.from_dict(d)
        assert obj.frt_hzt_zeit == timedelta(seconds=120)
        assert obj.position_key == (369, 1, 500)
        assert obj.primary_key == (369, 1001, 1, 500)

    # -- SelFztFeld -----------------------------------------------------------

    def test_sel_fzt_feld(self) -> None:
        d = {
            "BASIS_VERSION": 369, "BEREICH_NR": 10, "FGR_NR": 1,
            "ONR_TYP_NR": 1, "ORT_NR": 500,
            "SEL_ZIEL_TYP": 1, "SEL_ZIEL": 501, "SEL_FZT": 90,
        }
        obj = SelFztFeld.from_dict(d)
        assert obj.sel_fzt == timedelta(seconds=90)
        assert obj.start_station_primary_key == (369, 1, 500)
        assert obj.end_station_primary_key == (369, 1, 501)
        assert obj.primary_key == (369, 10, 1, 1, 500, 1, 501)

    # -- RecFrt ---------------------------------------------------------------

    def test_rec_frt(self) -> None:
        d = {
            "BASIS_VERSION": 369, "FRT_FID": 2001, "FRT_START": 28800,
            "LI_NR": 100, "TAGESART_NR": 1, "FAHRTART_NR": 1,
            "FGR_NR": 1, "STR_LI_VAR": "H", "UM_UID": 200,
        }
        obj = RecFrt.from_dict(d)
        assert obj.frt_start == timedelta(hours=8)
        assert obj.primary_key == (369, 2001)

    # -- RecOrt — coordinate variants -----------------------------------------

    def _base_ort(self) -> dict:
        return {
            "BASIS_VERSION": 369, "ONR_TYP_NR": 1, "ORT_NR": 500,
            "ORT_NAME": "Hauptbahnhof",
            "ORT_REF_ORT": 50, "ORT_REF_ORT_TYP": 1,
            "ORT_REF_ORT_KUERZEL": "HBF", "ORT_REF_ORT_NAME": "Hauptbahnhof Station",
            "ORT_POS_HOEHE": 100,
        }

    def test_rec_ort_wgs_coords(self) -> None:
        d = {**self._base_ort(), "WGS_XKOOR": 13.351, "WGS_YKOOR": 52.519}
        obj = RecOrt.from_dict(d)
        assert abs(obj.longitude - 13.351) < 1e-6  # type: ignore[operator]
        assert abs(obj.latitude - 52.519) < 1e-6   # type: ignore[operator]
        assert obj.altitude == 100

    def test_rec_ort_ort_pos_xy(self) -> None:
        d = {**self._base_ort(), "ORT_POS_X": 13351000, "ORT_POS_Y": 52519000}
        obj = RecOrt.from_dict(d)
        assert abs(obj.longitude - 13.351) < 0.001  # type: ignore[operator]
        assert abs(obj.latitude - 52.519) < 0.001   # type: ignore[operator]

    def test_rec_ort_ort_pos_dms(self) -> None:
        # 13°21'03.600" E  →  13*1e7 + 21*1e5 + 3600 = 132103600
        # 52°31'08.416" N  →  52*1e7 + 31*1e5 + 8416 = 523108416
        d = {**self._base_ort(), "ORT_POS_LAENGE": 132103600, "ORT_POS_BREITE": 523108416}
        obj = RecOrt.from_dict(d)
        assert abs(obj.longitude - 13.351) < 0.001  # type: ignore[operator]
        assert abs(obj.latitude - 52.519) < 0.001   # type: ignore[operator]

    def test_rec_ort_no_coords(self) -> None:
        d = {
            "BASIS_VERSION": 369, "ONR_TYP_NR": 2, "ORT_NR": 600,
            "ORT_NAME": "Depot",
            "ORT_REF_ORT": None, "ORT_REF_ORT_TYP": None,
            "ORT_REF_ORT_KUERZEL": None, "ORT_REF_ORT_NAME": None,
        }
        obj = RecOrt.from_dict(d)
        assert obj.latitude is None
        assert obj.longitude is None
        assert obj.altitude is None

    def test_rec_ort_standalone_primary_key(self) -> None:
        d = {
            "BASIS_VERSION": 369, "ONR_TYP_NR": 6, "ORT_NR": 700,
            "ORT_NAME": "Betriebspunkt",
            "ORT_REF_ORT": None, "ORT_REF_ORT_TYP": None,
            "ORT_REF_ORT_KUERZEL": None, "ORT_REF_ORT_NAME": None,
        }
        obj = RecOrt.from_dict(d)
        assert obj.primary_key == (369, 6, 700)

    # -- RecSel ---------------------------------------------------------------

    def test_rec_sel(self) -> None:
        d = {
            "BASIS_VERSION": 369, "BEREICH_NR": 10,
            "ONR_TYP_NR": 1, "ORT_NR": 500,
            "SEL_ZIEL_TYP": 1, "SEL_ZIEL": 501, "SEL_LAENGE": 350,
        }
        obj = RecSel.from_dict(d)
        assert obj.sel_laenge == 350
        assert obj.start_station_primary_key == (369, 1, 500)
        assert obj.end_station_primary_key == (369, 1, 501)

    # -- RecUmlauf ------------------------------------------------------------

    def _base_umlauf(self) -> dict:
        return {
            "BASIS_VERSION": 369, "TAGESART_NR": 1, "UM_UID": 200,
            "ANF_ORT": 300, "ANF_ONR_TYP": 1,
            "END_ORT": 301, "END_ONR_TYP": 1,
        }

    def test_rec_umlauf_with_fzg_typ(self) -> None:
        d = {**self._base_umlauf(), "FZG_TYP_NR": 5}
        obj = RecUmlauf.from_dict(d)
        assert obj.fzg_typ_nr == 5
        assert obj.start_station_primary_key == (369, 1, 300)
        assert obj.end_station_primary_key == (369, 1, 301)
        assert obj.primary_key == (369, 1, 200)

    def test_rec_umlauf_fzg_typ_zero_gives_none(self) -> None:
        d = {**self._base_umlauf(), "FZG_TYP_NR": 0}
        obj = RecUmlauf.from_dict(d)
        assert obj.fzg_typ_nr is None

    def test_rec_umlauf_fzg_typ_missing_gives_none(self) -> None:
        d = self._base_umlauf()
        obj = RecUmlauf.from_dict(d)
        assert obj.fzg_typ_nr is None

    # -- RecLid ---------------------------------------------------------------

    def test_rec_lid_with_name(self) -> None:
        d = {
            "BASIS_VERSION": 369, "LI_NR": 100, "STR_LI_VAR": "H",
            "BEREICH_NR": 10, "LI_KUERZEL": "42",
            "LIDNAME": "Linie 42 Hauptrichtung", "ROUTEN_ART": 1,
        }
        obj = RecLid.from_dict(d)
        assert obj.lidname == "Linie 42 Hauptrichtung"
        assert obj.routen_art == RoutenArt.NORMAL
        assert obj.primary_key == (369, 100, "H")

    def test_rec_lid_null_name_becomes_na(self) -> None:
        d = {
            "BASIS_VERSION": 369, "LI_NR": 100, "STR_LI_VAR": "R",
            "BEREICH_NR": 10, "LI_KUERZEL": "42",
            "LIDNAME": None, "ROUTEN_ART": 2,
        }
        obj = RecLid.from_dict(d)
        assert obj.lidname == "N/A"
        assert obj.routen_art == RoutenArt.TO_DEPOT

    # -- MengeFzgTyp ----------------------------------------------------------

    def test_menge_fzg_typ_full(self) -> None:
        d = {
            "BASIS_VERSION": 369, "FZG_TYP_NR": 5,
            "FZG_TYP_TEXT": "Solaris Urbino 12", "STR_FZG_TYP": "U12",
            "FZG_LAENGE": 12000, "FZG_TYP_HOEHE": 330, "FZG_TYP_BREITE": 255,
            "FZG_TYP_GEWICHT": 12000,
            "VERBRAUCH_DISTANZ": 800, "VERBRAUCH_ZEIT": 500,
        }
        obj = MengeFzgTyp.from_dict(d)
        assert obj.fzg_typ_text == "Solaris Urbino 12"
        assert obj.str_fzg_typ == "U12"
        assert obj.fzg_laenge == 12000
        assert obj.verbrauch is not None
        # 800 Wh/km → 0.8 kWh/km, 500 W → 0.5 kW, at 20 km/h = 0.025 kWh/km overhead
        assert abs(obj.verbrauch - 0.825) < 1e-9

    def test_menge_fzg_typ_minimal(self) -> None:
        d = {"BASIS_VERSION": 369, "FZG_TYP_NR": 7}
        obj = MengeFzgTyp.from_dict(d)
        assert obj.fzg_typ_text is None
        assert obj.fzg_laenge is None
        assert obj.verbrauch is None

    def test_menge_fzg_typ_zero_dims_become_none(self) -> None:
        d = {
            "BASIS_VERSION": 369, "FZG_TYP_NR": 3,
            "FZG_TYP_TEXT": "Bus", "STR_FZG_TYP": "B",
            "FZG_LAENGE": 0, "FZG_TYP_HOEHE": 0, "FZG_TYP_BREITE": 0,
        }
        obj = MengeFzgTyp.from_dict(d)
        assert obj.fzg_laenge is None
        assert obj.fzg_hoehe is None
        assert obj.fzg_breite is None
