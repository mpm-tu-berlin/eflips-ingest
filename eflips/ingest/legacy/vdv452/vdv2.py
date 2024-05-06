import os

from eflips.ingest.vdv import validate_input_data_vdv_451, VdvRequiredTables, import_vdv452_table_records

if __name__ == "__main__":
    path_to_this_file = os.path.dirname(os.path.abspath(__file__))
    sample_files_dir = os.path.join(path_to_this_file, "..", "..", "UVG")
    all_tables = validate_input_data_vdv_451(sample_files_dir)

    all_data = {}

    for tbl in all_tables:
        if tbl in VdvRequiredTables.required_tables.keys():
            all_data[tbl] = import_vdv452_table_records(all_tables[tbl])

    print("Done.")
