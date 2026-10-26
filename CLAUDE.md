# eflips-ingest

Importers (BVG-XML, VDV 451/452, GTFS) that write bus schedules into an eflips-model database. Python ≥3.11, Poetry.

## Commands
- Tests: `PYTHONPATH=tests:. DATABASE_URL=… ELEVATION_DUMMY_MODE=True pytest` (single test: `pytest tests/test_x.py::TestClass::test_name`)
- IMPORTANT: the test fixture runs `drop_all()` on `DATABASE_URL`. Use a throwaway database, e.g. `sqlite:///./test_database.db` (needs `SPATIALITE_LIBRARY_PATH`).
- CI runs the suite on both Postgres/PostGIS and SQLite/SpatiaLite; changes must work on both.
- black, `mypy eflips --explicit-package-bases --strict` and `poetry check` are enforced by hooks in `.claude/settings.json` and by CI.
- `samples/*.zip` are Git LFS objects.

## Pull requests
Bump `version` in `pyproject.toml` in every PR (CI compares against PyPI; merging to `main` publishes):
- patch: no API change
- minor: backwards-compatible API change, e.g. a new feature or `prepare()` parameter
- major: breaking change

## Ingester contract
- Every source subclasses `AbstractIngester` (`eflips/ingest/base.py`) and is listed in `all_ingesters()`.
- `prepare()` kwargs may only be `str`/`int`/`float`/`bool`/`Enum`/`pathlib.Path`, because UIs build forms from the signature. Each kwarg needs an entry in `prepare_param_names()` and `prepare_param_description()`.
- `ingest(uuid)` may run in a different process from `prepare()`, so it works only from the pickle in `path_for_uuid(uuid)`.
- Ingester tests subclass `tests/base.py::BaseIngester`.
- Collect all coordinates and call `eflips.ingest.util.altitude_map()` once per import. Google bills per request.

## Dependencies
Add them with `poetry add`; never hand-edit `poetry.lock`. Ask before adding a runtime dependency.

## Domain
A Betriebstag runs 03:00–03:00 local time. Times after midnight belong to the previous day and are written 24:00, 25:00, ….
