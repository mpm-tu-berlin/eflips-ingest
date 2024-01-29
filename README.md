[![Unit Tests](https://github.com/mpm-tu-berlin/eflips-ingest/actions/workflows/unittests.yml/badge.svg)](https://github.com/mpm-tu-berlin/eflips-ingest/actions/workflows/unittests.yml)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

# eflips-ingest

---

Part of the [eFLIPS/simBA](https://github.com/stars/ludgerheide/lists/ebus2030) list of projects.

---

This repository contains code to import bus schedules from various sources into an [eFLIPS-Model](https://github.com/mpm-tu-berlin/eflips-model) database.

## Installation

1. Set up a [PostgreSQL](https://www.postgresql.org/) database with the [PostGIS](https://postgis.net/) extension and `BTREE_gist` enabled.
   ```bash
   apt install postgresql postgis
   sudo -u postgres psql createdb eflips
   sudo -u postgres psql eflips -c "CREATE EXTENSION postgis;"
   sudo -u postgres psql eflips -c "CREATE EXTENSION btree_gist;"
   ```

2. Clone this git repository (or [download a specific release](https://github.com/mpm-tu-berlin/eflips-depot/releases))
    ```bash
    git clone git@github.com:mpm-tu-berlin/eflips-model.git
    ```
3. Install the packages listed in `poetry.lock` and `pyproject.toml` into your Python environment. Notes:
    - This project depends on [pyproj](https://pyproj4.github.io/pyproj/stable/installation.html), which may require the `proj-bin` package (`apt install proj-bin` on Ubuntu).
    - The supported platforms are macOS and Linux.
    - Using the [poetry](https://python-poetry.org/) package manager is recommended. It can be installed accoring to the
      instructions listed [here](https://python-poetry.org/docs/#installing-with-the-official-installer).
    ```bash
    poetry install
    ```

## Usage

### Command line

The code is organized into various Python files under the `eflips/ingest` folder, each for ingesting a specific data source. These files should be runnable using `python eflips/ingest/x.py` The following data sources are currently supported:

- `bvgxml.py`: XML files emitted by BVG's proprietary software. 
  - Requires at least `GOOGLE_MAPS_API_KEY`, also `OPENELEVATION_URL` is suggested to save money on Google Maps API calls.
  - Known Limitations:
    - The emitted `Rotations` may be partial (i. e. there may be ine for the first half of the day, ending somewhere outside of the depot and one on the second half of the day, starting outside the depot.) *You may want to merge those*.
    - Trips and Rotations on the first and last day of a week-long schedule may overlap, which will lead to them appearing *twice* if a weekly-wrapping simulation is used. *Check if there are rotations/trips offset by exactly one week and remove one of them*.

### API

To include the code in your own project, please review the source code of the ingest scripts and use the functions provided there. The API is not yet stable and may change without notice.

## Testing

---

**NOTE**: Be aware that the tests will clear the database specified in the `DATABASE_URL` environment variable. Make sure that you are not using a database that you want to keep.

---

Testing is done using the `pytest` framework with tests located in the `tests`directory. To run the tests, execute the following command in the root directory of the repository:

```bash
   export PYTHONPATH=tests:. # To make sure that the tests can find the eflips package
   export DATABASE_URL=postgis://postgres:postgres@localhost:5432/postgres # Or whatever your database URL is
   export GOOGLE_MAPS_API_KEY=put_your_api_key_here # Required for some tests
   export OPENELEVATION_URL=put_your_url_here # Optional, required for some tests
   pytest
```



## Development

We utilize the [GitHub Flow](https://docs.github.com/get-started/quickstart/github-flow) branching structure. This means
that the `main` branch is always deployable and that all development happens in feature branches. The feature branches
are merged into `main` via pull requests.


We use [black](https://black.readthedocs.io/en/stable/) for code formatting. You can use 
[pre-commit](https://pre-commit.com/) to ensure the code is formatted correctly before committing. You are also free to
use other methods to format the code, but please ensure that the code is formatted correctly before committing.

Please make sure that your `poetry.lock` and `pyproject.toml` files are consistent before committing. You can use `poetry check` to check this. This is also checked by pre-commit.

## License

This project is licensed under the AGPLv3 license - see the [LICENSE](LICENSE.md) file for details.

## Funding Notice

This code was developed as part of the project [eBus2030+](https://www.eflip.de/) funded by the Federal German Ministry for Digital and Transport (BMDV) under grant number 03EMF0402.