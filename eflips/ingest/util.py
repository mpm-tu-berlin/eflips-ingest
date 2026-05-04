import json
import logging
import os
from functools import lru_cache
from numbers import Number
from pathlib import Path
from tempfile import gettempdir
from typing import Tuple

import geoalchemy2
import platformdirs
import requests
from pyproj import Transformer
from eflips.model import AssocRouteStation, Route, Station
import eflips.ingest
from kv_cache import KVStore  # type: ignore[import-untyped]

# The KV Store is module-level, so that it is only initialized once and can be used across all functions in this module
cache_dir = Path(platformdirs.user_cache_dir("eflips", "de.tu-berlin", "1"))
cache_file = cache_dir / Path("eflips_ingest_altitude_cache.db")
store = KVStore(str(cache_file.absolute()))

"""
Some utility functions for the ingest module.
"""

# The transformer is initialized here, so that it is only initialized once
transformer = Transformer.from_crs("EPSG:3068", "EPSG:4326")


def get_altitude_openelevation(latlon: Tuple[float, float]) -> float:
    """
    Get altitude infomration for a given latitude and longitude
    """

    # If there is no "OPENELEVATION_URL" environment variable, fail
    # with an error message
    if not os.getenv("OPENELEVATION_URL"):
        raise ValueError("OPENELEVATION_URL not set")
    url = f"{os.getenv('OPENELEVATION_URL')}/api/v1/lookup?locations={latlon[0]},{latlon[1]}"

    # We use the requests library to get the data
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    if "elevation" in data["results"][0]:
        assert isinstance(data["results"][0]["elevation"], float) or isinstance(data["results"][0]["elevation"], int)
        result = data["results"][0]["elevation"]
        # 0 (which is bad, because it can actually exist) and < -9000 are sentinel values for "no elevation found"
        if result == 0 or result < -9000:
            raise ValueError("No elevation found")
        return result
    else:
        raise ValueError("No elevation found")


def get_altitude_google(latlon: Tuple[float, float]) -> float:
    """
    Get altitude infomration for a given latitude and longitude
    """
    if not os.getenv("GOOGLE_MAPS_API_KEY"):
        raise ValueError("GOOGLE_MAPS_API_KEY not set")

    url = f"https://maps.googleapis.com/maps/api/elevation/json?locations={latlon[0]},{latlon[1]}&key={os.getenv('GOOGLE_MAPS_API_KEY')}"

    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    if data["status"] != "OK":
        raise ValueError("No elevation found")
    assert isinstance(data["results"][0]["elevation"], float) or isinstance(data["results"][0]["elevation"], int)

    altitude = data["results"][0]["elevation"]

    return altitude


def get_altitude(latlon: Tuple[float, float]) -> float:
    """
    Get altitude information for a given latitude and longitude
    """
    if "ELEVATION_DUMMY_MODE" in os.environ:
        if os.environ["ELEVATION_DUMMY_MODE"] == "True":
            return 9999.0

    # We see whether a cache exist for the coordinate (rounded to the nearest 4 digits ~= 11m)
    # If it exists, we load it and return it.
    # Try loading the result from a cache file

    rounded_lat = round(latlon[0], 4)
    rounded_lon = round(latlon[1], 4)
    cache_key = f"{rounded_lat},{rounded_lon}"
    result_or_none = store.get(cache_key, default=None)
    if isinstance(result_or_none, float) or isinstance(result_or_none, int):
        altitude = result_or_none
    elif result_or_none is None:
        try:
            altitude = get_altitude_openelevation(latlon)
        except ValueError:
            altitude = get_altitude_google(latlon)
        store.set(cache_key, altitude)
    else:
        raise ValueError(f"Invalid cache value for key {cache_key}: {result_or_none}")
    return altitude


def soldner_to_pointz(x: float, y: float) -> str:
    """
    Converts a Soldner coordinate to a PostGIS POINTZ string, also setting the altitude using API lookups

    :param x: the x coordinate, in millimiters as per the BVG specification
    :param y: the y coordinate, in millimiters as per the BVG specification
    :return: a PostGIS POINTZ string. The altitude is calculated using the lookup methods from the
             eflips.ingest.util module
    """
    lat, lon = transformer.transform(y / 1000, x / 1000)

    # Check the type of eflips.model.Station.geom
    if geometry_has_z():
        z = eflips.ingest.util.get_altitude((lat, lon))

        return f"SRID=4326;POINTZ({lon} {lat} {z})"
    else:
        return f"SRID=4326;POINT({lon} {lat})"


def geometry_has_z() -> bool:
    """
    Check whether the geometry types of Station, Route and AssocRouteStation have Z coordinates.
    :return: True if they have Z coordinates, False otherwise
    """

    assert isinstance(AssocRouteStation.location.type, geoalchemy2.types.Geometry)
    assert isinstance(Station.geom.type, geoalchemy2.types.Geometry)
    assert isinstance(Route.geom.type, geoalchemy2.types.Geometry)
    if Station.geom.type.geometry_type == "POINTZ":
        assert (
            AssocRouteStation.location.type.geometry_type == "POINTZ"
        ), f"Inconsistent geometry types: {Station.geom.type.geometry_type } vs {AssocRouteStation.location.type.geometry_type }"
        assert (
            Route.geom.type.geometry_type == "LINESTRINGZ"
        ), f"Inconsistent geometry types: {Station.geom.type.geometry_type } vs {Route.geom.type.geometry_type }"
        has_z = True
    elif Station.geom.type.geometry_type == "POINT":
        assert (
            AssocRouteStation.location.type.geometry_type == "POINT"
        ), f"Inconsistent geometry types: {Station.geom.type.geometry_type } vs {AssocRouteStation.location.type.geometry_type }"
        assert (
            Route.geom.type.geometry_type == "LINESTRING"
        ), f"Inconsistent geometry types: {Station.geom.type.geometry_type } vs {Route.geom.type.geometry_type }"
        has_z = False
    else:
        raise ValueError("eflips.model.Station.geom has unsupported geometry type")
    return has_z
