import json
import logging
import os
from functools import lru_cache
from numbers import Number
from tempfile import gettempdir
from typing import Tuple

import requests
from pyproj import Transformer

import eflips.ingest

"""
Some utility functions for the ingest module.
"""

# The transformer is initialized here, so that it is only initialized once
transformer = Transformer.from_crs("EPSG:3068", "EPSG:4326")


@lru_cache(maxsize=4096)
def get_altitude_openelevation(latlon: Tuple[Number, Number]) -> float:
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
        return data["results"][0]["elevation"]
    else:
        raise ValueError("No elevation found")


# Since this is paid API we at least try to cache the results
def get_altitude_google(latlon: Tuple[float, float]) -> float:
    """
    Get altitude infomration for a given latitude and longitude
    """
    # Try loading the result from a cache file
    cache_dir = os.path.join(gettempdir(), "eflips_cache")
    # Create folders by lat, lon
    this_coord_dir = os.path.join(cache_dir, f"{int(latlon[0]*100)},{int(latlon[1]*100)}")
    os.makedirs(this_coord_dir, exist_ok=True)
    this_coord_file = os.path.join(this_coord_dir, f"{latlon}.json")
    if os.path.exists(this_coord_file):
        with open(this_coord_file, "r") as f:
            try:
                loaded = json.load(f)
                assert isinstance(loaded, float) or isinstance(loaded, int)
                return float(loaded)
            except json.JSONDecodeError:
                logging.error(f"Failed to load cache file {this_coord_file}")
                os.remove(this_coord_file)

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

    # Save the result to a cache file
    os.makedirs(this_coord_dir, exist_ok=True)
    with open(this_coord_file, "w") as f:
        json.dump(altitude, f)

    return altitude


def get_altitude(latlon: Tuple[float, float]) -> float:
    """
    Get altitude infomration for a given latitude and longitude
    """
    if "ELEVATION_DUMMY_MODE" in os.environ:
        if os.environ["ELEVATION_DUMMY_MODE"] == "True":
            return 9999.0

    try:
        return get_altitude_openelevation(latlon)
    except ValueError:
        return get_altitude_google(latlon)


def soldner_to_pointz(x: float, y: float) -> str:
    """
    Converts a Soldner coordinate to a PostGIS POINTZ string, also setting the altitude using API lookups

    :param x: the x coordinate, in millimiters as per the BVG specification
    :param y: the y coordinate, in millimiters as per the BVG specification
    :return: a PostGIS POINTZ string. The altitude is calculated using the lookup methods from the
             eflips.ingest.util module
    """
    lat, lon = transformer.transform(y / 1000, x / 1000)
    z = eflips.ingest.util.get_altitude((lat, lon))

    return f"SRID=4326;POINTZ({lon} {lat} {z})"
