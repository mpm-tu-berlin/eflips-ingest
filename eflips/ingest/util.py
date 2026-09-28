"""
Some utility functions for the ingest module.
"""
from typing import Dict, Iterable, Tuple

from pyproj import Transformer

from eflips.model.util import geometry_has_z, get_altitudes

# The transformer is initialized here, so that it is only initialized once
transformer = Transformer.from_crs("EPSG:3068", "EPSG:4326")

LatLon = Tuple[float, float]


def altitude_map(latlons: Iterable[LatLon]) -> Dict[LatLon, float]:
    """
    Look up the altitude of many coordinates in one batch.

    This is the one place an ingester should fetch altitudes from: collect every coordinate
    first, call this once, then build geometries from the returned mapping. Google bills the
    elevation API per request (of up to 512 points), not per point.

    :param latlons: ``(latitude, longitude)`` pairs; duplicates are fine
    :return: ``{(latitude, longitude): altitude}`` for every distinct input pair. Empty if the
        model's geometry types carry no Z coordinate.
    """
    if not geometry_has_z():
        return {}
    unique = list(dict.fromkeys(latlons))
    return dict(zip(unique, get_altitudes(unique)))


def soldner_to_latlon(x: float, y: float) -> LatLon:
    """
    Convert a Soldner coordinate (EPSG:3068) to WGS84.

    :param x: the x coordinate, in millimeters as per the BVG specification
    :param y: the y coordinate, in millimeters as per the BVG specification
    :return: ``(latitude, longitude)``
    """
    lat, lon = transformer.transform(y / 1000, x / 1000)
    return float(lat), float(lon)


def soldner_to_pointz_many(xys: Iterable[Tuple[float, float]]) -> Dict[Tuple[float, float], str]:
    """
    Convert many Soldner coordinates to PostGIS POINTZ strings with a single altitude lookup.

    :param xys: ``(x, y)`` pairs in millimeters as per the BVG specification
    :return: ``{(x, y): "SRID=4326;POINTZ(lon lat z)"}`` for every distinct input pair. If the
        model's geometry types carry no Z coordinate the values are ``POINT`` strings instead.
    """
    unique = list(dict.fromkeys(xys))
    latlons = [soldner_to_latlon(x, y) for x, y in unique]
    altitudes = altitude_map(latlons)

    geoms: Dict[Tuple[float, float], str] = {}
    for xy, (lat, lon) in zip(unique, latlons):
        if altitudes:
            geoms[xy] = f"SRID=4326;POINTZ({lon} {lat} {altitudes[(lat, lon)]})"
        else:
            geoms[xy] = f"SRID=4326;POINT({lon} {lat})"
    return geoms


def soldner_to_pointz(x: float, y: float) -> str:
    """
    Converts a single Soldner coordinate to a PostGIS POINTZ string, also setting the altitude using API lookups.

    Prefer :func:`soldner_to_pointz_many` when converting more than one point: it looks all
    altitudes up in one request.

    :param x: the x coordinate, in millimeters as per the BVG specification
    :param y: the y coordinate, in millimeters as per the BVG specification
    :return: a PostGIS POINTZ string. The altitude is calculated using the lookup methods from the
             eflips.model.util module
    """
    return soldner_to_pointz_many([(x, y)])[(x, y)]
