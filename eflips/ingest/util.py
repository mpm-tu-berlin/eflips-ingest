from pyproj import Transformer

from eflips.model.util import geometry_has_z, get_altitude

"""
Some utility functions for the ingest module.
"""

# The transformer is initialized here, so that it is only initialized once
transformer = Transformer.from_crs("EPSG:3068", "EPSG:4326")


def soldner_to_pointz(x: float, y: float) -> str:
    """
    Converts a Soldner coordinate to a PostGIS POINTZ string, also setting the altitude using API lookups

    :param x: the x coordinate, in millimiters as per the BVG specification
    :param y: the y coordinate, in millimiters as per the BVG specification
    :return: a PostGIS POINTZ string. The altitude is calculated using the lookup methods from the
             eflips.model.util module
    """
    lat, lon = transformer.transform(y / 1000, x / 1000)

    # Check the type of eflips.model.Station.geom
    if geometry_has_z():
        z = get_altitude((lat, lon))

        return f"SRID=4326;POINTZ({lon} {lat} {z})"
    else:
        return f"SRID=4326;POINT({lon} {lat})"
