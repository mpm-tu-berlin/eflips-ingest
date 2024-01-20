import os

import pytest

from eflips.ingest.util import get_altitude_google, get_altitude_openelevation


@pytest.mark.skipif(
    not os.getenv("OPENELEVATION_URL"),
    reason="OPENELEVATION_URL not set",
)
def test_altitude_openelevation():
    coords = [(52.5552562, 13.3294346), (52.48458526, 13.41229386)]
    altitudes = [52.0, 67.0]
    for i in range(len(coords)):
        assert get_altitude_openelevation(coords[i]) == altitudes[i]


@pytest.mark.skipif(
    not os.getenv("OPENELEVATION_URL"),
    reason="OPENELEVATION_URL not set",
)
def test_altitude_openelevation_out_os_scope():
    coords = (0, 0)
    with pytest.raises(ValueError):
        get_altitude_openelevation(coords)


@pytest.mark.skipif(
    not os.getenv("GOOGLE_MAPS_API_KEY"),
    reason="GOOGLE_MAPS_API_KEY not set",
)
def test_altitude_google():
    coords = [(52.5552562, 13.3294346), (52.48458526, 13.41229386)]
    altitudes = [52.22081756591797, 68.6605224609375]
    for i in range(len(coords)):
        assert get_altitude_google(coords[i]) == altitudes[i]
