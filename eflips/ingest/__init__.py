from typing import List, Type

from eflips.ingest.base import AbstractIngester
from eflips.ingest.dummy import DummyIngester
from eflips.ingest.gtfs import GtfsIngester
from eflips.ingest.vdv import VdvIngester


def all_ingesters() -> List[Type[AbstractIngester]]:
    return [DummyIngester, GtfsIngester, VdvIngester]
