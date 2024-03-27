from typing import List, Type

from eflips.ingest.base import AbstractIngester
from eflips.ingest.dummy import DummyIngester


def all_ingesters() -> List[Type[AbstractIngester]]:
    return [DummyIngester]
