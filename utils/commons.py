from dataclasses import dataclass
from typing import List, Tuple
import pandas as pd

@dataclass
class Endpoint:
    url: str
    name: str
    body: dict
    params: dict
    headers: List[dict]

@dataclass
class JoinCriteria:
    endpoint1_name: str
    endpoint1_columns: List[str]
    endpoint2_name: str
    endpoint2_columns: List[str]

@dataclass
class Response:
    tokenResultTable: str
    total: int
    rows: int
    records : object