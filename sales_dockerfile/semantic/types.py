# semantic/types.py

from dataclasses import dataclass
from typing import List, Optional


@dataclass
class Fact:
    name: str
    table: str
    grain: str
    primary_date: str


@dataclass
class DateDimension:
    name: str
    column: str
    sql: str
    supported_filters: List[str]
    supported_grains: List[str]


@dataclass
class Dimension:
    name: str
    column: str
    type: str


@dataclass
class Measure:
    name: str
    aggregation: str
    column: Optional[str] = None
    sql: Optional[str] = None
    mandatory_filters: Optional[List[str]] = None


@dataclass
class Metric:
    name: str
    expression: str
    behavior: str
    description: str = ""
    filters: Optional[List[str]] = None
    min_safe_grain: Optional[str] = None