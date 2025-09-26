from __future__ import annotations

import dataclasses
from enum import Enum
from typing import Any, NamedTuple, Optional, Tuple, TypedDict, Union

from sqlalchemy.engine.interfaces import ReflectedColumn


"""
Follow the mysql's ReflectedState, but with more specific types
It will be much cleaner and easier to use.
"""


@dataclasses.dataclass(**dict(kw_only=True) if 'KW_ONLY' in dataclasses.__all__ else {})
class ReflectedStateV1(object):
    """Stores information about table or view."""
    table_name: str | None = None
    columns: list[dict] = dataclasses.field(default_factory=list)
    table_options: dict[str, str] = dataclasses.field(default_factory=dict)
    keys: list[dict] = dataclasses.field(default_factory=list)
    fk_constraints: list[dict] = dataclasses.field(default_factory=list)
    ck_constraints: list[dict] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(**dict(kw_only=True) if 'KW_ONLY' in dataclasses.__all__ else {})
class ReflectedState(object):
    table_name: Optional[str]
    columns: list[ReflectedColumn]
    table_options: dict[str, str]  = dataclasses.field(default_factory=dict)
    keys: list[Union[ReflectedIndexInfo, ReflectedPKInfo, ReflectedUKInfo]]  = dataclasses.field(default_factory=list)
    fk_constraints: list[ReflectedFKInfo] = dataclasses.field(default_factory=list)
    ck_constraints: list[ReflectedCKInfo] = dataclasses.field(default_factory=list)


class MySQLKeyType(Enum):
    PRIMARY = "PRIMARY"
    UNIQUE = "UNIQUE"
    FULLTEXT = "FULLTEXT"
    SPATIAL = "SPATIAL"


class ReflectedIndexInfo(TypedDict):
    """In ReflectedState.keys
    And, will be used to form ReflectedIndex
    """
    name: str
    type: MySQLKeyType
    parser: Optional[Any]  # Object?
    columns: list[Tuple[str, int, Any]]  # name, length, ...
    dialect_options: dict[str, Any]


class ReflectedFKInfo(TypedDict):
    """In ReflectedState.fk_constraints
    And, will be used to form ReflectedForeignKeyConstraint
    """
    name: str
    table: NamedTuple[Optional[str], str]  # schema, name
    local: list[str]
    foreign: list[str]

    onupdate: bool
    ondelete: bool


class ReflectedPKInfo(TypedDict):
    """In ReflectedState.keys
    And, will be used to form ReflectedPrimaryKeyConstraint
    """
    type: MySQLKeyType
    columns: list[Tuple[str, Any, Any]]


class ReflectedUKInfo(TypedDict):
    """In ReflectedState.keys
    And, will be used to form ReflectedUniqueConstraint
    """
    name: str
    type: MySQLKeyType
    columns: list[Tuple[str, Any, Any]]


class ReflectedCKInfo(TypedDict):
    """In ReflectedState.ck_constraints
    And, will be used to form ReflectedCheckConstraint
    """
    name: str
    sqltext: str


@dataclasses.dataclass(**dict(kw_only=True) if 'KW_ONLY' in dataclasses.__all__ else {})
class ReflectedViewState:
    """Stores reflection information about a view."""
    name: str
    definition: str
    comment: str | None = None
    security: str | None = None


@dataclasses.dataclass(**dict(kw_only=True) if 'KW_ONLY' in dataclasses.__all__ else {})
class ReflectedMVState:
    """Stores reflection information about a materialized view."""
    name: str
    definition: str
    comment: str | None = None
    security: str | None = None


@dataclasses.dataclass(**dict(kw_only=True, frozen=True) if 'KW_ONLY' in dataclasses.__all__ else {})
class ReflectedPartitionInfo:
    """
    Stores structured reflection information about a table's partitioning scheme.

    Attributes:
        type: The partitioning type (e.g., 'RANGE', 'LIST', 'EXPRESSION').
        partition_by: The whole partitioning expression string
            (e.g., 'RANGE(id, name)', 'date_trunc('day', dt)', 'id, col1, col2').
        pre_created_partitions: A string containing the full DDL for all
            pre-created partitions (e.g.,
            "(PARTITION p1 VALUES LESS THAN ('100'), PARTITION p2 VALUES LESS THAN ('200'))").
    """
    type: str
    partition_method: str
    pre_created_partitions: Optional[str] = None

    def __str__(self) -> str:
        if self.pre_created_partitions:
            return f"{self.partition_method} {self.pre_created_partitions}"
        return f"{self.partition_method}"

    def __repr__(self) -> str:
        return repr(str(self))


@dataclasses.dataclass(**dict(kw_only=True) if 'KW_ONLY' in dataclasses.__all__ else {})
class ReflectedDistributionInfo:
    """Stores reflection information about a view."""
    type: str | None
    """The distribution type string like 'HASH' or 'RANDOM'."""
    columns: Optional[list[str], str] | None
    """The distribution columns string like 'id' or 'id, name'."""
    distribution_method: str | None
    """The distribution method string like 'HASH(id)' or 'RANDOM' without BUCKETS.
    It will be used first if it's not None."""
    buckets: int | None
    """The buckets count."""

    def __str__(self) -> str:
        """Convert to string representation of distribution option."""
        buckets_str = f' BUCKETS {self.buckets}' if self.buckets and str(self.buckets) != "0" else ""
        if not self.distribution_method:
            distribution_cols = ', '.join(self.columns) if isinstance(self.columns, list) else self.columns
            distribution_cols_str = f'({distribution_cols})' if distribution_cols else ""
            self.distribution_method = f'{self.type}{distribution_cols_str}'
        return f'{self.distribution_method}{buckets_str}'

    def __repr__(self) -> str:
        return repr(str(self))
