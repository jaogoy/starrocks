from __future__ import annotations

import dataclasses
from typing import Optional


@dataclasses.dataclass(**dict(kw_only=True) if 'KW_ONLY' in dataclasses.__all__ else {})
class ReflectedState(object):
    """Stores information about table or view."""

    table_name: str | None = None
    columns: list[dict] = dataclasses.field(default_factory=list)
    table_options: dict[str, str] = dataclasses.field(default_factory=dict)
    keys: list[dict] = dataclasses.field(default_factory=list)
    fk_constraints: list[dict] = dataclasses.field(default_factory=list)
    ck_constraints: list[dict] = dataclasses.field(default_factory=list)


@dataclasses.dataclass(kw_only=True)
class ReflectionViewInfo:
    """Stores reflection information about a view."""
    name: str
    definition: str
    comment: str | None = None
    security: str | None = None


@dataclasses.dataclass(kw_only=True)
class ReflectionMVInfo:
    """Stores reflection information about a materialized view."""
    name: str
    definition: str
    comment: str | None = None
    security: str | None = None


@dataclasses.dataclass(kw_only=True)
class ReflectionDistributionInfo:
    """Stores reflection information about a view."""
    type: str
    keys: Optional[list[str], str]
    buckets: int

    def to_string(self) -> str:
        """Convert to string representation of distribution option."""
        distribution_cols = ', '.join(self.keys) if isinstance(self.keys, list) else str(self.keys)
        distribution_str = f'({distribution_cols})' if distribution_cols else ""
        buckets_str = f' BUCKETS {self.buckets}' if self.buckets and str(self.buckets) != "0" else ""
        return f'{self.type}{distribution_str} {buckets_str}'
