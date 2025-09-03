from __future__ import annotations

from typing import Optional

from starrocks.reflection_info import ReflectionViewInfo
from starrocks.types import TableDistribution, TableEngine, TableType, ViewSecurityType
from starrocks.utils import TableAttributeNormalizer


class TableReflectionDefaults:
    """StarRocks table reflection default values management."""

    # StarRocks default properties that are automatically set
    # Note: These values should match StarRocks documentation defaults
    # Different defaults for different run modes
    _DEFAULT_PROPERTIES = {
        'compression': 'LZ4',
        'fast_schema_evolution': 'true',
        'replicated_storage': 'true',
        'storage_format': 'DEFAULT',
        'bucket_size': '4294967296',
    }
    DEFAULT_PROPERTIES_SHARED_NOTHING = {
        'replication_num': '3',
    } | _DEFAULT_PROPERTIES

    DEFAULT_PROPERTIES_SHARED_DATA = {
        'replication_num': '1',  # Different for shared-data
    } | _DEFAULT_PROPERTIES

    # Default table options
    # engine -> key -> comment -> partition -> distribution -> order by -> properties
    DEFAULT_ENGINE = TableEngine.OLAP
    DEFAULT_KEY = TableType.DUPLICATE_KEY
    # DEFAULT_COMMENT = ""
    DEFAULT_PARTITION_BY = None  # No default partition
    DEFAULT_DISTRIBUTED_BY = TableDistribution.RANDOM  # Default distribution is RANDOM
    DEFAULT_BUCKETS = 0  # Default buckets (from tables_conf when not set)
    DEFAULT_ORDER_BY = None  # No default order by

    @classmethod
    def get_default_properties(cls, run_mode: str = "shared_nothing") -> dict:
        """Get default properties based on run_mode."""
        if run_mode == "shared_data":
            return cls.DEFAULT_PROPERTIES_SHARED_DATA.copy()
        else:
            return cls.DEFAULT_PROPERTIES_SHARED_NOTHING.copy()

    @classmethod
    def normalize_engine(cls, engine: Optional[str]) -> str:
        """Normalize engine: None, empty, or OLAP are all treated as OLAP."""
        if engine is None or engine == '':
            return cls.DEFAULT_ENGINE
        return TableAttributeNormalizer.normalize_engine(engine)

    @classmethod
    def normalize_key(cls, key: Optional[str]) -> str:
        """Normalize key: None, empty, or DUPLICATE KEY are all treated as DUPLICATE KEY."""
        if key is None or key == '':
            return cls.DEFAULT_KEY
        return TableAttributeNormalizer.normalize_key(key)

    @classmethod
    def get_default_distribution(cls) -> str:
        """Get default distribution method."""
        return cls.DEFAULT_DISTRIBUTED_BY

    @classmethod
    def get_default_buckets(cls) -> int:
        """Get default buckets count."""
        return cls.DEFAULT_BUCKETS


class ReflectionViewDefaults:
    """Central place for view reflection default values and normalization."""

    DEFAULT_COMMENT: str = ""
    DEFAULT_SECURITY: str = ViewSecurityType.NONE

    @classmethod
    def apply(
        cls,
        *,
        name: str,
        definition: str,
        comment: str | None = None,
        security: str | None = None,
    ) -> ReflectionViewInfo:
        """Apply defaults and normalization to reflected view values.

        - comment: default empty string
        - security: default empty string, uppercase when present
        """
        normalized_comment = (comment or cls.DEFAULT_COMMENT)
        normalized_security = (security or cls.DEFAULT_SECURITY).upper()
        return ReflectionViewInfo(
            name=name,
            definition=definition,
            comment=normalized_comment,
            security=normalized_security,
        )

    @classmethod
    def apply_info(cls, reflectionViewInfo: ReflectionViewInfo) -> ReflectionViewInfo:
        """Apply defaults and normalization to reflected view values.
        """
        return ReflectionViewInfo(
            name=reflectionViewInfo.name,
            definition=reflectionViewInfo.definition,
            comment=(reflectionViewInfo.comment or cls.DEFAULT_COMMENT),
            security=(reflectionViewInfo.security or cls.DEFAULT_SECURITY).upper(),
        )
