from __future__ import annotations

from typing import Optional, Union

from starrocks.reflection_info import ReflectedMVState, ReflectedViewState
from starrocks.types import SystemRunMode, TableDistribution, TableEngine, TableType, ViewSecurityType
from starrocks.utils import TableAttributeNormalizer


class ReflectionTableDefaults:
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
    _DEFAULT_PROPERTIES_SHARED_NOTHING = {
        'replication_num': '3',
    } | _DEFAULT_PROPERTIES

    _DEFAULT_PROPERTIES_SHARED_DATA = {
        'replication_num': '1',  # Different for shared-data
    } | _DEFAULT_PROPERTIES

    # Default table options
    # engine -> key -> comment -> partition -> distribution -> order by -> properties

    @classmethod
    def normalize_engine(cls, engine: Optional[str]) -> str:
        """Normalize engine: None, empty, or OLAP are all treated as OLAP."""
        if engine is None or engine == '':
            return cls.engine()
        return TableAttributeNormalizer.normalize_engine(engine)

    @classmethod
    def normalize_key(cls, key: Optional[str]) -> str:
        """Normalize key: None, empty, or DUPLICATE KEY are all treated as DUPLICATE KEY."""
        if key is None or key == '':
            return cls.key()
        return TableAttributeNormalizer.normalize_key(key)

    @classmethod
    def engine(cls) -> str:
        return TableEngine.OLAP
    
    @classmethod
    def key(cls) -> str:
        return TableType.DUPLICATE_KEY

    @classmethod
    def partition_by(cls) -> Optional[str]:
        return None

    @classmethod
    def distribution_type(cls) -> str:
        """Get default distribution method. such as HASH, RANDOM."""
        return TableDistribution.RANDOM

    @classmethod
    def distribution_columns(cls) -> Optional[Union[list[str], str]]:
        """Get default distribution keys. such as id, name."""
        return None

    @classmethod
    def buckets(cls) -> int:
        """Get default buckets count. such as 8."""
        return 0
    
    @classmethod
    def distribution(cls) -> str:
        """Get default distribution by. such as 'HASH(id) BUCKETS 10'."""
        return TableDistribution.RANDOM

    @classmethod
    def order_by(cls) -> Optional[str]:
        """Get default order by."""
        return None

    @classmethod
    def properties(cls, run_mode: str = SystemRunMode.SHARED_NOTHING) -> dict:
        """Get default properties based on run_mode.
        Keep mind not to change the default properties, because it will affect the table creation.
        Or you need to make a copy of the default properties.
        """
        if run_mode == SystemRunMode.SHARED_DATA:
            return cls._DEFAULT_PROPERTIES_SHARED_DATA
        else:
            return cls._DEFAULT_PROPERTIES_SHARED_NOTHING



class ReflectionViewDefaults:
    """Central place for view reflection default values and normalization."""

    @classmethod
    def comment(cls) -> str:
        return ""
    
    @classmethod
    def security(cls) -> str:
        return ViewSecurityType.EMPTY

    @classmethod
    def apply(
        cls,
        *,
        name: str,
        definition: str,
        comment: str | None = None,
        security: str | None = None,
    ) -> ReflectedViewState:
        """Apply defaults and normalization to reflected view values.

        - comment: default empty string
        - security: default empty string, uppercase when present
        """
        normalized_comment = (comment or cls.comment())
        normalized_security = (security or cls.security()).upper()
        return ReflectedViewState(
            name=name,
            definition=definition,
            comment=normalized_comment,
            security=normalized_security,
        )

    @classmethod
    def apply_info(cls, reflection_view_info: ReflectedViewState) -> ReflectedViewState:
        """Apply defaults and normalization to reflected view values.
        """
        return ReflectedViewState(
            name=reflection_view_info.name,
            definition=reflection_view_info.definition,
            comment=(reflection_view_info.comment or cls.comment()),
            security=(reflection_view_info.security or cls.security()).upper(),
        )


class ReflectionMVDefaults:
    """Central place for materialized view reflection default values and normalization."""

    @classmethod
    def comment(cls) -> str:
        return ""

    @classmethod
    def security(cls) -> str:
        return ViewSecurityType.DEFINER

    @classmethod
    def apply(cls, *, name: str, definition: str, comment: str | None = None, security: str | None = None) -> ReflectionMaterializedViewInfo:
        """Apply defaults and normalization to reflected materialized view values.
        """
        return ReflectedMVState(
            name=name,
            definition=definition,
            comment=(comment or cls.comment()),
            security=(security or cls.security()).upper(),
        )