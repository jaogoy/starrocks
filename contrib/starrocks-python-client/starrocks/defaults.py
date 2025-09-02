from typing import Optional

from starrocks.types import TableEngine


class TableReflectionDefaults:
    """StarRocks table reflection default values management."""

    # StarRocks default properties that are automatically set
    # Note: These values should match StarRocks documentation defaults
    DEFAULT_PROPERTIES = {
        'compression': 'LZ4',
        'fast_schema_evolution': 'true',
        'replicated_storage': 'true',
        'storage_format': 'DEFAULT',
        'bucket_size': '4294967296',
        'replication_num': '3',
    }

    # Default engine
    DEFAULT_ENGINE = TableEngine.OLAP

    @classmethod
    def normalize_engine(cls, engine: Optional[str]) -> str:
        """Normalize engine: None, empty, or OLAP are all treated as OLAP."""
        if engine is None or engine == '' or engine == TableEngine.OLAP:
            return TableEngine.OLAP
        return engine
