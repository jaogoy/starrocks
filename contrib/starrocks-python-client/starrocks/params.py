from .types import TableType


DialectName: str = 'starrocks'
SRKwargsPrefix = 'starrocks_'
"""Prefix for StarRocks-specific kwargs."""


class TableInfoKey:
    """Centralizes starrocks_ prefixed kwargs for Table objects. Clean names without prefix."""

    # Individual key kwargs for clarity
    PRIMARY_KEY = 'PRIMARY_KEY'
    DUPLICATE_KEY = 'DUPLICATE_KEY'
    AGGREGATE_KEY = 'AGGREGATE_KEY'
    UNIQUE_KEY = 'UNIQUE_KEY'

    # Key type kwargs and their mapping to DDL strings
    KEY_KWARG_MAP = {
        PRIMARY_KEY: TableType.PRIMARY_KEY,
        DUPLICATE_KEY: TableType.DUPLICATE_KEY,
        AGGREGATE_KEY: TableType.AGGREGATE_KEY,
        UNIQUE_KEY: TableType.UNIQUE_KEY,
    }

    # Other table-level kwargs
    ENGINE = 'ENGINE'
    KEY = 'KEY'
    PARTITION_BY = 'PARTITION_BY'
    DISTRIBUTED_BY = 'DISTRIBUTED_BY'
    BUCKETS = 'BUCKETS'
    ORDER_BY = 'ORDER_BY'
    PROPERTIES = 'PROPERTIES'


class ColumnAggInfoKey:
    """StarRocks-specific Column.info keys for aggregate-model tables. Clean names without prefix.

    - IS_AGG_KEY: mark a column as a KEY column in AGGREGATE KEY tables.
    - AGG_TYPE: specify the aggregate function for value columns (see ColumnAggType).
    """

    IS_AGG_KEY = "IS_AGG_KEY"
    AGG_TYPE = "AGG_TYPE"


class TableInfoKeyWithPrefix:
    """Centralizes starrocks_ prefixed kwargs for Table objects. Full prefixed names."""

    # Individual key kwargs for clarity
    PRIMARY_KEY = 'starrocks_PRIMARY_KEY'
    DUPLICATE_KEY = 'starrocks_DUPLICATE_KEY'
    AGGREGATE_KEY = 'starrocks_AGGREGATE_KEY'
    UNIQUE_KEY = 'starrocks_UNIQUE_KEY'

    # Key type kwargs and their mapping to DDL strings
    KEY_KWARG_MAP = {
        PRIMARY_KEY: TableType.PRIMARY_KEY,
        DUPLICATE_KEY: TableType.DUPLICATE_KEY,
        AGGREGATE_KEY: TableType.AGGREGATE_KEY,
        UNIQUE_KEY: TableType.UNIQUE_KEY,
    }

    # Other table-level kwargs
    KEY = 'starrocks_KEY'
    ENGINE = 'starrocks_ENGINE'
    PARTITION_BY = 'starrocks_PARTITION_BY'
    DISTRIBUTED_BY = 'starrocks_DISTRIBUTED_BY'
    BUCKETS = 'starrocks_BUCKETS'
    ORDER_BY = 'starrocks_ORDER_BY'
    PROPERTIES = 'starrocks_PROPERTIES'


class ColumnAggInfoKeyWithPrefix:
    """StarRocks-specific Column.info keys for aggregate-model tables. Full prefixed names.

    - IS_AGG_KEY: mark a column as a KEY column in AGGREGATE KEY tables.
    - AGG_TYPE: specify the aggregate function for value columns (see ColumnAggType).
    """

    IS_AGG_KEY = "starrocks_IS_AGG_KEY"
    AGG_TYPE = "starrocks_AGG_TYPE"


ColumnSROptionsKey: str = "column_sr_options"
"""Column StarRocks-specific options key, used to store StarRocks-specific options in the Column object.
"""
