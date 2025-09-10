from .types import TableType, TableModel


DialectName: str = 'starrocks'
SRKwargsPrefix = 'starrocks_'
"""Prefix for StarRocks-specific kwargs."""


class TableInfoKey:
    """Centralizes starrocks_ prefixed kwargs for Table objects. Clean names without prefix."""

    # Individual key kwargs for clarity
    KEY = 'KEY'  # Not in the options, but used for comparison
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
    MODEL_TO_KEY_MAP = {
        TableModel.PRI_KEYS: PRIMARY_KEY,
        TableModel.PRI_KEYS2: PRIMARY_KEY,
        TableModel.UNQ_KEYS: UNIQUE_KEY,
        TableModel.UNQ_KEYS2: UNIQUE_KEY,
        TableModel.DUP_KEYS: DUPLICATE_KEY,
        TableModel.DUP_KEYS2: DUPLICATE_KEY,
        TableModel.AGG_KEYS: AGGREGATE_KEY,
        TableModel.AGG_KEYS2: AGGREGATE_KEY,
    }

    # Other table-level kwargs
    ENGINE = 'ENGINE'
    COMMENT = 'COMMENT'
    PARTITION_BY = 'PARTITION_BY'
    DISTRIBUTED_BY = 'DISTRIBUTED_BY'
    BUCKETS = 'BUCKETS'
    ORDER_BY = 'ORDER_BY'
    PROPERTIES = 'PROPERTIES'


TableInfoKey.ALL = {
    k for k, v in vars(TableInfoKey).items() if not callable(v) and not k.startswith("__")
}


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
    ENGINE = 'starrocks_ENGINE'
    COMMENT = 'starrocks_COMMENT'
    PARTITION_BY = 'starrocks_PARTITION_BY'
    DISTRIBUTED_BY = 'starrocks_DISTRIBUTED_BY'
    BUCKETS = 'starrocks_BUCKETS'
    ORDER_BY = 'starrocks_ORDER_BY'
    PROPERTIES = 'starrocks_PROPERTIES'


TableInfoKeyWithPrefix.ALL = {
    k for k, v in vars(TableInfoKeyWithPrefix).items() if not callable(v) and not k.startswith("__")
}


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
