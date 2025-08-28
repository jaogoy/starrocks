class TableType:
    PRIMARY_KEY = "PRIMARY KEY"
    DUPLICATE_KEY = "DUPLICATE KEY"
    AGGREGATE_KEY = "AGGREGATE KEY"
    UNIQUE_KEY = "UNIQUE KEY"

    DEFAULT = DUPLICATE_KEY

    # For validation and mapping
    ALL_KEY_TYPES = {PRIMARY_KEY, DUPLICATE_KEY, AGGREGATE_KEY, UNIQUE_KEY}


class TableModel:
    """ Table type in information_schema.tables_config.TABLE_MODEL
    """
    DUP_KEYS = "DUP_KEYS"
    AGG_KEYS = "AGG_KEYS"
    PRI_KEYS = "PRI_KEYS"
    UNQ_KEYS = "UNQ_KEYS"

    TO_TYPE_MAP = {
        DUP_KEYS: TableType.DUPLICATE_KEY,
        AGG_KEYS: TableType.AGGREGATE_KEY,
        PRI_KEYS: TableType.PRIMARY_KEY,
        UNQ_KEYS: TableType.UNIQUE_KEY,
    }


class ColumnAggType:
    """Supported StarRocks aggregate functions for value columns.

    Use these constants in Column.info["starrocks_agg"].
    """

    # Key column type, rather than aggregate value type
    KEY = "KEY"

    # Core aggregate types
    SUM = "SUM"
    COUNT = "COUNT"
    MIN = "MIN"
    MAX = "MAX"

    # Specialized aggregate types
    HLL_UNION = "HLL_UNION"
    BITMAP_UNION = "BITMAP_UNION"
    REPLACE = "REPLACE"
    REPLACE_IF_NOT_NULL = "REPLACE_IF_NOT_NULL"

    # Allowed set for validation
    ALLOWED = {
        SUM,
        COUNT,
        MIN,
        MAX,
        HLL_UNION,
        BITMAP_UNION,
        REPLACE,
        REPLACE_IF_NOT_NULL,
    }


