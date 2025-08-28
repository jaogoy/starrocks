class TableType:
    PRIMARY_KEY = "PRIMARY KEY"
    DUPLICATE_KEY = "DUPLICATE KEY"
    AGGREGATE_KEY = "AGGREGATE KEY"
    UNIQUE_KEY = "UNIQUE KEY"

    DEFAULT = DUPLICATE_KEY

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


