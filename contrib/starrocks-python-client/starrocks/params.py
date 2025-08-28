class ColumnAggInfoKey:
    """StarRocks-specific Column.info keys for aggregate-model tables.

    - is_agg_key: mark a column as a KEY column in AGGREGATE KEY tables.
    - agg: specify the aggregate function for value columns (see ColumnAggType).
    """

    is_agg_key = "starrocks_is_agg_key"
    agg = "starrocks_agg"


