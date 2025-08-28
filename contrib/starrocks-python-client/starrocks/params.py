from .types import TableType


SRKwargsPrefix = 'starrocks_'

class TableInfoKey:
    """Centralizes starrocks_ prefixed kwargs for Table objects. Clean names without prefix."""

    # Individual key kwargs for clarity
    PRIMARY_KEY = 'primary_key'
    DUPLICATE_KEY = 'duplicate_key'
    AGGREGATE_KEY = 'aggregate_key'
    UNIQUE_KEY = 'unique_key'

    # Key type kwargs and their mapping to DDL strings
    KEY_KWARG_MAP = {
        PRIMARY_KEY: TableType.PRIMARY_KEY,
        DUPLICATE_KEY: TableType.DUPLICATE_KEY,
        AGGREGATE_KEY: TableType.AGGREGATE_KEY,
        UNIQUE_KEY: TableType.UNIQUE_KEY,
    }

    # Other table-level kwargs
    ENGINE = 'engine'
    PARTITION_BY = 'partition_by'
    DISTRIBUTED_BY = 'distributed_by'
    BUCKETS = 'buckets'
    ORDER_BY = 'order_by'
    PROPERTIES = 'properties'


class ColumnAggInfoKey:
    """StarRocks-specific Column.info keys for aggregate-model tables. Clean names without prefix.

    - is_agg_key: mark a column as a KEY column in AGGREGATE KEY tables.
    - agg_type: specify the aggregate function for value columns (see ColumnAggType).
    """

    is_agg_key = "is_agg_key"
    agg_type = "agg_type"


def _create_prefixed_class(base_cls, prefix):
    """Dynamically creates a class with attribute values prefixed with the given string."""
    new_attrs = {'__doc__': base_cls.__doc__}
    for name, value in vars(base_cls).items():
        if name.startswith('__') or callable(value):
            continue
        if isinstance(value, str):
            new_attrs[name] = f"{prefix}{value}"
        elif isinstance(value, dict):
            new_attrs[name] = {f"{prefix}{k}": v for k, v in value.items()}
    return type(f"{base_cls.__name__}WithPrefix", (), new_attrs)


TableInfoKeyWithPrefix = _create_prefixed_class(TableInfoKey, SRKwargsPrefix)
ColumnAggInfoKeyWithPrefix = _create_prefixed_class(ColumnAggInfoKey, SRKwargsPrefix)
