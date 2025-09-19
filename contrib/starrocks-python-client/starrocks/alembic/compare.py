import logging
from functools import wraps
from typing import Any, Dict, List, Optional, Set, Tuple, Union, Callable

from alembic.autogenerate import comparators
from alembic.autogenerate.api import AutogenContext

from alembic.operations.ops import AlterColumnOp, AlterTableOp, UpgradeOps
from sqlalchemy import Column, quoted_name
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import NotSupportedError
from sqlalchemy.sql.schema import Table

from starrocks.defaults import ReflectionTableDefaults
from starrocks.params import (
    AlterTableEnablement,
    DialectName,
    SRKwargsPrefix,
    ColumnAggInfoKey,
    ColumnAggInfoKeyWithPrefix,
    TableInfoKey,
)

from starrocks.reflection import StarRocksTableDefinitionParser
from starrocks.reflection_info import ReflectedViewState, ReflectedDistributionInfo, ReflectedPartitionInfo
from starrocks.sql.schema import MaterializedView, View

from starrocks.alembic.ops import (
    AlterViewOp,
    CreateViewOp,
    DropViewOp,
    CreateMaterializedViewOp,
    DropMaterializedViewOp,
)
from starrocks.utils import CaseInsensitiveDict, TableAttributeNormalizer

logger = logging.getLogger(__name__)


def comparators_dispatch_for_starrocks(dispatch_type: str):
    """
    StarRocks-specific dispatch decorator.
    
    Automatically handles dialect checking, only executes the decorated function under StarRocks dialect.
    
    Args:
        dispatch_type: Alembic dispatch type ("table", "column", "view", etc.)
    
    Usage:
        @starrocks_dispatch_for("table")
        def compare_starrocks_table(autogen_context, conn_table, metadata_table):
            # No need to manually check dialect, decorator handles it automatically
            pass
    """
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            autogen_context = args[0]  # First arg is always autogen_context
            
            # Only execute for StarRocks dialect
            if autogen_context.dialect.name != DialectName:
                # Return default value based on return type annotation
                return_type = func.__annotations__.get('return')
                if return_type is not None:
                    if hasattr(return_type, '__origin__') and return_type.__origin__ is list:
                        return []
                    elif 'List' in str(return_type):
                        return []
                return None
            
            # StarRocks dialect, execute actual logic
            return func(*args, **kwargs)
        
        # Register to Alembic dispatch system
        return comparators.dispatch_for(dispatch_type)(wrapper)
    
    return decorator


# ==============================================================================
# View Comparison
# ==============================================================================
@comparators_dispatch_for_starrocks("schema")
def autogen_for_views(
    autogen_context: AutogenContext, upgrade_ops: UpgradeOps, schemas: List[Optional[str]]
) -> None:
    """
    Main autogenerate entrypoint for views.
    
    Scan views in database and compare with metadata.
    """
    inspector: Inspector = autogen_context.inspector

    conn_views: Set[Tuple[Optional[str], str]] = set()
    for schema in schemas:
        conn_views.update((schema, name) for name in inspector.get_view_names(schema=schema))

    metadata_views_info = autogen_context.metadata.info.get("views", {})
    metadata_views: Dict[Tuple[Optional[str], str], View] = {
        (view_obj.schema or schema, view_obj.name): view_obj
        for key, view_obj in metadata_views_info.items()
        for schema in schemas
    }

    logger.debug(f"_compare_views: conn_views: ({conn_views}), metadata_views: ({metadata_views})")
    _compare_views(conn_views, metadata_views, autogen_context, upgrade_ops)


def _compare_views(
    conn_views: Set[Tuple[Optional[str], str]],
    metadata_views: Dict[Tuple[Optional[str], str], View],
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
) -> None:
    """Compare views between the database and the metadata and generate operations."""
    inspector: Inspector = autogen_context.inspector

    # Find new views to create
    for schema, view_name in sorted(metadata_views.keys() - conn_views):
        view: View = metadata_views[(schema, view_name)]
        upgrade_ops.ops.append(
            CreateViewOp(
                view.name,
                view.definition,
                schema=schema,
                security=view.security,
                comment=view.comment,
            )
        )

    # Find old views to drop
    for schema, view_name in sorted(conn_views - metadata_views.keys()):
        view_info: Optional[ReflectedViewState] = inspector.get_view(view_name, schema=schema)
        if not view_info:
            continue
        upgrade_ops.ops.append(
            DropViewOp(
                view_name,
                schema=schema,
                _reverse_view_definition=view_info.definition,
                _reverse_view_comment=view_info.comment,
                _reverse_view_security=view_info.security,
            )
        )

    # Find views that exist in both and compare their definitions
    for schema, view_name in sorted(conn_views.intersection(metadata_views.keys())):
        view_info: Optional[ReflectedViewState] = inspector.get_view(view_name, schema=schema)
        if not view_info:
            continue

        conn_view = View(
            name=view_info.name,
            definition=view_info.definition,
            schema=schema,
            comment=view_info.comment,
            security=view_info.security
        )
        metadata_view: View = metadata_views[(schema, view_name)]

        logger.debug(
            "Comparing view %s.%s: conn(def)=%r meta(def)=%r",
            schema or autogen_context.dialect.default_schema_name,
            view_name,
            TableAttributeNormalizer.normalize_sql(conn_view.definition),
            TableAttributeNormalizer.normalize_sql(metadata_view.definition),
        )

        comparators.dispatch("view")(
            autogen_context,
            upgrade_ops,
            schema,
            view_name,
            conn_view,
            metadata_view,
        )


@comparators_dispatch_for_starrocks("view")
def compare_view(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    view_name: str,
    conn_view: View,
    metadata_view: View,
) -> None:
    """
    Compare a single view and generate operations if needed.
    
    Check for changes in view definition, comment and security attributes.
    """
    # currently, conn_view or metadata_view is not None.
    if conn_view is None or metadata_view is None:
        logger.warning(f"both conn_view and meta_view should not be None for compare_view: {schema}.{view_name}, "
                       f"skipping. conn_view: {'not None' if conn_view else 'None'}, "
                       f"meta_view: {'not None' if metadata_view else 'None'}")
        return
    
    conn_def_norm: Optional[str] = TableAttributeNormalizer.normalize_sql(conn_view.definition)
    metadata_def_norm: Optional[str] = TableAttributeNormalizer.normalize_sql(metadata_view.definition)
    definition_changed = conn_def_norm != metadata_def_norm
    # Comment/security normalized for comparison
    conn_view_comment = (conn_view.comment or "").strip()
    metadata_view_comment = (metadata_view.comment or "").strip()
    comment_changed = conn_view_comment != metadata_view_comment
    conn_view_security = (conn_view.security or "").upper()
    metadata_view_security = (metadata_view.security or "").upper()
    security_changed = conn_view_security != metadata_view_security

    logger.debug(
        "compare_view: %s.%s def_changed=%s comment_changed=%s security_changed=%s",
        schema or autogen_context.dialect.default_schema_name,
        view_name,
        definition_changed,
        comment_changed,
        security_changed,
    )

    if comment_changed:
        logger.warning(
            "StarRocks does not support altering view comments via ALTER VIEW; "
            "comment change detected for %s.%s and will be ignored",
            schema or autogen_context.dialect.default_schema_name,
            view_name,
        )

    if security_changed:
        logger.warning(
            "StarRocks does not support altering view security via ALTER VIEW; "
            "security change detected for %s.%s and will be ignored",
            schema or autogen_context.dialect.default_schema_name,
            view_name,
        )

    if definition_changed:
        upgrade_ops.ops.append(
            AlterViewOp(
                metadata_view.name,
                metadata_view.definition,
                schema=schema,
                reverse_view_definition=conn_view.definition,
            )
        )
        logger.debug(
            "Generated AlterViewOp for %s.%s",
            schema or autogen_context.dialect.default_schema_name,
            view_name,
        )
    # else: only comment/security changed -> no operation generated


# ==============================================================================
# Materialized View Comparison
# ==============================================================================
@comparators_dispatch_for_starrocks("schema")
def autogen_for_materialized_views(
    autogen_context: AutogenContext, upgrade_ops: UpgradeOps, schemas: List[Optional[str]]
) -> None:
    """
    Main autogenerate entrypoint for materialized views.
    
    Scan materialized views in database and compare with metadata.
    """
    inspector: Inspector = autogen_context.inspector

    conn_mvs: Set[Tuple[Optional[str], str]] = set()
    for schema in schemas:
        conn_mvs.update(
            (schema, name)
            for name in inspector.get_materialized_view_names(schema=schema)
        )

    metadata_mvs_info = autogen_context.metadata.info.get("materialized_views", {})
    metadata_mvs: Dict[Tuple[Optional[str], str], MaterializedView] = {
        (
            mv_obj.schema or autogen_context.dialect.default_schema_name,
            mv_obj.name,
        ): mv_obj
        for key, mv_obj in metadata_mvs_info.items()
    }
    _compare_materialized_views(conn_mvs, metadata_mvs, autogen_context, upgrade_ops)


def _compare_materialized_views(
    conn_mvs: Set[Tuple[Optional[str], str]],
    metadata_mvs: Dict[Tuple[Optional[str], str], MaterializedView],
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
) -> None:
    """Compare MVs between the database and the metadata and generate operations."""
    inspector: Inspector = autogen_context.inspector

    # Find new MVs to create
    for schema, mv_name in sorted(metadata_mvs.keys() - conn_mvs):
        mv: MaterializedView = metadata_mvs[(schema, mv_name)]
        upgrade_ops.ops.append(
            CreateMaterializedViewOp(mv.name, mv.definition, schema=schema)
        )

    # Find old MVs to drop
    for schema, mv_name in sorted(conn_mvs - metadata_mvs.keys()):
        upgrade_ops.ops.append(DropMaterializedViewOp(mv_name, schema=schema))

    # Find modified MVs
    for schema, mv_name in sorted(conn_mvs.intersection(metadata_mvs.keys())):
        view_info: Optional[str] = inspector.get_materialized_view_definition(mv_name, schema=schema)
        conn_mv = MaterializedView(
            mv_name,
            view_info,
            schema=schema
        )
        metadata_mv: MaterializedView = metadata_mvs[(schema, mv_name)]

        comparators.dispatch("materialized_view")(
            autogen_context,
            upgrade_ops,
            schema,
            mv_name,
            conn_mv,
            metadata_mv,
        )


@comparators_dispatch_for_starrocks("materialized_view")
def compare_mv(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    mv_name: str,
    conn_mv: MaterializedView,
    metadata_mv: MaterializedView,
) -> None:
    """
    Compare a single materialized view and generate operations if needed.
    
    Check for changes in materialized view definition.
    """
    if conn_mv.definition != metadata_mv.definition:
        upgrade_ops.ops.append(
            (
                DropMaterializedViewOp(mv_name, schema=schema),
                CreateMaterializedViewOp(
                    metadata_mv.name, metadata_mv.definition, schema=schema
                ),
            )
        )


# ==============================================================================
# Table Comparison
# Only starrocks-specific table attributes are compared.
# Other table attributes are compared using generic comparison logic in Alembic.
# ==============================================================================
@comparators_dispatch_for_starrocks("table")
def compare_starrocks_table(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    table_name: str,
    conn_table: Optional[Table],
    metadata_table: Optional[Table],
) -> None:
    """
    Compare StarRocks-specific table attributes and generate operations.

    Other table attributes are compared using generic comparison logic in Alembic.
    For some starrocks-specific attributes of columns, see compare_starrocks_column.

    Args:
        autogen_context: AutogenContext
        conn_table: Table object in the database, already reflected from the database
        metadata_table: Table object in the metadata

    Raises:
        NotImplementedError: If a change is detected that is not supported in StarRocks.
    """
    # Handle table creation and deletion scenarios
    if conn_table is None:
        # Table exists in metadata but not in DB; this is a CREATE TABLE.
        # Alembic handles CreateTableOp separately. Our comparator should do nothing.
        logger.debug(f"compare_starrocks_table: conn_table is None for '{metadata_table.name}', skipping.")
        return
    if metadata_table is None:
        # Table exists in DB but not in metadata; this is a DROP TABLE.
        # Alembic handles DropTableOp separately. Our comparator should do nothing.
        logger.debug(f"compare_starrocks_table: metadata_table is None for '{conn_table.name}', skipping.")
        return

    # logger.debug(f"compare_starrocks_table: conn_table: {conn_table!r}, metadata_table: {metadata_table!r}")
    # Get the system run_mode for proper default value comparison
    run_mode = autogen_context.dialect.run_mode
    logger.info(f"compare starrocks table. table: {table_name}, schema:{schema}, run_mode: {run_mode}")
    
    conn_table_attributes = CaseInsensitiveDict({k: v for k, v in conn_table.dialect_options[DialectName].items() if v is not None})
    meta_table_attributes = CaseInsensitiveDict({k: v for k, v in metadata_table.dialect_options[DialectName].items() if v is not None})    

    logger.debug(
        "StarRocks-specific attributes comparison for table '%s': "
        "Detected in database: %s. Found in metadata: %s.",
        conn_table.name,
        conn_table_attributes,
        meta_table_attributes,
    )

    # Note: Table comment comparison is handled by Alembic's built-in _compare_table_comment

    # Compare each type of table attribute using dedicated functions
    # Order follows StarRocks CREATE TABLE grammar:
    #   engine -> key -> comment -> partition -> distribution -> order by -> properties
    table, schema = conn_table.name, conn_table.schema
    _compare_table_engine(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_key(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    # Note: COMMENT comparison is handled by Alembic's built-in _compare_table_comment
    _compare_table_partition(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_distribution(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_order_by(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_properties(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes, run_mode)

    return False  # Return False to indicate we didn't add any ops to the list directly


def _compare_table_engine(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any]
) -> None:
    """Compare engine changes and add AlterTableEngineOp if needed.

    Note: StarRocks does not support ALTER TABLE ENGINE, so this will raise an error
    if a change is detected.
    """
    meta_engine = meta_table_attributes.get(TableInfoKey.ENGINE)
    conn_engine = conn_table_attributes.get(TableInfoKey.ENGINE)
    logger.debug(f"ENGINE. meta_engine: {meta_engine}, conn_engine: {conn_engine}")

    # if not meta_engine:
    #     logger.error(f"Engine info should be specified in metadata to change for table {table_name} in schema {schema}.")
    #     return

    normalized_meta: Optional[str] = TableAttributeNormalizer.normalize_engine(meta_engine)
    # Reflected table must have a default ENGINE, so we need to normalize it
    normalized_conn: Optional[str] = ReflectionTableDefaults.normalize_engine(conn_engine)

    _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.ENGINE,
        normalized_conn,
        normalized_meta,
        default_value=ReflectionTableDefaults.engine(),
        support_change=AlterTableEnablement.ENGINE
    )


def _compare_table_key(
    ops_list: List[AlterTableOp], schema: Optional[str], table_name: str,
    conn_table_attributes: Dict[str, Any], meta_table_attributes: Dict[str, Any]
) -> None:
    """Compare key changes and add AlterTableKeyOp if needed.
    
    Note: StarRocks does not support ALTER TABLE KEY, so this will raise an error
    if a change is detected.
    """
    conn_key: Optional[str] = _get_table_key_type(conn_table_attributes)
    meta_key: Optional[str] = _get_table_key_type(meta_table_attributes)
    logger.debug(f"KEY. conn_key: {conn_key}, meta_key: {meta_key}")

    # if not meta_key:
    #     logger.error(f"Key info should be specified in metadata to change for table {table_name} in schema {schema}.")
    #     return

    # Reflected table must have a default KEY, so we need to normalize it
    # Actually, the conn key must not be None, because it is inspected from database.
    normalized_conn: Optional[str] = ReflectionTableDefaults.normalize_key(conn_key)
    normalized_meta: Optional[str] = TableAttributeNormalizer.normalize_key(meta_key)

    _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.KEY,
        normalized_conn,
        normalized_meta,
        default_value=ReflectionTableDefaults.key(),
        equal_to_default_cmp_func=_compare_key_with_defaults,
        support_change=AlterTableEnablement.KEY
    )

def _get_table_key_type(table_attributes: Dict[str, Any]) -> Optional[str]:
    """Get table key type. like 'PRIMARY KEY (id, name)'"""
    for key_type in TableInfoKey.KEY_KWARG_MAP:
        key_columns = table_attributes.get(key_type)
        if key_columns:
            key_columns = TableAttributeNormalizer.remove_outer_parentheses(key_columns)
            return f"{TableInfoKey.KEY_KWARG_MAP[key_type]} ({key_columns})"
    return None


def _compare_key_with_defaults(
    conn_value: Optional[str], default_value: Optional[str]
) -> bool:
    """
    Compare key / table type, considering that the reflected default might be more specific.

    For example, a default of 'DUPLICATE KEY' should match a connection value of
    'DUPLICATE KEY(id, dt)'.

    Args:
        conn_value: The key attribute value reflected from the database.
        default_value: The known default value for the key attribute.

    Returns:
        True if the connection value is considered equal to the default (e.g., it starts
        with the default), False otherwise.
    """
    if conn_value is None:
        return default_value is None
    if default_value is None:
        return conn_value is None

    # Normalize by converting to uppercase and removing extra spaces
    conn_norm = conn_value.upper()
    default_norm = default_value.upper()

    # Check if conn_value starts with the default_value, ignoring case and spaces
    return conn_norm.startswith(default_norm)


def _compare_partition_method(
    conn_partition: Optional[ReflectedPartitionInfo | str],
    default_partition: Optional[str]
) -> bool:
    """
    Compare two ReflectedPartitionInfo objects for equality.

    This comparison deliberately ignores pre-created partition info (e.g., `VALUES
    LESS THAN (...)`) and only compares the partitioning scheme itself (type and
    the column list or expression list).

    Args:
        conn_partition: The partition info reflected from the database.
        meta_partition: The partition info from the target metadata.

    Returns:
        True if the partitioning schemes are considered equal, False otherwise.
    """
    if conn_partition is None:
        return default_partition is None
    if default_partition is None:
        return conn_partition is None

    # If the partition info is a string, it's the partition_by expression, not a ReflectedPartitionInfo object
    if isinstance(conn_partition, ReflectedPartitionInfo):
        conn_partition = conn_partition.partition_method
        
    # Only compare the partition_method.
    return conn_partition == default_partition


def _compare_table_partition(
    ops_list: List[AlterTableOp], schema: Optional[str], table_name: str,
    conn_table_attributes: Dict[str, Any], meta_table_attributes: Dict[str, Any]
) -> None:
    """Compare partition changes and add AlterTablePartitionOp if needed."""
    conn_partition = conn_table_attributes.get(TableInfoKey.PARTITION_BY)
    meta_partition = meta_table_attributes.get(TableInfoKey.PARTITION_BY)
    logger.debug(f"PARTITION_BY. conn_partition: {conn_partition}, meta_partition: {meta_partition}")

    # if not meta_partition:
    #     logger.error(f"Partition info should be specified in metadata for table {table_name} in schema {schema}.")
    #     return

    # Parse the partition info if it's a string
    if isinstance(conn_partition, str):
        conn_partition = StarRocksTableDefinitionParser.parse_partition_clause(conn_partition)
    if isinstance(meta_partition, str):
        meta_partition = StarRocksTableDefinitionParser.parse_partition_clause(meta_partition)

    # Normalize the partition method, such as 'RANGE(dt)', 'LIST(dt, col2)', which is used to be compared.
    normalized_conn: Optional[str] = TableAttributeNormalizer.normalize_partition_method(conn_partition)
    normalized_meta: str = TableAttributeNormalizer.normalize_partition_method(meta_partition)

    if _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.PARTITION_BY, 
        normalized_conn, 
        normalized_meta, 
        default_value=ReflectionTableDefaults.partition_by(),
        support_change=AlterTableEnablement.PARTITION_BY,
        equal_to_default_cmp_func=_compare_partition_method
    ):
        from starrocks.alembic.ops import AlterTablePartitionOp
        ops_list.append(
            AlterTablePartitionOp(
                table_name,
                meta_partition.partition_method,
                schema=schema,
            )
        )


def _compare_table_distribution(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any],
) -> None:
    """Compare distribution changes and add AlterTableDistributionOp if needed."""
    conn_distribution = conn_table_attributes.get(TableInfoKey.DISTRIBUTED_BY)
    meta_distribution = meta_table_attributes.get(TableInfoKey.DISTRIBUTED_BY)
    if isinstance(conn_distribution, str):
        conn_distribution = StarRocksTableDefinitionParser.parse_distribution(conn_distribution)
    if isinstance(meta_distribution, str):
        meta_distribution = StarRocksTableDefinitionParser.parse_distribution(meta_distribution)

    # Normalize both strings for comparison (handles backticks)
    normalized_conn: Optional[str] = TableAttributeNormalizer.normalize_distribution_string(conn_distribution)
    normalized_meta: Optional[str] = TableAttributeNormalizer.normalize_distribution_string(meta_distribution)
    logger.debug(f"DISTRIBUTED_BY. normalized_conn: {normalized_conn}, normalized_meta: {normalized_meta}")

    # Use generic comparison logic with default distribution
    if _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.DISTRIBUTED_BY,
        normalized_conn,
        normalized_meta,
        default_value=ReflectionTableDefaults.distribution_type(),
        support_change=AlterTableEnablement.DISTRIBUTED_BY
    ):
        from starrocks.alembic.ops import AlterTableDistributionOp

        ops_list.append(
            AlterTableDistributionOp(
                table_name,
                meta_distribution.distribution_method,
                meta_distribution.buckets,
                schema=schema,
                reverse_distribution_method=conn_distribution.distribution_method if conn_distribution else None,
                reverse_buckets=conn_distribution.buckets if conn_distribution else None,
            )
        )


def _compare_table_order_by(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any],
) -> None:
    """Compare ORDER BY changes and add AlterTableOrderOp if needed."""
    conn_order = conn_table_attributes.get(TableInfoKey.ORDER_BY)
    meta_order = meta_table_attributes.get(TableInfoKey.ORDER_BY)

    # Normalize both for comparison (handles backticks and list vs string)
    normalized_conn: Optional[str] = TableAttributeNormalizer.normalize_order_by_string(conn_order) if conn_order else None
    normalized_meta: Optional[str] = TableAttributeNormalizer.normalize_order_by_string(meta_order) if meta_order else None
    logger.debug(f"ORDERY BY. normalized_conn: {normalized_conn}, normalized_meta: {normalized_meta}")

    # if ORDER BY is not set, we directly recoginize it as no change
    if not normalized_meta:
        return

    # Use generic comparison logic with default ORDER BY
    if _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.ORDER_BY,
        normalized_conn,
        normalized_meta,
        default_value=ReflectionTableDefaults.order_by(),
        support_change=AlterTableEnablement.ORDER_BY
    ):
        from starrocks.alembic.ops import AlterTableOrderOp
        ops_list.append(
            AlterTableOrderOp(
                table_name,
                meta_order,  # Use original format
                schema=schema,
                reverse_order_by=conn_order if conn_order else None,
            )
        )


def _compare_table_properties(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table_attributes: Dict[str, Any],
    meta_table_attributes: Dict[str, Any],
    run_mode: str
) -> None:
    """Compare properties changes and add AlterTablePropertiesOp if needed.

    - If a property is specified in metadata, it is compared with the database.
    - If a property is NOT specified in metadata but exists in the database with a NON-DEFAULT value,
      a change is detected.
    - The generated operation will set all properties defined in the metadata, effectively resetting
      any omitted, non-default properties back to their defaults in StarRocks.
    """
    conn_properties: dict[str, str] = conn_table_attributes.get(TableInfoKey.PROPERTIES, {})
    meta_properties: dict[str, str] = meta_table_attributes.get(TableInfoKey.PROPERTIES, {})
    logger.debug(f"PROPERTIES. conn_properties: {conn_properties}, meta_properties: {meta_properties}")

    normalized_conn = CaseInsensitiveDict(conn_properties)
    normalized_meta = CaseInsensitiveDict(meta_properties)
    # logger.debug(f"PROPERTIES. normalized_conn: {normalized_conn}, normalized_meta: {normalized_meta}")

    if normalized_meta == normalized_conn:
        return

    # Check for meaningful changes, ignoring defaults
    has_meaningful_change = False
    all_keys = set(normalized_meta.keys()) | set(normalized_conn.keys())
    for key in all_keys:
        conn_value = normalized_conn.get(key)
        meta_value = normalized_meta.get(key)
        default_value = ReflectionTableDefaults.properties(run_mode).get(key)

        # Convert all to strings for comparison to avoid type issues (e.g., int vs str)
        conn_str = str(conn_value) if conn_value is not None else None
        meta_str = str(meta_value) if meta_value is not None else None
        default_str = str(default_value) if default_value is not None else None

        if meta_str is not None:
            # Case 1 & 2: meta specifies a value.
            # Change if it differs from conn, or if conn is None.
            if meta_str != conn_str:
                has_meaningful_change = True
        else:
            # Case 3 & 4: meta does NOT specify a value.
            # Change ONLY if conn has a NON-DEFAULT value.
            if conn_str is not None and conn_str != default_str:
                full_table_name = f"{schema}.{table_name}" if schema else table_name or "unknown_table"
                # If metadata doesn't specify a property that exists in DB with a non-default value,
                # implicitly add it to meta_properties with its default value for the ALTER operation.
                if default_value is not None:
                    has_meaningful_change = True
                    normalized_meta[key] = default_value
                    logger.warning(
                        f"Table '{full_table_name}': Property '{key}' in database has non-default value '{conn_str}' "
                        f"(default: '{default_str}'), but is not specified in metadata. "
                        f"An ALTER TABLE SET operation will be generated to effectively reset it to its default. "
                        f"Consider explicitly setting default properties in your metadata to avoid ambiguity."
                    )
                else:
                    # If there's no explicit default, and meta doesn't specify, it means removal.
                    # The current meta_properties already reflects this omission.
                    # Add a warning for this case where no default is known.
                    logger.info(
                        f"Table '{full_table_name}': Property '{key}' in database has non-default value '{conn_str}', "
                        f"but no default is defined in ReflectionTableDefaults and it's not specified in metadata. "
                        f"No ALTER TABLE SET operation will be generated automatically. "
                        f"Please specify this property explicitly in your table definition if you want to manage it."
                    )
                    pass  # No change needed to meta_properties, its absence implies removal.

    
    if has_meaningful_change:
        from starrocks.alembic.ops import AlterTablePropertiesOp
        ops_list.append(
            AlterTablePropertiesOp(
                table_name,
                normalized_meta,
                schema=schema,
                reverse_properties=normalized_conn,
            )
        )


def _compare_single_table_attribute(
        table_name: Optional[str],
        schema: Optional[str],
        attribute_name: str,
        conn_value: Optional[str],
        meta_value: Optional[str],
        default_value: Optional[str] = None,
        equal_to_default_cmp_func: Optional[Callable[[Any, Any], bool]] = None,
        support_change: bool = True
) -> bool:
    """
    Generic comparison logic for a single table attribute.

    Args:
        table_name: Table name for logging context.
        schema: Schema name for logging context.
        attribute_name: Name of the attribute for logging.
        conn_value: Value reflected from database (None if not present).
        meta_value: Value specified in metadata (None if not specified).
        default_value: Known default value for this attribute (None if no default).
        support_change: Whether this attribute supports ALTER operations.
        equal_to_default_cmp_func: An optional function to perform a custom comparison
            between the connection value and the default value. If provided, this is
            used when `meta_value` is None.

    Returns:
        True if there's a meaningful change that requires an ALTER statement.

    Raises:
        NotImplementedError: If support_change=False and a change is detected

    Logic:
        1. If meta specifies value != (conn value or default value) -> change needed
        2. If meta specifies value == (conn value or default value) -> no change
        3. If meta not specified and (conn is None or conn == default) -> no change
        4. If meta not specified and conn != default -> log error, return False (user must decide)
    """
    # Convert values to strings for comparison (handle None gracefully)
    conn_str = str(conn_value) if conn_value is not None else None
    meta_str = str(meta_value) if meta_value is not None else None
    default_str = str(default_value) if default_value is not None else None

    full_table_name = f"{schema}.{table_name}" if schema else table_name or "unknown_table"
    attribute_name: str = attribute_name.upper().replace('_', ' ')

    if meta_str is not None:
        # Case 1 & 2: meta_table specifies this attribute
        if meta_str != (conn_str or default_str):
            # Case 1: meta specified, different from conn -> has change
            logger.debug(
                f"Table '{full_table_name}', Attribute '{attribute_name}' "
                f"has changed from '{conn_str or '(not set)'}' to '{meta_str}' "
                f"with default value '{default_str}'")
            if meta_value.lower() == (conn_str or default_str or '').lower():
                logger.warning(
                    f"Table '{full_table_name}': Attribute '{attribute_name}' has a case-only difference: "
                    f"'{conn_value}' (database) vs '{meta_value}' (metadata). "
                    f"Consider making them consistent for clarity."
                    f"No ALTER statement will be generated automatically."
                )
                return False
            if not support_change:
                # This attribute doesn't support ALTER operations
                error_msg = (
                    f"StarRocks does not support 'ALTER TABLE {attribute_name}'. "
                    f"Table '{full_table_name}' has {attribute_name.upper()} '{conn_str or '(not set)'}' in database "
                    f"but '{meta_str}' in metadata. "
                    f"Please update your metadata to match the database."
                )
                logger.error(error_msg)
                raise NotImplementedError(error_msg)
            return True
        # Case 2: meta specified, same as conn -> no change
        return False
    else:
        # Case 3.1: both conn and meta are None
        if conn_str is None:
            return False
        # Case 3 & 4: meta_table does NOT specify this attribute
        if conn_str != default_str:
            # If custom comparison function is provided, use it for default comparison
            if equal_to_default_cmp_func and equal_to_default_cmp_func(conn_value, default_value):
                logger.debug(
                    f"Table '{full_table_name}': Attribute '{attribute_name}' in database is considered "
                    f"equal to default '{default_str}' via custom comparison function."
                )
                return False

            # Case 4: meta not specified, conn is non-default -> log error, NO automatic change
            if conn_str.lower() == (default_str or '').lower():
                logger.warning(
                    f"Table '{full_table_name}': Attribute '{attribute_name}' has a case-only difference: "
                    f"'{conn_str}' (database) vs '{default_str}' (default). "
                    f"Consider making them consistent for clarity. "
                    f"No ALTER statement will be generated automatically."
                )
                pass
            else:
                error_msg = (
                    f"Table '{full_table_name}': Attribute '{attribute_name}' "
                    f"in database has non-default value '{conn_str}' (default: '{default_str}'), "
                    f"but not specified in metadata. Please specify this attribute explicitly "
                    "in your table definition to avoid unexpected behavior. "
                    "No ALTER statement will be generated automatically."
                )
                logger.error(error_msg)
                raise NotImplementedError(error_msg)  # Don't generate ALTER - user must decide explicitly
        # Case 3: meta not specified, conn is default (or no default defined) -> no change
        return False

def extract_starrocks_dialect_attributes(kwargs: Dict[str, Any]) -> CaseInsensitiveDict:
    """Extract StarRocks-specific dialect attributes from a dict, with each attribute prefixed with 'starrocks_'.

    Returns a CaseInsensitiveDict for case-insensitive key access, with prefix 'starrocks_' removed.

    Currently, it's useless, because we use Table.dialect_options[dialect] to get it.
    """
    result = CaseInsensitiveDict()
    if not kwargs:
        return result
    for k, v in kwargs.items():
        if k.lower().startswith(SRKwargsPrefix):
            result[k[len(SRKwargsPrefix):]] = v
    return result


@comparators_dispatch_for_starrocks("column")
def compare_starrocks_column_agg_type(
    autogen_context: AutogenContext,
    alter_column_op: AlterColumnOp,
    schema: Optional[str],
    tname: Union[quoted_name, str],
    cname: Union[quoted_name, str],
    conn_col: Column[Any],
    metadata_col: Column[Any],
) -> None:
    """
    Compare StarRocks-specific column options.
    
    Check for changes in StarRocks-specific attributes like aggregate type.
    """
    conn_opts = CaseInsensitiveDict(
        {k: v for k, v in conn_col.dialect_options[DialectName].items() if v is not None}
    )
    meta_opts = CaseInsensitiveDict(
        {k: v for k, v in metadata_col.dialect_options[DialectName].items() if v is not None}
    )
    conn_agg_type: str | None = conn_opts.get(ColumnAggInfoKey.AGG_TYPE)
    meta_agg_type: str | None = meta_opts.get(ColumnAggInfoKey.AGG_TYPE)
    logger.debug(f"AGG_TYPE. conn_agg_type: {conn_agg_type}, meta_agg_type: {meta_agg_type}")

    if meta_agg_type != conn_agg_type:
        # Update the alter_column_op with the new aggregate type. useless now
        # "KEY", "SUM" for set, None for unsert
        if alter_column_op is not None:
            alter_column_op.kwargs[ColumnAggInfoKeyWithPrefix.AGG_TYPE] = meta_agg_type
        raise NotSupportedError(
            f"StarRocks does not support changing the aggregation type of a column: '{cname}', "
            f"from {conn_agg_type} to {meta_agg_type}.",
            None, None
        )


@comparators_dispatch_for_starrocks("column")
def compare_starrocks_column_autoincrement(
    autogen_context: AutogenContext,
    alter_column_op: AlterColumnOp,
    schema: Optional[str],
    tname: Union[quoted_name, str],
    cname: quoted_name,
    conn_col: Column[Any],
    metadata_col: Column[Any],
) -> None:
    """
    Compare StarRocks-specific column options.
    It will run after the  built-in comparator for "column" auto_increment.

    StarRocks does not support changing the autoincrement of a column.
    """
    # Because we can't inpsect the autoincrement, we can't do the check the difference.
    if conn_col.autoincrement != metadata_col.autoincrement:
        logger.warning(
            f"Detected AUTO_INCREMENT is changed for column {cname}. "
            f"conn_col.autoincrement: {conn_col.autoincrement}, "
            f"metadata_col.autoincrement: {metadata_col.autoincrement}. "
            f"No ALTER statement will be generated automatically, "
            f"Because we can't inpsect the column's autoincrement currently."
        )
    return None

