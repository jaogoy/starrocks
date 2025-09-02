import re
import logging
from functools import wraps
from typing import Any, Dict, List, Optional, Set, Tuple, Union

from alembic.autogenerate import comparators
from alembic.autogenerate.api import AutogenContext

from alembic.operations.ops import AlterColumnOp, AlterTableOp, UpgradeOps
from sqlalchemy import Column, quoted_name
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.schema import Table

from starrocks.defaults import TableReflectionDefaults
from starrocks.params import (
    DialectName,
    SRKwargsPrefix,
    ColumnAggInfoKey,
    ColumnAggInfoKeyWithPrefix,
    TableInfoKey,
)

from starrocks.reflection import ReflectionViewInfo
from starrocks.sql.schema import MaterializedView, View

from starrocks.alembic.ops import (
    AlterViewOp,
    CreateViewOp,
    DropViewOp,
    CreateMaterializedViewOp,
    DropMaterializedViewOp,
)
from starrocks.utils import CaseInsensitiveDict

logger = logging.getLogger("starrocks.alembic.compare")


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


def _strip_identifier_backticks(sql_text: str) -> str:
    """Remove MySQL-style identifier quotes (`) while preserving string literals."""
    in_single_quote = False
    escaped = False
    out: list[str] = []
    for ch in sql_text:
        if in_single_quote:
            out.append(ch)
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == "'":
                in_single_quote = False
            continue
        if ch == "'":
            in_single_quote = True
            out.append(ch)
        elif ch == "`":
            # drop identifier quote
            continue
        else:
            out.append(ch)
    return "".join(out)


def normalize_sql(sql_text: Optional[str]) -> Optional[str]:
    """A normalizer for SQL text for diffing.

    - Strips single-line comments
    - Removes identifier backticks outside string literals
    - Collapses whitespace
    - Lowercases
    """
    if sql_text is None:
        return None
    # Remove comments
    sql_text = re.sub(r"--.*?(?:\n|$)", " ", sql_text)
    # Remove identifier quotes but keep quotes inside strings
    sql_text = _strip_identifier_backticks(sql_text)
    # Collapse whitespace and convert to lowercase
    sql_text = re.sub(r"\s+", " ", sql_text).strip().lower()
    return sql_text


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
        view_info: Optional[ReflectionViewInfo] = inspector.get_view(view_name, schema=schema)
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
        view_info: Optional[ReflectionViewInfo] = inspector.get_view(view_name, schema=schema)
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
            normalize_sql(conn_view.definition),
            normalize_sql(metadata_view.definition),
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
    conn_def_norm: Optional[str] = normalize_sql(conn_view.definition)
    metadata_def_norm: Optional[str] = normalize_sql(metadata_view.definition)
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
        autogen_context: AutogenContext, conn_table: Table, metadata_table: Table
) -> List[AlterTableOp]:
    """
    Compare StarRocks-specific table attributes and generate operations.

    Other table attributes are compared using generic comparison logic in Alembic.
    For some starrocks-specific attributes of columns, see compare_starrocks_column.

    Args:
        autogen_context: AutogenContext
        conn_table: Table object in the database, already reflected from the database
        metadata_table: Table object in the metadata

    Returns:
        List of AlterTableOp

    Raises:
        NotImplementedError: If a change is detected that is not supported in StarRocks.
    """
    conn_table_attributes = _extract_starrocks_dialect_attributes(conn_table.kwargs)
    meta_table_attributes = _extract_starrocks_dialect_attributes(metadata_table.kwargs)

    ops_list = []

    # Table info for passing to comparison functions
    table_info = {
        'name': conn_table.name,
        'schema': conn_table.schema,
    }

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

    _compare_engine(conn_table_attributes, meta_table_attributes, ops_list, table_info)
    # Note: KEY comparison not implemented yet (would be _compare_key)
    # Note: COMMENT comparison is handled by Alembic's built-in _compare_table_comment
    _compare_partition(conn_table_attributes, meta_table_attributes, ops_list, table_info)
    _compare_distribution(conn_table_attributes, meta_table_attributes, ops_list, table_info)
    _compare_order_by(conn_table_attributes, meta_table_attributes, ops_list, table_info)
    _compare_properties(conn_table_attributes, meta_table_attributes, ops_list, table_info)

    return ops_list


def _compare_distribution(conn_table_options: Dict[str, Any], meta_table_options: Dict[str, Any], ops_list: List[AlterTableOp], table_info: Dict[str, Any]) -> None:

def _compare_distribution(conn_table_attributes: Dict[str, Any], meta_table_attributes: Dict[str, Any], ops_list: List[AlterTableOp], table_info: Dict[str, Any]) -> None:
    """Compare distribution changes and add AlterTableDistributionOp if needed."""
    meta_distribution = meta_table_attributes.get(TableInfoKey.DISTRIBUTED_BY)
    conn_distribution = conn_table_attributes.get(TableInfoKey.DISTRIBUTED_BY)

    # Normalize both strings for comparison (handles backticks)
    normalized_meta = _normalize_distribution_string(meta_distribution) if meta_distribution else None
    normalized_conn = _normalize_distribution_string(conn_distribution) if conn_distribution else None
    logger.debug(f"DISTRIBUTED_BY. normalized_meta: {normalized_meta}, normalized_conn: {normalized_conn}")

    # Use generic comparison logic (no default value for distribution)
    if _compare_single_table_attribute(
        table_info['name'],
        table_info['schema'],
        TableInfoKey.DISTRIBUTED_BY, 
        normalized_meta, 
        normalized_conn, 
        default_value=None  # No standard default for distribution
    ):
        from starrocks.alembic.ops import AlterTableDistributionOp

        # Parse distribution and buckets from original (non-normalized) string
        distributed_by, buckets = _parse_distribution_string(meta_distribution)

        ops_list.append(
            AlterTableDistributionOp(
                table_info['name'],
                distributed_by,
                buckets=buckets,
                schema=table_info['schema'],
            )
        )


def _compare_order_by(conn_table_attributes: Dict[str, Any], meta_table_attributes: Dict[str, Any], ops_list: List[AlterTableOp], table_info: Dict[str, Any]) -> None:
    """Compare ORDER BY changes and add AlterTableOrderOp if needed."""
    meta_order = meta_table_attributes.get(TableInfoKey.ORDER_BY)
    conn_order = conn_table_attributes.get(TableInfoKey.ORDER_BY)

    # Normalize both for comparison (handles backticks and list vs string)
    normalized_meta = _normalize_order_by_string(meta_order) if meta_order else None
    normalized_conn = _normalize_order_by_string(conn_order) if conn_order else None
    logger.debug(f"ORDERY BY. normalized_meta: {normalized_meta}, normalized_conn: {normalized_conn}")
    
    # Use generic comparison logic (no default value for ORDER BY)
    if _compare_single_table_attribute(
        table_info['name'],
        table_info['schema'],
        TableInfoKey.ORDER_BY, 
        normalized_meta, 
        normalized_conn, 
        default_value=None  # No standard default for ORDER BY
    ):
        from starrocks.alembic.ops import AlterTableOrderOp
        ops_list.append(
            AlterTableOrderOp(
                table_info['name'],
                meta_order,  # Use original format
                schema=table_info['schema'],
            )
        )


def _compare_properties(conn_table_attributes: Dict[str, Any], meta_table_attributes: Dict[str, Any],
                        ops_list: List[AlterTableOp], table_info: Dict[str, Any]) -> None:
    """Compare properties changes and add AlterTablePropertiesOp if needed.

    It will generate an AlterTablePropertiesOp if there are any changes in the properties.
    Because change of any property is just a simple set, it won't trigger any data restructure.
    """
    meta_properties = meta_table_attributes.get(TableInfoKey.PROPERTIES, {})
    conn_properties = conn_table_attributes.get(TableInfoKey.PROPERTIES, {})

    # Collect all property keys that might need comparison
    all_property_keys = set(meta_properties.keys()) | set(conn_properties.keys())

    has_meaningful_change = False

    for key in all_property_keys:
        meta_value = meta_properties.get(key)
        conn_value = conn_properties.get(key)
        default_value = TableReflectionDefaults.DEFAULT_PROPERTIES.get(key)

        # Use generic comparison logic
        if _compare_single_table_attribute(
                table_info['name'],
                table_info['schema'],
                f"PROPERTIES.{key}",
                str(meta_value) if meta_value is not None else None,
                str(conn_value) if conn_value is not None else None,
                default_value
        ):
            has_meaningful_change = True
            break

    if has_meaningful_change:
        from starrocks.alembic.ops import AlterTablePropertiesOp
        ops_list.append(
            AlterTablePropertiesOp(
                table_info['name'],
                meta_properties,  # Use user-specified properties
                schema=table_info['schema'],
            )
        )


def _compare_single_table_attribute(
        table_name: Optional[str],
        schema: Optional[str],
        attribute_name: str,
        meta_value: Optional[str],
        conn_value: Optional[str],
        default_value: Optional[str] = None
) -> bool:
    """
    Generic comparison logic for a single table attribute.

    Args:
        table_name: Table name for logging context
        schema: Schema name for logging context
        attribute_name: Name of the attribute for logging
        meta_value: Value specified in metadata (None if not specified)
        conn_value: Value reflected from database (None if not present)
        default_value: Known default value for this attribute (None if no default)

    Returns:
        True if there's a meaningful change that requires ALTER statement

    Logic:
        1. If meta specifies value != conn value -> change needed
        2. If meta specifies value == conn value -> no change
        3. If meta not specified and conn == default -> no change
        4. If meta not specified and conn != default -> log error, return False (user must decide)
    """
    # Convert values to strings for comparison (handle None gracefully)
    meta_str = meta_value if meta_value is not None else None
    conn_str = conn_value if conn_value is not None else None
    default_str = default_value if default_value is not None else None

    if meta_str is not None:
        # Case 1 & 2: meta_table specifies this attribute
        if meta_str != (conn_str or ''):
            # Case 1: meta specified, different from conn -> has change
            return True
        # Case 2: meta specified, same as conn -> no change
        return False

    else:
        # Case 3 & 4: meta_table does NOT specify this attribute
        if default_str is not None and (conn_str or '') != default_str:
            # Case 4: meta not specified, conn is non-default -> log error, NO automatic change
            full_table_name = f"{schema}.{table_name}" if schema else table_name or "unknown_table"
            logger.error(
                "Table '%s': Attribute '%s' in database has non-default value '%s' (default: '%s'), "
                "but not specified in metadata. Please specify this attribute explicitly "
                "in your table definition to avoid unexpected behavior. "
                "No ALTER statement will be generated automatically.",
                full_table_name, attribute_name, conn_str, default_str
            )
            return False  # Don't generate ALTER - user must decide explicitly
        # Case 3: meta not specified, conn is default (or no default defined) -> no change
        return False


def _extract_starrocks_dialect_attributes(kwargs: Dict[str, Any]) -> CaseInsensitiveDict:
    """Extract StarRocks-specific dialect attributes from a dict, with each attribute prefixed with 'starrocks_'.

    Returns a CaseInsensitiveDict for case-insensitive key access, with prefix 'starrocks_' removed.
    """
    result = CaseInsensitiveDict()
    for k, v in kwargs.items():
        if k.lower().startswith(SRKwargsPrefix):
            result[k[len(SRKwargsPrefix):]] = v
    return result


def _parse_distribution_string(distribution: str) -> Tuple[str, Optional[int]]:
    """Parse DISTRIBUTED BY string to extract distribution and buckets.

    Args:
        distribution: String like "HASH(id) BUCKETS 8" or "HASH(id)"

    Returns:
        Tuple of (distributed_by, buckets)
    """
    if not distribution:
        return distribution, None

    # Use regex to extract BUCKETS value
    import re
    buckets_match = re.search(r'\sBUCKETS\s+(\d+)', distribution, re.IGNORECASE)

    if buckets_match:
        buckets = int(buckets_match.group(1))
        # Remove BUCKETS part to get pure distribution
        distributed_by = re.sub(r'\s+BUCKETS\s+\d+', '', distribution, flags=re.IGNORECASE).strip()
        return distributed_by, buckets
    else:
        return distribution, None


def _normalize_distribution_string(distribution: str) -> str:
    """Normalize distribution string by removing backticks and extra spaces.

    Args:
        distribution: String like "HASH(`id`)" or "HASH(id)"

    Returns:
        Normalized string like "HASH(id)"
    """
    return _normalize_column_identifiers(distribution)


def _normalize_order_by_string(order_by: Union[str, List[str], None]) -> str:
    """Normalize ORDER BY string by removing backticks and standardizing format.

    Args:
        order_by: String or list representing ORDER BY clause

    Returns:
        Normalized string
    """
    if isinstance(order_by, list):
        order_by = ', '.join(str(item) for item in order_by)
    elif order_by is None:
        return ''

    return _normalize_column_identifiers(str(order_by))


def _normalize_column_identifiers(text: str) -> str:
    """Normalize column identifiers by removing backticks and standardizing spaces.

    This is the unified function for handling column names in various contexts.

    Args:
        text: String containing column names with possible backticks

    Returns:
        Normalized string with backticks removed and spaces standardized
    """
    if not text:
        return text

    # Remove backticks around column names
    import re
    normalized = re.sub(r'`([^`]+)`', r'\1', text)
    # Standardize spaces
    normalized = re.sub(r'\s+', ' ', normalized).strip()
    return normalized

