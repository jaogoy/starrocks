# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from functools import wraps
import logging
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

from alembic.autogenerate import comparators
from alembic.autogenerate.api import AutogenContext
from alembic.ddl import DefaultImpl
from alembic.operations.ops import AlterColumnOp, AlterTableOp, UpgradeOps
from sqlalchemy import Column, quoted_name
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.exc import ArgumentError, NotSupportedError
from sqlalchemy.sql import schema as sa_schema, sqltypes
from sqlalchemy.sql.schema import Table

from starrocks import datatype
from starrocks.alembic.ops import (
    AlterViewOp,
    CreateMaterializedViewOp,
    CreateViewOp,
    DropMaterializedViewOp,
    DropViewOp,
)
from starrocks.common.defaults import ReflectionTableDefaults
from starrocks.common.params import (
    AlterTableEnablement,
    ColumnAggInfoKey,
    ColumnAggInfoKeyWithPrefix,
    DialectName,
    SRKwargsPrefix,
    TableInfoKey,
    TableKind,
    TableObjectInfoKey,
    TablePropertyForFuturePartitions,
)
from starrocks.common.utils import CaseInsensitiveDict, TableAttributeNormalizer
from starrocks.datatype import ARRAY, BOOLEAN, MAP, STRING, STRUCT, TINYINT, VARCHAR
from starrocks.engine.interfaces import ReflectedPartitionInfo, ReflectedTableKeyInfo
from starrocks.reflection import StarRocksTableDefinitionParser
from starrocks.sql.schema import MaterializedView, View


logger = logging.getLogger(__name__)


def compare_simple_type(impl: DefaultImpl, inspector_column: Column[Any], metadata_column: Column[Any]) -> bool:
    """
    Set StarRocks' specific simple type comparison logic for some special cases.

    For some special cases:
        - meta.BOOLEAN equals to conn.TINYINT(1)
        - meta.STRING equals to conn.VARCHAR(65533)

    Args:
        impl: The implementation of the dialect.
        inspector_column: The column from the inspector.
        metadata_column: The column from the metadata.

    Returns:
        True if the types are different, False if the types are the same.
    """
    inspector_type = inspector_column.type
    metadata_type = metadata_column.type

    # logger.debug(f"compare_simple_type: inspector_type: {inspector_type}, metadata_type: {metadata_type}")
    # Scenario 1.a: model defined BOOLEAN, database stored TINYINT(1)
    if (isinstance(metadata_type, BOOLEAN) and
        isinstance(inspector_type, TINYINT) and
        getattr(inspector_type, 'display_width', None) == 1):
        logger.debug("compare_simple_type with BOOLEAN vs TINYINT(1), treat them as the same.")
        return False

    # Scenario 1.b: model defined TINYINT(1), database may display as Boolean (theoretically not possible, but for safety)
    if (isinstance(metadata_type, TINYINT) and
        getattr(metadata_type, 'display_width', None) == 1 and
        isinstance(inspector_type, BOOLEAN)):
        logger.debug("compare_simple_type with TINYINT(1) vs BOOLEAN, treat them as the same.")
        return False

    # Scenario 2.a: model defined STRING, database stored VARCHAR(65533)
    if (isinstance(metadata_type, STRING) and
        isinstance(inspector_type, VARCHAR) and
        getattr(inspector_type, 'length', None) == 65533):
        logger.debug("compare_simple_type with STRING vs VARCHAR(65533), treat them as the same.")
        return False

    # Scenario 2.b: model defined VARCHAR(65533), database stored STRING (theoretically not possible, but for safety)
    if (isinstance(metadata_type, VARCHAR) and
        getattr(metadata_type, 'length', None) == 65533 and
        isinstance(inspector_type, STRING)):
        logger.debug("compare_simple_type with VARCHAR(65533) vs STRING, treat them as the same.")
        return False

    # Other cases use default comparison logic from the parent class
    from starrocks.alembic.starrocks import StarRocksImpl
    return super(StarRocksImpl, impl).compare_type(inspector_column, metadata_column)


def compare_complex_type(impl: DefaultImpl, inspector_type: sqltypes.TypeEngine, metadata_type: sqltypes.TypeEngine) -> bool:
    """
    Recursively compares two StarRocks SQLAlchemy complex types.
    Returns True if they are different, False if they are the same.

    Args:
        impl: The implementation of the dialect. It should be a StarRocksImpl instance.
        inspector_type: The type from the inspector.
        metadata_type: The type from the metadata.

    Returns:
        True if the types are different, False if the types are the same.
    """
    # First check if they are the exact same type class
    # logger.debug(f"compare_complex_type with inspector_type: {inspector_type}, metadata_type: {metadata_type}.")
    if not isinstance(metadata_type, datatype.StructuredType):
        # For simple types and other types, use compare_simple_type by composing fake columns
        conn_col = Column("fake_conn_col", inspector_type)
        meta_col = Column("fake_meta_col", metadata_type)
        return compare_simple_type(impl, conn_col, meta_col)

    # Now, the type should be StructuredType (complex data type)
    if type(inspector_type) is not type(metadata_type):
        logger.debug(f"compare_complex_type with different classes: inspector_type: {inspector_type}, metadata_type: {metadata_type}.")
        return True  # Different classes

    if isinstance(inspector_type, ARRAY):
        # We know metadata_type is also ARRAY due to the initial type check
        return compare_complex_type(impl, inspector_type.item_type, metadata_type.item_type)

    if isinstance(inspector_type, MAP):
        # We know metadata_type is also MAP
        if compare_complex_type(impl, inspector_type.key_type, metadata_type.key_type):
            logger.debug(f"compare_complex_type with different key types of MAP: inspector_type: {inspector_type}, metadata_type: {metadata_type}.")
            return True
        return compare_complex_type(impl, inspector_type.value_type, metadata_type.value_type)

    if isinstance(inspector_type, STRUCT):
        # We know metadata_type is also STRUCT
        if len(inspector_type.field_tuples) != len(metadata_type.field_tuples):
            logger.debug(f"compare_complex_type with different number of fields of STRUCT: inspector_type: {inspector_type}, metadata_type: {metadata_type}.")
            return True  # Different number of fields

        # Compare field names and types in order. StarRocks STRUCTs are order-sensitive.
        for (name1, type1_sub), (name2, type2_sub) in zip(
            inspector_type.field_tuples, metadata_type.field_tuples
        ):
            if name1 != name2:
                logger.debug(f"compare_complex_type with different field names of STRUCT: inspector_type: {inspector_type}, metadata_type: {metadata_type}.")
                return True
            if compare_complex_type(impl, type1_sub, type2_sub):
                logger.debug(f"compare_complex_type with different field types of STRUCT: inspector_type: {inspector_type}, metadata_type: {metadata_type}.")
                return True
        return False

    # should not reach here
    return True


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
# include_object handling
# ==============================================================================
def include_object_for_view_mv(object, name, type_, reflected, compare_to):
    """
    Filter objects for Alembic'autogenerate - exclude View/MV from table comparisons.

    This function is used to filter out views and materialized views from the
    default table comparison logic, as they are handled by custom hooks.
    """
    if type_ == "table":
        # object is a sqlalchemy.Table object, from metadata or reflected
        table_kind = object.info.get(TableObjectInfoKey.TABLE_KIND)
        if table_kind in (TableKind.VIEW, TableKind.MATERIALIZED_VIEW):
            return False
    return True


def combine_include_object(user_include_object):
    """
    Combine the dialect's include_object function with a user-defined one.

    The dialect's filter is executed first. If it returns False, the object
    is excluded. If it returns True, the user's filter is then executed.
    This allows users to further customize object inclusion without overriding
    the dialect's necessary filters.
    """
    dialect_include_object = include_object_for_view_mv

    if user_include_object is None:
        return dialect_include_object

    @wraps(user_include_object)
    def combined(object, name, type_, reflected, compare_to):
        if not dialect_include_object(object, name, type_, reflected, compare_to):
            return False
        return user_include_object(object, name, type_, reflected, compare_to)

    return combined


# ==============================================================================
# View Comparison
# ==============================================================================
@comparators_dispatch_for_starrocks("schema")
def autogen_for_views(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schemas: Union[Set[None], Set[Optional[str]]]
) -> None:
    """
    Main autogenerate entrypoint for views.

    Scan views in database and compare with metadata.
    """
    inspector: Inspector = autogen_context.inspector
    metadata = autogen_context.metadata

    conn_view_names: Set[Tuple[Optional[str], str]] = set()

    for schema in schemas:
        views = set(inspector.get_view_names(schema=schema))
        conn_view_names.update(
            (schema, vname)
            for vname in views
            if autogen_context.run_name_filters(
                vname, "view", {"schema_name": schema}
            )
        )

    # Get all views from metadata and apply name filters
    metadata_view_names = set(
        (table.schema, table.name)
        for table in metadata.tables.values()
        if table.info.get(TableObjectInfoKey.TABLE_KIND) == TableKind.VIEW
        and autogen_context.run_name_filters(
            table.name, "view", {"schema_name": table.schema}
        )
    )

    _compare_views(
        conn_view_names,
        metadata_view_names,
        inspector,
        upgrade_ops,
        autogen_context,
    )


def _compare_views(
    conn_view_names: Set[Tuple[Optional[str], str]],
    metadata_view_names: Set[Tuple[Optional[str], str]],
    inspector: Inspector,
    upgrade_ops: UpgradeOps,
    autogen_context: AutogenContext,
) -> None:
    """Compare views between database and metadata, generating add/drop/alter operations."""
    metadata = autogen_context.metadata

    logger.debug(f"conn_view_names (from DB): {conn_view_names}, metadata_view_names (from metadata): {metadata_view_names}")

    # Build a lookup from (schema, name) to Table object for metadata views
    view_name_to_table = {
        (table.schema, table.name): table
        for table in metadata.tables.values()
        if table.info.get(TableObjectInfoKey.TABLE_KIND) == TableKind.VIEW
    }

    logger.debug(f"view_name_to_table keys: {view_name_to_table.keys()}")

    # Added views (in metadata but not in database)
    added_views = metadata_view_names.difference(conn_view_names)
    logger.debug(f"Added views (in metadata but not in DB): {added_views}")
    for s, vname in added_views:
        name = "%s.%s" % (s, vname) if s else vname
        metadata_view = view_name_to_table[(s, vname)]
        if autogen_context.run_object_filters(metadata_view, vname, "view", False, None):
            upgrade_ops.ops.append(CreateViewOp.from_view(metadata_view))
            logger.info("Detected added view %r", name)

    # Dropped views (in database but not in metadata)
    # Use a separate MetaData to avoid polluting the user's metadata
    removal_metadata = sa_schema.MetaData()

    dropped_views = conn_view_names.difference(metadata_view_names)
    logger.debug(f"Dropped views (in DB but not in metadata): {dropped_views}")
    for s, vname in dropped_views:
        logger.debug(f"Processing dropped view: schema={s}, name={vname}")
        name = sa_schema._get_table_key(vname, s)
        exists = name in removal_metadata.tables
        # Create a View object (not Table) since we know it's a view
        # Use empty definition - it will be populated by reflect_table
        t = View(vname, removal_metadata, definition='', schema=s)

        if not exists:
            # Reflect the view using StarRocks' custom reflection logic
            # This will automatically populate table_kind, definition, and dialect_options
            inspector.reflect_table(t, include_columns=None)

        if autogen_context.run_object_filters(t, vname, "view", True, None):
            upgrade_ops.ops.append(DropViewOp.from_view(t))
            logger.info("Detected removed view %r", name)

    # Modified views (in both database and metadata)
    # Use a separate MetaData for reflected views
    existing_metadata = sa_schema.MetaData()
    existing_views = conn_view_names.intersection(metadata_view_names)
    logger.debug(f"Existing views (in both DB and metadata): {existing_views}")

    for s, vname in existing_views:
        # Use reflect_table to get the full view object from database
        name = sa_schema._get_table_key(vname, s)
        exists = name in existing_metadata.tables
        # Create a View object (not Table) since we know it's a view
        # Use empty definition - it will be populated by reflect_table
        t = View(vname, existing_metadata, definition='', schema=s)

        if not exists:
            # Reflect the view using StarRocks' custom reflection logic
            # This will automatically populate table_kind, definition, and dialect_options
            inspector.reflect_table(t, include_columns=None)

    # Compare existing views in sorted order for consistent output
    for s, vname in sorted(existing_views, key=lambda x: (x[0] or "", x[1])):
        s = s or None
        name = "%s.%s" % (s, vname) if s else vname
        metadata_view = view_name_to_table[(s, vname)]
        conn_view = existing_metadata.tables[name]

        logger.debug(f"Comparing existing view: {name}")
        if autogen_context.run_object_filters(metadata_view, vname, "view", False, conn_view):
            logger.debug(f"Dispatching compare_view for {name}")
            # Dispatch to compare_view for detailed comparison
            comparators.dispatch("view")(
                autogen_context,
                upgrade_ops,
                s,
                vname,
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

    Check for changes in view definition, comment, security attributes, and columns.
    """
    # Handle view creation and deletion scenarios (should not happen here)
    if conn_view is None or metadata_view is None:
        logger.warning(
            "compare_view: both conn_view and metadata_view should not be None for %s.%s, skipping",
            schema or autogen_context.dialect.default_schema_name,
            view_name
        )
        return

    logger.debug(f"compare_view: view_name={view_name}, schema={schema}")

    # Extract dialect_options for comparison, similar to compare_starrocks_table
    conn_view_attributes = CaseInsensitiveDict(
        {k: v for k, v in conn_view.dialect_options[DialectName].items() if v is not None}
    )
    meta_view_attributes = CaseInsensitiveDict(
        {k: v for k, v in metadata_view.dialect_options[DialectName].items() if v is not None}
    )

    logger.debug(
        "View-specific attributes comparison for view '%s': "
        "Detected in database: %s. Found in metadata: %s.",
        view_name,
        conn_view_attributes,
        meta_view_attributes,
    )

    # Create AlterViewOp object first, then each comparison function will set attributes if needed
    alter_view_op = AlterViewOp(
        view_name=metadata_view.name,
        schema=schema,
    )

    view_fqn = f"{schema or autogen_context.dialect.default_schema_name}.{view_name}"

    # Compare each view attribute using dedicated functions
    # Order: definition+columns -> comment -> security
    _compare_view_definition_and_columns(
        alter_view_op, view_fqn, conn_view, metadata_view
    )
    _compare_view_comment(
        alter_view_op, view_fqn, conn_view, metadata_view
    )
    _compare_view_security(
        alter_view_op, view_fqn, conn_view, metadata_view,
        conn_view_attributes, meta_view_attributes
    )

    # If any attribute has been set, append the operation
    if (alter_view_op.definition is not None or
        alter_view_op.comment is not None or
        alter_view_op.security is not None):
        upgrade_ops.ops.append(alter_view_op)

        # Log which attributes changed
        changed_attrs = []
        if alter_view_op.definition is not None:
            changed_attrs.append("definition")
        if alter_view_op.comment is not None:
            changed_attrs.append("comment")
        if alter_view_op.security is not None:
            changed_attrs.append("security")

        logger.info(
            "Detected view changes for %s: %s",
            view_fqn,
            ", ".join(changed_attrs),
        )


def _compare_view_definition_and_columns(
    alter_view_op: AlterViewOp,
    view_fqn: str,
    conn_view: View,
    metadata_view: View,
) -> None:
    """
    Compare view definition and columns, and update AlterViewOp if changed.

    Definition and columns are compared together because in StarRocks,
    columns can only be changed by changing the definition (SELECT statement).

    Args:
        alter_view_op: AlterViewOp object to update
        view_fqn: Fully qualified view name for logging
        conn_view: View reflected from database
        metadata_view: View defined in metadata
    """
    from starrocks.sql.schema import extract_view_columns

    # Compare definition (the main content of a view)
    # Definition is stored in table.info (not in dialect_options)
    conn_definition = conn_view.info.get(TableObjectInfoKey.DEFINITION, "")
    meta_definition = metadata_view.info.get(TableObjectInfoKey.DEFINITION, "")
    conn_def_norm = TableAttributeNormalizer.normalize_sql(conn_definition)
    meta_def_norm = TableAttributeNormalizer.normalize_sql(meta_definition)
    definition_changed = conn_def_norm != meta_def_norm

    # Compare columns (if metadata specifies columns explicitly)
    # Note: Only column names and comments are compared, as StarRocks VIEW
    # does not support explicit column type/nullable specifications
    columns_changed = _compare_view_columns(conn_view, metadata_view)

    # Log comparison results
    logger.debug(
        "compare_view_definition_and_columns: %s, definition_changed=%s, columns_changed=%s",
        view_fqn,
        definition_changed,
        columns_changed,
    )

    # Log detailed changes for debugging
    if definition_changed:
        logger.debug(
            "  Definition change for %s:\n"
            "    Database: %s\n"
            "    Metadata: %s",
            view_fqn,
            conn_def_norm[:100] + "..." if len(conn_def_norm) > 100 else conn_def_norm,
            meta_def_norm[:100] + "..." if len(meta_def_norm) > 100 else meta_def_norm,
        )

    if columns_changed:
        conn_cols = {col.name: (col.comment or '') for col in conn_view.columns} if conn_view.columns else {}
        meta_cols = {col.name: (col.comment or '') for col in metadata_view.columns} if metadata_view.columns else {}
        logger.debug(
            "  Columns change for %s:\n"
            "    Database columns: %s\n"
            "    Metadata columns: %s",
            view_fqn,
            conn_cols,
            meta_cols,
        )

    # StarRocks limitation: Columns can only be changed together with definition
    if columns_changed and not definition_changed:
        raise ValueError(
            f"StarRocks does not support altering view columns independently; "
            f"column changes detected for {view_fqn}, "
            f"but definition is unchanged. "
            f"You must change the definition (SELECT statement) together with columns, "
            f"or use DROP + CREATE to apply this change."
        )

    # Set definition and columns if definition changed
    if definition_changed:
        alter_view_op.definition = meta_definition
        alter_view_op.columns = extract_view_columns(metadata_view)
        alter_view_op.reverse_view_definition = conn_definition
        alter_view_op.reverse_view_columns = extract_view_columns(conn_view)


def _compare_view_comment(
    alter_view_op: AlterViewOp,
    view_fqn: str,
    conn_view: View,
    metadata_view: View,
) -> None:
    """
    Compare view comment and update AlterViewOp if changed.

    Args:
        alter_view_op: AlterViewOp object to update
        view_fqn: Fully qualified view name for logging
        conn_view: View reflected from database
        metadata_view: View defined in metadata
    """
    import warnings

    # Compare comment (views don't use Alembic's built-in _compare_table_comment)
    conn_comment = (conn_view.comment or "").strip()
    meta_comment = (metadata_view.comment or "").strip()
    comment_changed = conn_comment != meta_comment

    logger.debug(
        "compare_view_comment: %s, comment_changed=%s",
        view_fqn,
        comment_changed,
    )

    if comment_changed:
        logger.debug(
            "  Comment change for %s:\n"
            "    Database: '%s'\n"
            "    Metadata: '%s'",
            view_fqn,
            conn_comment,
            meta_comment,
        )

        # Warn about comment changes (not supported via ALTER VIEW)
        warnings.warn(
            f"StarRocks does not support altering view comments via ALTER VIEW; "
            f"comment change detected for {view_fqn}, "
            f"from '{conn_comment}' to '{meta_comment}'. "
            f"Consider using DROP + CREATE to apply this change.",
            UserWarning,
            stacklevel=4  # Adjusted stacklevel for nested function call
        )

        # Set comment in AlterViewOp for future compatibility
        alter_view_op.comment = metadata_view.comment
        alter_view_op.reverse_view_comment = conn_view.comment


def _compare_view_security(
    alter_view_op: AlterViewOp,
    view_fqn: str,
    conn_view: View,
    metadata_view: View,
    conn_view_attributes: CaseInsensitiveDict,
    meta_view_attributes: CaseInsensitiveDict,
) -> None:
    """
    Compare view security attribute and update AlterViewOp if changed.

    Args:
        alter_view_op: AlterViewOp object to update
        view_fqn: Fully qualified view name for logging
        conn_view: View reflected from database
        metadata_view: View defined in metadata
        conn_view_attributes: View attributes reflected from database
        meta_view_attributes: View attributes defined in metadata
    """
    import warnings

    # Compare security attribute
    conn_security = TableAttributeNormalizer._simple_normalize(
        conn_view_attributes.get(TableInfoKey.SECURITY)
    )
    meta_security = TableAttributeNormalizer._simple_normalize(
        meta_view_attributes.get(TableInfoKey.SECURITY)
    )
    security_changed = conn_security != meta_security

    logger.debug(
        "compare_view_security: %s, security_changed=%s",
        view_fqn,
        security_changed,
    )

    if security_changed:
        logger.debug(
            "  Security change for %s:\n"
            "    Database: '%s'\n"
            "    Metadata: '%s'",
            view_fqn,
            conn_security,
            meta_security,
        )

        # Warn about security changes (not supported via ALTER VIEW)
        warnings.warn(
            f"StarRocks does not support altering view security via ALTER VIEW; "
            f"security change detected for {view_fqn}, "
            f"from '{conn_security}' to '{meta_security}'. "
            f"Consider using DROP + CREATE to apply this change.",
            UserWarning,
            stacklevel=4  # Adjusted stacklevel for nested function call
        )

        # Set security in AlterViewOp for future compatibility
        alter_view_op.security = meta_view_attributes.get(TableInfoKey.SECURITY)
        alter_view_op.reverse_view_security = conn_view_attributes.get(TableInfoKey.SECURITY)


def _compare_view_columns(conn_view: View, metadata_view: View) -> bool:
    """
    Compare columns between connection view and metadata view.

    Returns True if columns have changed, False otherwise.
    Only compares if metadata_view explicitly defines columns.

    Note: StarRocks VIEW columns only support name and comment (not type or nullable).
    These are derived from the query statement and cannot be explicitly specified.

    Args:
        conn_view: View reflected from database
        metadata_view: View defined in metadata

    Returns:
        True if column names or comments differ, False otherwise
    """
    # If metadata doesn't define columns, skip comparison
    if not metadata_view.columns:
        return False

    # If conn_view has no columns (reflection failed), we can't compare
    # This might happen if reflection encountered an error
    if not conn_view.columns:
        logger.warning(
            f"View '{metadata_view.name}' has columns defined in metadata, "
            f"but no columns were reflected from database. Cannot compare columns."
        )
        return False

    # Build column name -> comment mapping
    conn_cols = {col.name: (col.comment or '') for col in conn_view.columns}
    meta_cols = {col.name: (col.comment or '') for col in metadata_view.columns}

    # Check for added or removed columns
    conn_col_names = set(conn_cols.keys())
    meta_col_names = set(meta_cols.keys())

    if conn_col_names != meta_col_names:
        logger.debug(
            f"View '{metadata_view.name}': Column names differ. "
            f"Database: {sorted(conn_col_names)}, Metadata: {sorted(meta_col_names)}"
        )
        return True

    # Check for column comment changes
    for col_name in meta_col_names:
        conn_comment = conn_cols[col_name]
        meta_comment = meta_cols[col_name]

        if conn_comment != meta_comment:
            logger.debug(
                f"View '{metadata_view.name}': Column '{col_name}' comment differs. "
                f"Database: '{conn_comment}', Metadata: '{meta_comment}'"
            )
            return True

    return False


# ==============================================================================
# Materialized View Comparison
# ==============================================================================
@comparators_dispatch_for_starrocks("schema")
def autogen_for_materialized_views(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schemas: Union[Set[None], Set[Optional[str]]]
) -> None:
    """
    Main autogenerate entrypoint for materialized views.

    Scan materialized views in database and compare with metadata.
    """
    inspector: Inspector = autogen_context.inspector
    metadata = autogen_context.metadata

    conn_mv_names: Set[Tuple[Optional[str], str]] = set()

    for schema in schemas:
        mvs = set(inspector.get_materialized_view_names(schema))
        conn_mv_names.update(
            (schema, mvname)
            for mvname in mvs
            if autogen_context.run_name_filters(
                mvname, "materialized_view", {"schema_name": schema}
            )
        )

    metadata_mv_names = set(
        (table.schema, table.name)
        for table in metadata.tables.values()
        if table.info.get(TableObjectInfoKey.TABLE_KIND) == TableKind.MATERIALIZED_VIEW
    )

    _compare_mvs(
        conn_mv_names,
        metadata_mv_names,
        inspector,
        upgrade_ops,
        autogen_context,
    )


def _compare_mvs(
    conn_mv_names: Set[Tuple[Optional[str], str]],
    metadata_mv_names: Set[Tuple[Optional[str], str]],
    inspector: Inspector,
    upgrade_ops: UpgradeOps,
    autogen_context: AutogenContext,
) -> None:
    """Compare materialized views between database and metadata, generating add/drop/alter operations."""
    metadata = autogen_context.metadata

    # Build a lookup from (schema, name) to Table object for metadata MVs
    mv_name_to_table = {
        (table.schema, table.name): table
        for table in metadata.tables.values()
        if table.info.get(TableObjectInfoKey.TABLE_KIND) == TableKind.MATERIALIZED_VIEW
    }

    # Added MVs (in metadata but not in database)
    for s, mvname in metadata_mv_names.difference(conn_mv_names):
        name = "%s.%s" % (s, mvname) if s else mvname
        metadata_mv = mv_name_to_table[(s, mvname)]
        if autogen_context.run_object_filters(
            metadata_mv, mvname, "materialized_view", False, None
        ):
            upgrade_ops.ops.append(CreateMaterializedViewOp.from_materialized_view(metadata_mv))
            logger.info("Detected added materialized view %r", name)

    # Dropped MVs (in database but not in metadata)
    # Use a separate MetaData to avoid polluting the user's metadata
    removal_metadata = sa_schema.MetaData()

    for s, mvname in conn_mv_names.difference(metadata_mv_names):
        name = sa_schema._get_table_key(mvname, s)
        exists = name in removal_metadata.tables
        # Create a MaterializedView object (not Table) since we know it's a materialized view
        # Use empty definition - it will be populated by reflect_table
        t = MaterializedView(mvname, removal_metadata, definition='', schema=s)

        if not exists:
            # Reflect the MV using StarRocks' custom reflection logic
            # This will automatically populate table_kind, definition, and dialect_options
            inspector.reflect_table(t, include_columns=None)

        if autogen_context.run_object_filters(t, mvname, "materialized_view", True, None):
            upgrade_ops.ops.append(DropMaterializedViewOp.from_materialized_view(t))
            logger.info("Detected removed materialized view %r", name)

    # Modified MVs (in both database and metadata)
    # Use a separate MetaData for reflected MVs
    existing_metadata = sa_schema.MetaData()
    existing_mvs = conn_mv_names.intersection(metadata_mv_names)

    for s, mvname in existing_mvs:
        # Use reflect_table to get the full MV object from database
        name = sa_schema._get_table_key(mvname, s)
        exists = name in existing_metadata.tables
        # Create a MaterializedView object (not Table) since we know it's a materialized view
        # Use empty definition - it will be populated by reflect_table
        t = MaterializedView(mvname, existing_metadata, definition='', schema=s)

        if not exists:
            # Reflect the MV using StarRocks' custom reflection logic
            # This will automatically populate table_kind, definition, and dialect_options
            inspector.reflect_table(t, include_columns=None)

    # Compare existing MVs in sorted order for consistent output
    for s, mvname in sorted(existing_mvs, key=lambda x: (x[0] or "", x[1])):
        s = s or None
        name = "%s.%s" % (s, mvname) if s else mvname
        metadata_mv = mv_name_to_table[(s, mvname)]
        conn_mv = existing_metadata.tables[name]

        # Apply object filters
        if autogen_context.run_object_filters(
            metadata_mv, mvname, "materialized_view", False, conn_mv
        ):
            # Dispatch to compare_materialized_view for detailed comparison
            comparators.dispatch("materialized_view")(
                autogen_context,
                upgrade_ops,
                s,
                mvname,
                conn_mv,
                metadata_mv,
            )


@comparators_dispatch_for_starrocks("materialized_view")
def compare_materialized_view(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    mv_name: str,
    conn_mv: MaterializedView,
    metadata_mv: MaterializedView,
) -> None:
    """
    Compare a single materialized view and generate operations if needed.

    StarRocks does not support ALTER MATERIALIZED VIEW, so any change
    requires DROP + CREATE.
    """
    # Handle MV creation and deletion scenarios (should not happen here)
    if conn_mv is None or metadata_mv is None:
        logger.warning(
            "compare_materialized_view: both conn_mv and metadata_mv should not be None for %s.%s, skipping",
            schema or autogen_context.dialect.default_schema_name,
            mv_name
        )
        return

    logger.debug(f"compare_materialized_view: mv_name={mv_name}, schema={schema}")

    # Extract dialect_options for comparison, similar to compare_starrocks_table
    conn_mv_attributes = CaseInsensitiveDict(
        {k: v for k, v in conn_mv.dialect_options[DialectName].items() if v is not None}
    )
    meta_mv_attributes = CaseInsensitiveDict(
        {k: v for k, v in metadata_mv.dialect_options[DialectName].items() if v is not None}
    )

    logger.debug(
        "MV-specific attributes comparison for materialized view '%s': "
        "Detected in database: %s. Found in metadata: %s.",
        mv_name,
        conn_mv_attributes,
        meta_mv_attributes,
    )

    # Compare definition
    definition_changed = (
        TableAttributeNormalizer.normalize_sql(conn_mv.definition) !=
        TableAttributeNormalizer.normalize_sql(metadata_mv.definition)
    )

    # Compare physical table attributes
    partition_changed = (
        TableAttributeNormalizer.normalize_partition_method(
            conn_mv_attributes.get(TableInfoKey.PARTITION_BY)
        ) !=
        TableAttributeNormalizer.normalize_partition_method(
            meta_mv_attributes.get(TableInfoKey.PARTITION_BY)
        )
    )

    distribution_changed = (
        TableAttributeNormalizer.normalize_distribution_string(
            conn_mv_attributes.get(TableInfoKey.DISTRIBUTED_BY)
        ) !=
        TableAttributeNormalizer.normalize_distribution_string(
            meta_mv_attributes.get(TableInfoKey.DISTRIBUTED_BY)
        )
    )

    order_by_changed = (
        TableAttributeNormalizer.normalize_order_by_string(
            conn_mv_attributes.get(TableInfoKey.ORDER_BY)
        ) !=
        TableAttributeNormalizer.normalize_order_by_string(
            meta_mv_attributes.get(TableInfoKey.ORDER_BY)
        )
    )

    # Compare refresh attributes
    refresh_moment_changed = (
        conn_mv_attributes.get(TableInfoKey.REFRESH_MOMENT) !=
        meta_mv_attributes.get(TableInfoKey.REFRESH_MOMENT)
    )

    refresh_type_changed = (
        conn_mv_attributes.get(TableInfoKey.REFRESH_TYPE) !=
        meta_mv_attributes.get(TableInfoKey.REFRESH_TYPE)
    )

    # Compare properties
    properties_changed = (
        conn_mv_attributes.get(TableInfoKey.PROPERTIES) !=
        meta_mv_attributes.get(TableInfoKey.PROPERTIES)
    )

    # Compare comment (MVs don't use Alembic's built-in _compare_table_comment)
    conn_comment = (conn_mv.comment or "").strip()
    meta_comment = (metadata_mv.comment or "").strip()
    comment_changed = conn_comment != meta_comment

    logger.debug(
        "compare_materialized_view: %s.%s definition_changed=%s partition_changed=%s "
        "distribution_changed=%s order_by_changed=%s refresh_moment_changed=%s "
        "refresh_type_changed=%s properties_changed=%s comment_changed=%s",
        schema or autogen_context.dialect.default_schema_name,
        mv_name,
        definition_changed,
        partition_changed,
        distribution_changed,
        order_by_changed,
        refresh_moment_changed,
        refresh_type_changed,
        properties_changed,
        comment_changed,
    )

    # If any attribute changed, generate DROP + CREATE operations
    # StarRocks does not support ALTER MATERIALIZED VIEW
    if any([
        definition_changed,
        partition_changed,
        distribution_changed,
        order_by_changed,
        refresh_moment_changed,
        refresh_type_changed,
        properties_changed,
        comment_changed,
    ]):
        upgrade_ops.ops.append(
            (
                DropMaterializedViewOp.from_materialized_view(conn_mv),
                CreateMaterializedViewOp.from_materialized_view(metadata_mv),
            )
        )
        logger.info(
            "Detected materialized view change for %s.%s, will DROP and CREATE",
            schema or autogen_context.dialect.default_schema_name,
            mv_name,
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

    if metadata_table is not None and metadata_table.comment is None:
        # Handle backward compatibility for 'starrocks_comment'.
        if starrocks_comment := meta_table_attributes.get(TableInfoKey.COMMENT):
            import warnings
            warnings.warn(
                f"The 'starrocks_comment' dialect argument is deprecated for table '{table_name}'. "
                "Please use the standard 'comment' argument on the Table object instead.",
                DeprecationWarning,
                stacklevel=4,
            )
            metadata_table.comment = starrocks_comment

    # Note: Table comment comparison is handled by Alembic's built-in _compare_table_comment

    # Compare each type of table attribute using dedicated functions
    # Order follows StarRocks CREATE TABLE grammar:
    #   engine -> key -> comment -> partition -> distribution -> order by -> properties
    table, schema = conn_table.name, conn_table.schema

    # Track the number of operations before comparison
    ops_before = len(upgrade_ops.ops)

    _compare_table_engine(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_key(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    # Note: COMMENT comparison is handled by Alembic's built-in _compare_table_comment
    _compare_table_partition(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_distribution(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_order_by(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes)
    _compare_table_properties(upgrade_ops.ops, schema, table, conn_table_attributes, meta_table_attributes, run_mode)

    # Log summary if any operations were generated
    ops_after = len(upgrade_ops.ops)
    if ops_after > ops_before:
        full_table_name = f"{schema}.{table_name}" if schema else table_name
        num_changes = ops_after - ops_before
        logger.info(
            f"Table '{full_table_name}' comparison complete: "
            f"{num_changes} ALTER operation(s) generated"
        )

    return False

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
    But, if only the key columns are changed, we can generate an WARNING, but not .
    """
    conn_key: Optional[ReflectedTableKeyInfo] = _get_table_key_type(conn_table_attributes)
    meta_key: Optional[ReflectedTableKeyInfo] = _get_table_key_type(meta_table_attributes)
    logger.debug(f"KEY. conn_key: {conn_key}, meta_key: {meta_key}")

    if isinstance(conn_key, str):
        conn_key = StarRocksTableDefinitionParser.parse_key_clause(conn_key)
    if isinstance(meta_key, str):
        meta_key = StarRocksTableDefinitionParser.parse_key_clause(meta_key)

    # Reflected table must have a default KEY, so we need to normalize it
    # Actually, the conn key must not be None, because it is inspected from database.
    normalized_conn: Optional[str] = TableAttributeNormalizer.normalize_key(conn_key)
    normalized_meta: Optional[str] = TableAttributeNormalizer.normalize_key(meta_key)
    logger.debug(f"KEY. normalized_conn: {normalized_conn!r}, normalized_meta: {normalized_meta!r}")

    if _compare_single_table_attribute(
        table_name,
        schema,
        TableInfoKey.KEY,
        normalized_conn,
        normalized_meta,
        default_value=ReflectionTableDefaults.key(),
        equal_to_default_cmp_func=_is_equal_key_with_defaults,
        support_change=AlterTableEnablement.KEY
    ):
        if conn_key is None:
            conn_key = ReflectionTableDefaults.reflected_key_info()
        if meta_key is None:
            meta_key = ReflectionTableDefaults.reflected_key_info()
        if conn_key.type != meta_key.type:
            raise NotSupportedError(
                f"Table '{table_name}' has different key types: {conn_key.type} to {meta_key.type}, "
                "but it's not supported to change the key type.",
                None,
                None,
            )
        else:
            logger.warning(f"Table '{table_name}' has different key columns: ({conn_key.columns}) to ({meta_key.columns}), "
                           f"with the same table type: {conn_key.type}. "
                           f"But it's not explicitly supported to change the key columns.")

def _get_table_key_type(table_attributes: Dict[str, Any]) -> Optional[ReflectedTableKeyInfo]:
    """Get table key type. like 'PRIMARY KEY (id, name)'
    The key in table_attributes is like 'PRIMARY_KEY' without prefix 'starrocks_',
    and the value is like 'id, name'.

    Args:
        table_attributes: All table attributes without prefix 'starrocks_'.

    Returns:
        The table key type. like ReflectedTableKeyInfo('PRIMARY KEY', 'id, name').
        None if the table key type is not found.
    """
    for key_type in TableInfoKey.KEY_KWARG_MAP:
        key_columns: str = table_attributes.get(key_type)
        if key_columns:
            key_columns = TableAttributeNormalizer.remove_outer_parentheses(key_columns)
            # return f"{TableInfoKey.KEY_KWARG_MAP[key_type]} ({key_columns})"
            return ReflectedTableKeyInfo(type=TableInfoKey.KEY_KWARG_MAP[key_type], columns=key_columns)
    return None

def _is_equal_key_with_defaults(
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


def _is_equal_partition_method(
    conn_partition: Optional[Union[ReflectedPartitionInfo, str]],
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

def _compare_table_comment_sr(
    ops_list: List[AlterTableOp],
    schema: Optional[str],
    table_name: str,
    conn_table: Table,
    metadata_table: Table,
    meta_table_attributes: CaseInsensitiveDict,
) -> None:
    """Compare table comments, with backward compatibility for 'starrocks_comment'.
    Note: useless now.
    """
    conn_comment = conn_table.comment
    meta_comment = metadata_table.comment

    if meta_comment is None:
        # For backward compatibility
        if starrocks_comment := meta_table_attributes.get(TableInfoKey.COMMENT):
            import warnings
            warnings.warn(
                f"The 'starrocks_comment' dialect argument is deprecated for table '{table_name}'. "
                "Please use the standard 'comment' argument on the Table object instead.",
                DeprecationWarning,
                stacklevel=4,
            )
            meta_comment = starrocks_comment

    if conn_comment != meta_comment:
        from alembic.operations import ops

        if meta_comment is None:
            ops_list.append(
                ops.DropTableCommentOp(
                    table_name, schema=schema, existing_comment=conn_comment
                )
            )
        else:
            ops_list.append(
                ops.CreateTableCommentOp(
                    table_name,
                    meta_comment,
                    schema=schema,
                    existing_comment=conn_comment,
                )
            )

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
        equal_to_default_cmp_func=_is_equal_partition_method
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
    if meta_distribution is None:
        # For backward compatibility
        if starrocks_distribution := meta_table_attributes.get("DISTRIBUTION"):
            import warnings
            warnings.warn(
                f"The 'starrocks_distribution' dialect argument is deprecated for table '{table_name}'. "
                "Please use 'starrocks_distributed_by' instead.",
                DeprecationWarning,
                stacklevel=4,
            )
            meta_distribution = starrocks_distribution

    if isinstance(conn_distribution, str):
        conn_distribution = StarRocksTableDefinitionParser.parse_distribution(conn_distribution)
    if isinstance(meta_distribution, str):
        meta_distribution = StarRocksTableDefinitionParser.parse_distribution(meta_distribution)

    # If distribution method is the same and meta doesn't specify buckets,
    # consider it unchanged, as conn buckets might be system-assigned.
    if (
        conn_distribution and meta_distribution and
        conn_distribution.distribution_method == meta_distribution.distribution_method and
        meta_distribution.buckets is None
    ):
        return

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
    run_mode: str,
) -> None:
    """Compare properties changes and add AlterTablePropertiesOp if needed.

    - If a property is specified in metadata, it is compared with the database.
    - If a property is NOT specified in metadata but exists in the database with a NON-DEFAULT value,
      a change is detected.
    - The generated operation will set only the properties that have changed.
      Because some of the properties are not supported to be changed.
    """
    conn_properties: Dict[str, str] = conn_table_attributes.get(TableInfoKey.PROPERTIES, {})
    meta_properties: Dict[str, str] = meta_table_attributes.get(TableInfoKey.PROPERTIES, {})
    logger.debug(f"PROPERTIES. conn_properties: {conn_properties}, meta_properties: {meta_properties}")

    normalized_conn = CaseInsensitiveDict(conn_properties)
    normalized_meta = CaseInsensitiveDict(meta_properties)
    # logger.debug(f"PROPERTIES. normalized_conn: {normalized_conn}, normalized_meta: {normalized_meta}")

    if normalized_conn == normalized_meta:
        return

    properties_to_set = {}
    properties_for_reverse = {}

    all_keys = set(normalized_conn.keys()) | set(normalized_meta.keys())
    full_table_name = f"{schema}.{table_name}" if schema else table_name

    for key in all_keys:
        conn_value = normalized_conn.get(key)
        meta_value = normalized_meta.get(key)
        default_value = ReflectionTableDefaults.properties(run_mode).get(key)

        # Convert all to strings for comparison to avoid type issues (e.g., int vs str)
        conn_str = str(conn_value) if conn_value is not None else None
        meta_str = str(meta_value) if meta_value is not None else None
        default_str = str(default_value) if default_value is not None else None

        # The effective value in the database is conn_str if set, otherwise default_str
        effective_conn_str = conn_str if conn_str is not None else default_str
        # The effective value in the metadata is meta_str if set, otherwise default_str
        effective_meta_str = meta_str if meta_str is not None else default_str

        if effective_conn_str == effective_meta_str:
            logger.debug(f"Property no changes. key: {key}, effective_conn_str: {effective_conn_str}, effective_meta_str: {effective_meta_str}")
            continue

        # A meaningful change has been detected for this property.
        logger.info(
            f"Detected property change for table '{full_table_name}': "
            f"'{key}' changed from '{effective_conn_str}' to '{effective_meta_str}'"
        )
        logger.debug(f"Property changes. key: {key}, effective_conn_str: {effective_conn_str}, effective_meta_str: {effective_meta_str}")
        if meta_value is None:
            if default_value is None:
                # Scenario 1: Implicit deletion of a property with no default.
                if conn_value is not None:
                    logger.warning(
                        f"Table '{full_table_name}': Property '{key}' exists in the database with value '{conn_value}' "
                        f"but is not specified in metadata and no default is defined in ReflectionTableDefaults."
                        f"Implicit deletion is not recommended. "
                        f"To manage this property, please specify it explicitly in your metadata. "
                        f"No ALTER TABLE SET operation will be generated for this property."
                    )
                    continue  # Skip generating an op for this property
            else:
                # Scenario 2: Implicit reset to default.
                logger.warning(
                    f"Table '{full_table_name}': Property '{key}' has non-default value '{conn_value}' in database "
                    f"but is not specified in metadata. An ALTER TABLE SET operation will be generated to "
                    f"reset it to its default value '{default_value}'. "
                    f"Consider explicitly setting default properties in your metadata to avoid ambiguity."
                )
        # Determine the value for the upgrade operation.
        target_val_upgrade = meta_str if meta_str is not None else default_str
        prop_key = (
            TablePropertyForFuturePartitions.wrap(key)
            if TablePropertyForFuturePartitions.contains(key)
            else key
        )
        # logger.debug(f"Newly changed property. prop_key: '{prop_key}', target_val_upgrade: '{target_val_upgrade}'")
        properties_to_set[prop_key] = target_val_upgrade
        if prop_key != key:
            logger.warning(f"The property '{key}' will be changed to '{target_val_upgrade}' "
                f"for the future partitions only by using '{prop_key}'. "
                f"If you want to change the property for all partitions, "
                f"please modify it by removing the 'default.' prefix."
            )

        # Determine the value for the downgrade (reverse) operation.
        target_val_downgrade = conn_str if conn_str is not None else default_str
        properties_for_reverse[prop_key] = target_val_downgrade

    if properties_to_set:
        from starrocks.alembic.ops import AlterTablePropertiesOp

        ops_list.append(
            AlterTablePropertiesOp(
                table_name,
                properties_to_set,
                schema=schema,
                reverse_properties=properties_for_reverse,
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
            # Log the detected change at INFO level
            logger.info(
                f"Detected table attribute change for '{full_table_name}': "
                f"{attribute_name} changed from '{conn_str or '(not set)'}' to '{meta_str}'"
            )
            logger.debug(f"Table '{full_table_name}', Attribute '{attribute_name}' has changed "
                         f"from '{conn_str or '(not set)'}' to '{meta_str}' with default value '{default_str}'")
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
    if conn_col is None or metadata_col is None:
        raise ArgumentError("Both conn column and meta column should not be None.")

    conn_opts = CaseInsensitiveDict(
        {k: v for k, v in conn_col.dialect_options[DialectName].items() if v is not None}
    )
    meta_opts = CaseInsensitiveDict(
        {k: v for k, v in metadata_col.dialect_options[DialectName].items() if v is not None}
    )
    conn_agg_type: Union[str, None] = conn_opts.get(ColumnAggInfoKey.AGG_TYPE)
    meta_agg_type: Union[str, None] = meta_opts.get(ColumnAggInfoKey.AGG_TYPE)
    # logger.debug(f"AGG_TYPE. conn_agg_type: {conn_agg_type}, meta_agg_type: {meta_agg_type}")

    if meta_agg_type != conn_agg_type:
        # Update the alter_column_op with the new aggregate type. useless now
        # "KEY", "SUM" for set, None for unsert
        if alter_column_op is not None:
            alter_column_op.kw[ColumnAggInfoKeyWithPrefix.AGG_TYPE] = meta_agg_type
        raise NotSupportedError(
            f"StarRocks does not support changing the aggregation type of a column: '{cname}', "
            f"from {conn_agg_type} to {meta_agg_type}.",
            None, None
        )

    # we need to set it in the AlterColumnOp, because the KEY/AGG_TYPE is always needed.
    # TODO: But currently, it's not passed to MySQLModifyColumn
    if alter_column_op:
        alter_column_op.kw[ColumnAggInfoKeyWithPrefix.AGG_TYPE] = meta_agg_type


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
    if conn_col is None or metadata_col is None:
        raise ArgumentError("Both conn column and meta column should not be None.")

    # Because we can't inpsect the autoincrement, we can't do the check the difference.
    if conn_col.autoincrement != metadata_col.autoincrement and \
            "auto" != metadata_col.autoincrement:
        logger.warning(
            f"Detected AUTO_INCREMENT is changed for column {cname}. "
            f"conn_col.autoincrement: {conn_col.autoincrement}, "
            f"metadata_col.autoincrement: {metadata_col.autoincrement}. "
            f"No ALTER statement will be generated automatically, "
            f"Because we can't inpsect the column's autoincrement currently."
        )
    return None

