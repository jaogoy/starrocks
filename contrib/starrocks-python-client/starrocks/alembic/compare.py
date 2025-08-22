import re
from typing import Dict, List, Optional, Set, Tuple

from alembic.autogenerate import api, comparators
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps

from starrocks.sql.schema import MaterializedView, View

from .ops import (
    CreateMaterializedViewOp,
    CreateViewOp,
    DropMaterializedViewOp,
    DropViewOp,
)


def normalize_sql(sql_text: Optional[str]) -> Optional[str]:
    """A simple normalizer for SQL text."""
    if sql_text is None:
        return None
    # Remove comments
    sql_text = re.sub(r"--.*?\n", "", sql_text)
    # Collapse whitespace and convert to lowercase
    sql_text = re.sub(r"\s+", " ", sql_text).strip().lower()
    return sql_text

# ==============================================================================
# View Comparison
# ==============================================================================

@comparators.dispatch_for("schema")
def autogen_for_views(
    autogen_context: AutogenContext, upgrade_ops: UpgradeOps, schemas: List[Optional[str]]
) -> None:
    """Main autogenerate entrypoint for views."""
    inspector = autogen_context.inspector

    conn_views: Set[Tuple[Optional[str], str]] = set()
    for schema in schemas:
        conn_views.update((schema, name) for name in inspector.get_view_names(schema=schema))

    metadata_views_info = autogen_context.metadata.info.get("views", {})
    metadata_views: Dict[Tuple[Optional[str], str], View] = {
        (view_obj.schema or autogen_context.dialect.default_schema_name, view_obj.name): view_obj
        for key, view_obj in metadata_views_info.items()
    }

    _compare_views(conn_views, metadata_views, autogen_context, upgrade_ops)

def _compare_views(
    conn_views: Set[Tuple[Optional[str], str]],
    metadata_views: Dict[Tuple[Optional[str], str], View],
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
) -> None:
    """Compare views between the database and the metadata and generate operations."""
    inspector = autogen_context.inspector

    # Find new views to create
    for schema, view_name in sorted(metadata_views.keys() - conn_views):
        view = metadata_views[(schema, view_name)]
        upgrade_ops.ops.append(CreateViewOp(view.name, view.definition, schema=schema, security=view.security))

    # Find old views to drop
    for schema, view_name in sorted(conn_views - metadata_views.keys()):
        upgrade_ops.ops.append(DropViewOp(view_name, schema=schema))

    # Find views that exist in both and compare their definitions
    for schema, view_name in sorted(conn_views.intersection(metadata_views.keys())):
        # NOTE: We don't have a way to reflect the SECURITY property yet.
        # This will be a TODO for the inspector. For now, assume None.
        conn_view = View(
            view_name,
            inspector.get_view_definition(view_name, schema=schema),
            schema=schema,
            security=None # TODO: Reflect this property
        )
        metadata_view = metadata_views[(schema, view_name)]

        comparators.dispatch("view")(
            autogen_context,
            upgrade_ops,
            schema,
            view_name,
            conn_view,
            metadata_view,
        )

@comparators.dispatch_for("view")
def compare_view(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    view_name: str,
    conn_view: View,
    metadata_view: View,
) -> None:
    """Compare a single view and generate operations if needed."""
    if conn_view.definition != metadata_view.definition or conn_view.security != metadata_view.security:
        upgrade_ops.ops.append(
            (
                DropViewOp(view_name, schema=schema),
                CreateViewOp(
                    metadata_view.name,
                    metadata_view.definition,
                    schema=schema,
                    security=metadata_view.security,
                ),
            )
        )

# ==============================================================================
# Materialized View Comparison
# ==============================================================================

@comparators.dispatch_for("schema")
def autogen_for_materialized_views(
    autogen_context: AutogenContext, upgrade_ops: UpgradeOps, schemas: List[Optional[str]]
) -> None:
    """Main autogenerate entrypoint for MVs."""
    inspector = autogen_context.inspector

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
    inspector = autogen_context.inspector

    # Find new MVs to create
    for schema, mv_name in sorted(metadata_mvs.keys() - conn_mvs):
        mv = metadata_mvs[(schema, mv_name)]
        upgrade_ops.ops.append(
            CreateMaterializedViewOp(mv.name, mv.definition, schema=schema)
        )

    # Find old MVs to drop
    for schema, mv_name in sorted(conn_mvs - metadata_mvs.keys()):
        upgrade_ops.ops.append(DropMaterializedViewOp(mv_name, schema=schema))

    # Find modified MVs
    for schema, mv_name in sorted(conn_mvs.intersection(metadata_mvs.keys())):
        conn_mv = MaterializedView(
            mv_name,
            inspector.get_materialized_view_definition(mv_name, schema=schema),
            schema=schema
        )
        metadata_mv = metadata_mvs[(schema, mv_name)]

        comparators.dispatch("materialized_view")(
            autogen_context,
            upgrade_ops,
            schema,
            mv_name,
            conn_mv,
            metadata_mv,
        )

@comparators.dispatch_for("materialized_view")
def _compare_one_mv(
    autogen_context: AutogenContext,
    upgrade_ops: UpgradeOps,
    schema: Optional[str],
    mv_name: str,
    conn_mv: MaterializedView,
    metadata_mv: MaterializedView,
) -> None:
    """Compare a single MV and generate operations if needed."""
    if conn_mv.definition != metadata_mv.definition:
        upgrade_ops.ops.append(
            (
                DropMaterializedViewOp(mv_name, schema=schema),
                CreateMaterializedViewOp(
                    metadata_mv.name, metadata_mv.definition, schema=schema
                ),
            )
        )

