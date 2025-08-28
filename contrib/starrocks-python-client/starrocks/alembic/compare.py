import re
import logging
from typing import Any, Dict, List, Optional, Set, Tuple

from alembic.autogenerate import comparators
from alembic.autogenerate.api import AutogenContext
from alembic.operations.ops import UpgradeOps
from sqlalchemy.engine.reflection import Inspector
from sqlalchemy.sql.schema import Table

from starrocks.params import SRKwargsPrefix, TableInfoKey
from starrocks.sql.schema import MaterializedView, View
from starrocks.reflection import ReflectionViewInfo

from .ops import (
    CreateViewOp,
    AlterViewOp,
    DropViewOp,
    CreateMaterializedViewOp,
    DropMaterializedViewOp,
)

logger = logging.getLogger("starrocks.alembic.compare")


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

@comparators.dispatch_for("schema")
def autogen_for_views(
    autogen_context: AutogenContext, upgrade_ops: UpgradeOps, schemas: List[Optional[str]]
) -> None:
    """Main autogenerate entrypoint for views."""
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
        upgrade_ops.ops.append(CreateViewOp(view.name, view.definition, schema=schema, security=view.security, comment=view.comment))

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
            "StarRocks does not support altering view comments via ALTER VIEW; comment change detected for %s.%s and will be ignored",
            schema or autogen_context.dialect.default_schema_name,
            view_name,
        )

    if security_changed:
        logger.warning(
            "StarRocks does not support altering view security via ALTER VIEW; security change detected for %s.%s and will be ignored",
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

@comparators.dispatch_for("schema")
def autogen_for_materialized_views(
    autogen_context: AutogenContext, upgrade_ops: UpgradeOps, schemas: List[Optional[str]]
) -> None:
    """Main autogenerate entrypoint for MVs."""
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

