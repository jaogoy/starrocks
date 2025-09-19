from typing import Final
from alembic.autogenerate import renderers
from .ops import (
    CreateViewOp, DropViewOp, CreateMaterializedViewOp, DropMaterializedViewOp, AlterViewOp,
    AlterTablePropertiesOp, AlterTableDistributionOp, AlterTableOrderOp
)
from alembic.autogenerate.api import AutogenContext
import logging

logger = logging.getLogger("starrocks.alembic.render")


op_param_indent: Final[str] = " " * 4


def _quote_schema(schema: str) -> str:
    """Quote schema name using single quotes with proper escaping.
    That is (' -> \')
    """
    return f"'{schema.replace(chr(39), chr(92) + chr(39))}'" if schema else None


@renderers.dispatch_for(AlterViewOp)
def _alter_view(autogen_context: AutogenContext, op: AlterViewOp) -> str:
    """Render an AlterViewOp for autogenerate."""
    args = [
        f"{op.view_name!r}",
    ]
    args.append(f"\n{op_param_indent}{op.definition!r}\n")
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")
    if op.comment:
        args.append(f"comment={op.comment!r}")
    if op.security:
        args.append(f"security={op.security!r}")

    call = f"op.alter_view({', '.join(args)})"
    logger.debug("render alter_view: %s", call)
    return call

@renderers.dispatch_for(CreateViewOp)
def _create_view(autogen_context: AutogenContext, op: CreateViewOp) -> str:
    args = [
        f"{op.view_name!r}",
        f"{op.definition!r}"
    ]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")
    if op.security:
        args.append(f"security={op.security!r}")
    if op.comment:
        args.append(f"comment={op.comment!r}")

    call = f"op.create_view({', '.join(args)})"
    logger.debug("render create_view: %s", call)
    return call


@renderers.dispatch_for(DropViewOp)
def _drop_view(autogen_context: AutogenContext, op: DropViewOp) -> str:
    args = [f"{op.view_name!r}"]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")
    
    call = f"op.drop_view({', '.join(args)})"
    logger.debug("render drop_view: %s", call)
    return call


@renderers.dispatch_for(CreateMaterializedViewOp)
def _create_materialized_view(autogen_context: AutogenContext, op: CreateMaterializedViewOp) -> str:
    args = [
        f"{op.view_name!r}",
        f"{op.definition!r}",
    ]
    if op.properties:
        args.append(f"properties={op.properties!r}")
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")

    call = f"op.create_materialized_view({', '.join(args)})"
    logger.debug("render create_materialized_view: %s", call)
    return call


@renderers.dispatch_for(DropMaterializedViewOp)
def _drop_materialized_view(autogen_context: AutogenContext, op: DropMaterializedViewOp) -> str:
    args = [f"{op.view_name!r}"]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")

    call = f"op.drop_materialized_view({', '.join(args)})"
    logger.debug("render drop_materialized_view: %s", call)
    return call


@renderers.dispatch_for(AlterTableDistributionOp)
def _render_alter_table_distribution(autogen_context: AutogenContext, op: AlterTableDistributionOp) -> str:
    """Render an AlterTableDistributionOp for autogenerate."""
    args = [
        f"{op.table_name!r}",
        f"{op.distribution_method!r}",
    ]
    if op.buckets is not None:
        args.append(f"buckets={op.buckets}")
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")

    return f"op.alter_table_distribution({', '.join(args)})"


@renderers.dispatch_for(AlterTableOrderOp)
def _render_alter_table_order(autogen_context: AutogenContext, op: AlterTableOrderOp) -> str:
    """Render an AlterTableOrderOp for autogenerate."""
    args = [
        f"{op.table_name!r}",
        f"{op.order_by!r}"
    ]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")
    
    return f"op.alter_table_order({', '.join(args)})"


@renderers.dispatch_for(AlterTablePropertiesOp)
def _render_alter_table_properties(autogen_context: AutogenContext, op: AlterTablePropertiesOp) -> str:
    """Render an AlterTablePropertiesOp for autogenerate."""
    args = [
        f"{op.table_name!r}",
        f"{op.properties!r}"
    ]
    if op.schema:
        args.append(f"schema={_quote_schema(op.schema)}")
    
    return f"op.alter_table_properties({', '.join(args)})"
