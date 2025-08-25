from alembic.autogenerate import renderers
from .ops import CreateViewOp, DropViewOp, CreateMaterializedViewOp, DropMaterializedViewOp, AlterViewOp
from alembic.autogenerate.api import AutogenContext


@renderers.dispatch_for(AlterViewOp)
def _alter_view(autogen_context: AutogenContext, op: AlterViewOp) -> str:
    """Render an AlterViewOp for autogenerate."""
    args = [
        f"'{op.view_name}'",
        f"'{op.definition}'"
    ]
    if op.schema:
        args.append(f"schema='{op.schema}'")
    if op.comment:
        args.append(f"comment='{op.comment}'")
    if op.security:
        args.append(f"security='{op.security}'")
    
    return f"op.alter_view({', '.join(args)})"

@renderers.dispatch_for(CreateViewOp)
def _add_view(autogen_context: AutogenContext, op: CreateViewOp) -> str:
    args = [
        f"'{op.view_name}'",
        f"'{op.definition}'"
    ]
    if op.schema:
        args.append(f"schema='{op.schema}'")
    if op.security:
        args.append(f"security='{op.security}'")
    
    return f"op.create_view({', '.join(args)})"


@renderers.dispatch_for(DropViewOp)
def _drop_view(autogen_context: AutogenContext, op: DropViewOp) -> str:
    return f"op.drop_view('{op.view_name}', schema='{op.schema}')"


@renderers.dispatch_for(CreateMaterializedViewOp)
def _add_materialized_view(autogen_context: AutogenContext, op: CreateMaterializedViewOp) -> str:
    return f"op.create_materialized_view('{op.view_name}', '{op.definition}', properties={op.properties}, schema='{op.schema}')"


@renderers.dispatch_for(DropMaterializedViewOp)
def _drop_materialized_view(autogen_context: AutogenContext, op: DropMaterializedViewOp) -> str:
    return f"op.drop_materialized_view('{op.view_name}', schema='{op.schema}')"
