from alembic.autogenerate import renderers
from .ops import CreateViewOp, DropViewOp, CreateMaterializedViewOp, DropMaterializedViewOp

@renderers.dispatch_for(CreateViewOp)
def _add_view(autogen_context, op):
    return f"op.create_view('{op.view_name}', '{op.definition}', schema='{op.schema}')"


@renderers.dispatch_for(DropViewOp)
def _drop_view(autogen_context, op):
    return f"op.drop_view('{op.view_name}', schema='{op.schema}')"


@renderers.dispatch_for(CreateMaterializedViewOp)
def _add_materialized_view(autogen_context, op):
    return f"op.create_materialized_view('{op.view_name}', '{op.definition}', properties={op.properties}, schema='{op.schema}')"


@renderers.dispatch_for(DropMaterializedViewOp)
def _drop_materialized_view(autogen_context, op):
    return f"op.drop_materialized_view('{op.view_name}', schema='{op.schema}')"
