from alembic.operations.base import Operations
from alembic.operations import ops
from starrocks.sql.ddl import CreateView, DropView, CreateMaterializedView, DropMaterializedView
from starrocks.sql.schema import View, MaterializedView
from typing import Any

@Operations.register_operation("create_view")
class CreateViewOp(ops.MigrateOperation):
    def __init__(self, view_name: str, definition: str, schema: str | None = None, security: str | None = None) -> None:
        self.view_name = view_name
        self.definition = definition
        self.schema = schema
        self.security = security

    @classmethod
    def create_view(cls, operations: Operations, view_name: str, definition: str, schema: str | None = None, security: str | None = None):
        op = cls(view_name, definition, schema=schema, security=security)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        return DropViewOp(self.view_name, schema=self.schema)

@Operations.register_operation("drop_view")
class DropViewOp(ops.MigrateOperation):
    def __init__(self, view_name: str, schema: str | None = None) -> None:
        self.view_name = view_name
        self.schema = schema

    @classmethod
    def drop_view(cls, operations, view_name: str, schema: str | None = None):
        op = cls(view_name, schema=schema)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        # The definition is not available here, so autogenerate
        # will typically produce a drop and a create for modifications.
        raise NotImplementedError("Cannot reverse a DropViewOp without the view's definition.")

@Operations.register_operation("create_materialized_view")
class CreateMaterializedViewOp(ops.MigrateOperation):
    def __init__(self, view_name: str, definition: str, properties: dict | None = None, schema: str | None = None) -> None:
        self.view_name = view_name
        self.definition = definition
        self.properties = properties
        self.schema = schema

    @classmethod
    def create_materialized_view(cls, operations, view_name: str, definition: str, properties: dict | None = None, schema: str | None = None):
        op = cls(view_name, definition, properties=properties, schema=schema)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        return DropMaterializedViewOp(self.view_name, schema=self.schema)


@Operations.register_operation("drop_materialized_view")
class DropMaterializedViewOp(ops.MigrateOperation):
    def __init__(self, view_name: str, schema: str | None = None) -> None:
        self.view_name = view_name
        self.schema = schema

    @classmethod
    def drop_materialized_view(cls, operations, view_name: str, schema: str | None = None):
        op = cls(view_name, schema=schema)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        raise NotImplementedError("Cannot reverse a DropMaterializedViewOp without the view's definition and properties.")


@Operations.implementation_for(CreateViewOp)
def create_view(operations: Operations, op: CreateViewOp) -> None:
    """Implementation for the 'create_view' operation."""
    operations.execute(CreateView(View(op.view_name, op.definition, schema=op.schema, security=op.security)))


@Operations.implementation_for(DropViewOp)
def drop_view(operations: Operations, op: DropViewOp) -> None:
    """Implementation for the 'drop_view' operation."""
    operations.execute(DropView(View(op.view_name, None, schema=op.schema)))

@Operations.implementation_for(CreateMaterializedViewOp)
def create_materialized_view(operations: Operations, op: CreateMaterializedViewOp) -> None:
    """Implementation for the 'create_materialized_view' operation."""
    operations.execute(
        CreateMaterializedView(
            MaterializedView(op.view_name, op.definition, properties=op.properties, schema=op.schema)
        )
    )

@Operations.implementation_for(DropMaterializedViewOp)
def drop_materialized_view(operations: Operations, op: DropMaterializedViewOp) -> None:
    """Implementation for the 'drop_materialized_view' operation."""
    operations.execute(
        DropMaterializedView(
            MaterializedView(op.view_name, None, schema=op.schema)
        )
    )
