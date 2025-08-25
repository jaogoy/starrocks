from alembic.operations.base import Operations
from alembic.operations import ops
from starrocks.sql.ddl import AlterView, CreateView, DropView, CreateMaterializedView, DropMaterializedView
from starrocks.sql.schema import View, MaterializedView
from typing import Optional, Dict, Any

from sqlalchemy.sql.elements import quoted_name

@Operations.register_operation("alter_view")
class AlterViewOp(ops.MigrateOperation):
    """Represent an ALTER VIEW operation."""

    def __init__(
        self,
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        security: Optional[str] = None,
        reverse_view_definition: Optional[str] = None,
        reverse_view_comment: Optional[str] = None,
        reverse_view_security: Optional[str] = None,
    ):
        self.view_name = view_name
        self.definition = definition
        self.schema = schema
        self.comment = comment
        self.security = security
        self.reverse_view_definition = reverse_view_definition
        self.reverse_view_comment = reverse_view_comment
        self.reverse_view_security = reverse_view_security

    @classmethod
    def alter_view(
        cls,
        operations: "Operations",
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        security: Optional[str] = None,
        reverse_view_definition: Optional[str] = None,
        reverse_view_comment: Optional[str] = None,
        reverse_view_security: Optional[str] = None,
    ):
        """Invoke an ALTER VIEW operation."""
        op = cls(
            view_name,
            definition,
            schema=schema,
            comment=comment,
            security=security,
            reverse_view_definition=reverse_view_definition,
            reverse_view_comment=reverse_view_comment,
            reverse_view_security=reverse_view_security,
        )
        return operations.invoke(op)

    def reverse(self):
        # Reversing an ALTER is another ALTER, using the stored "reverse" attributes.
        return AlterViewOp(
            self.view_name,
            self.reverse_view_definition,
            schema=self.schema,
            comment=self.reverse_view_comment,
            security=self.reverse_view_security,
            reverse_view_definition=self.definition,
            reverse_view_comment=self.comment,
            reverse_view_security=self.security,
        )

@Operations.register_operation("create_view")
class CreateViewOp(ops.MigrateOperation):
    def __init__(
        self, 
        view_name: str, 
        definition: str, 
        schema: str | None = None, 
        security: str | None = None,
        comment: str | None = None
    ) -> None:
        self.view_name = view_name
        self.definition = definition
        self.schema = schema
        self.security = security
        self.comment = comment

    @classmethod
    def create_view(
        cls, 
        operations: Operations, 
        view_name: str, 
        definition: str, 
        schema: str | None = None, 
        security: str | None = None,
        comment: str | None = None
    ):
        op = cls(view_name, definition, schema=schema, security=security, comment=comment)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        return DropViewOp(
            self.view_name,
            schema=self.schema,
            _reverse_view_definition=self.definition,
            _reverse_view_comment=self.comment,
            _reverse_view_security=self.security,
        )

@Operations.register_operation("drop_view")
class DropViewOp(ops.MigrateOperation):
    def __init__(
        self,
        view_name: str,
        schema: Optional[str] = None,
        _reverse_view_definition: Optional[str] = None,
        _reverse_view_comment: Optional[str] = None,
        _reverse_view_security: Optional[str] = None,
    ):
        self.view_name = view_name
        self.schema = schema
        self._reverse_view_definition = _reverse_view_definition
        self._reverse_view_comment = _reverse_view_comment
        self._reverse_view_security = _reverse_view_security

    @classmethod
    def drop_view(cls, operations: Operations, view_name: str, schema: Optional[str] = None):
        op = cls(view_name, schema=schema)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        if self._reverse_view_definition is None:
            raise NotImplementedError("Cannot reverse a DropViewOp without the view's definition.")
        return CreateViewOp(
            self.view_name,
            self._reverse_view_definition,
            schema=self.schema,
            comment=self._reverse_view_comment,
            security=self._reverse_view_security,
        )

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


@Operations.implementation_for(AlterViewOp)
def alter_view(operations: "Operations", op: AlterViewOp):
    """Execute an ALTER VIEW statement."""
    view = View(
        name=op.view_name,
        definition=op.definition,
        schema=op.schema,
        comment=op.comment,
        security=op.security,
    )
    operations.execute(AlterView(view))

@Operations.implementation_for(CreateViewOp)
def create_view(operations: "Operations", op: CreateViewOp):
    """Execute a CREATE VIEW statement."""
    view = View(
        name=op.view_name,
        definition=op.definition,
        schema=op.schema,
        comment=op.comment,
        security=op.security,
    )
    operations.execute(CreateView(view))


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
