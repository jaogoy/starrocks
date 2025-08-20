from alembic.operations import Operations
from alembic.operations import ops

@Operations.register_operation("create_view")
class CreateViewOp(ops.MigrateOperation):
    def __init__(self, view_name, definition, schema=None):
        self.view_name = view_name
        self.definition = definition
        self.schema = schema

    def reverse(self):
        return DropViewOp(self.view_name, schema=self.schema)

@Operations.register_operation("drop_view")
class DropViewOp(ops.MigrateOperation):
    def __init__(self, view_name, schema=None):
        self.view_name = view_name
        self.schema = schema

    def reverse(self):
        # The definition is not available here, so autogenerate
        # will typically produce a drop and a create for modifications.
        raise NotImplementedError("Cannot reverse a DropViewOp without the view's definition.")

@Operations.register_operation("create_materialized_view")
class CreateMaterializedViewOp(ops.MigrateOperation):
    def __init__(self, view_name, definition, properties=None, schema=None):
        self.view_name = view_name
        self.definition = definition
        self.properties = properties
        self.schema = schema

    def reverse(self):
        return DropMaterializedViewOp(self.view_name, schema=self.schema)


@Operations.register_operation("drop_materialized_view")
class DropMaterializedViewOp(ops.MigrateOperation):
    def __init__(self, view_name, schema=None):
        self.view_name = view_name
        self.schema = schema

    def reverse(self):
        raise NotImplementedError("Cannot reverse a DropMaterializedViewOp without the view's definition and properties.")
