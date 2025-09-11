import logging
from typing import Optional

from alembic.operations import ops
from alembic.operations.base import Operations

from starrocks.sql.ddl import (
    AlterView, CreateView, DropView, CreateMaterializedView, DropMaterializedView
)
from starrocks.sql.schema import View, MaterializedView


logger = logging.getLogger(__name__)


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
        operations: Operations,
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
        logger.debug("reverse AlterViewOp for %s", self.view_name)
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
        logger.debug("reverse CreateViewOp for %s", self.view_name)
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
        logger.debug("reverse DropViewOp for %s", self.view_name)
        return CreateViewOp(
            self.view_name,
            self._reverse_view_definition,
            schema=self.schema,
            comment=self._reverse_view_comment,
            security=self._reverse_view_security,
        )


@Operations.register_operation("alter_materialized_view")
class AlterMaterializedViewOp(ops.MigrateOperation):
    def __init__(self, view_name: str, definition: str, properties: dict | None = None, schema: str | None = None,
                 reverse_definition: str | None = None,
                 reverse_properties: dict | None = None
                 ) -> None:
        self.view_name = view_name
        self.definition = definition
        self.properties = properties
        self.schema = schema
        self.reverse_definition = reverse_definition
        self.reverse_properties = reverse_properties

    @classmethod
    def alter_materialized_view(cls, operations, view_name: str, definition: str, properties: dict | None = None, schema: str | None = None,
                                reverse_definition: str | None = None,
                                reverse_properties: dict | None = None):
        op = cls(view_name, definition, properties=properties, schema=schema,
                 reverse_definition=reverse_definition,
                 reverse_properties=reverse_properties)
        return operations.invoke(op)

    def reverse(self) -> ops.MigrateOperation:
        return AlterMaterializedViewOp(
            self.view_name,
            definition=self.reverse_definition,
            properties=self.reverse_properties,
            schema=self.schema,
            reverse_definition=self.definition,
            reverse_properties=self.properties
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
def alter_view(operations: Operations, op: AlterViewOp):
    """Execute an ALTER VIEW statement."""
    logger.debug("implementation alter_view: %s", op.view_name)
    view = View(
        name=op.view_name,
        definition=op.definition,
        schema=op.schema,
        comment=op.comment,
        security=op.security,
    )
    operations.execute(AlterView(view))


@Operations.implementation_for(CreateViewOp)
def create_view(operations: Operations, op: CreateViewOp):
    """Execute a CREATE VIEW statement."""
    logger.debug("implementation create_view: %s", op.view_name)
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
    logger.debug("implementation drop_view: %s", op.view_name)
    operations.execute(DropView(View(op.view_name, None, schema=op.schema)))


@Operations.register_operation("alter_table_properties")
class AlterTablePropertiesOp(ops.AlterTableOp):
    """Represent an ALTER TABLE SET (...) operation for StarRocks properties."""

    def __init__(
        self,
        table_name: str,
        properties: dict[str, str],
        schema: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.properties = properties

    @classmethod
    def alter_table_properties(
        cls,
        operations: Operations,
        table_name: str,
        properties: dict,
        schema: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE SET (...) operation for StarRocks properties."""
        op = cls(table_name, properties, schema=schema)
        return operations.invoke(op)


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


# Operation classes ordered according to StarRocks grammar:
# engine → key → (comment) → partition → distribution → order by → properties
@Operations.register_operation("alter_table_engine")
class AlterTableEngineOp(ops.AlterTableOp):
    """Represent an ALTER TABLE ENGINE operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        engine: str,
        schema: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.engine = engine

    @classmethod
    def alter_table_engine(
        cls,
        operations: Operations,
        table_name: str,
        engine: str,
        schema: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE ENGINE operation for StarRocks."""
        op = cls(table_name, engine, schema=schema)
        return operations.invoke(op)


@Operations.register_operation("alter_table_key")
class AlterTableKeyOp(ops.AlterTableOp):
    """Represent an ALTER TABLE KEY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        key_type: str,
        key_columns: str,
        schema: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.key_type = key_type
        self.key_columns = key_columns

    @classmethod
    def alter_table_key(
        cls,
        operations: Operations,
        table_name: str,
        key_type: str,
        key_columns: str,
        schema: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE KEY operation for StarRocks."""
        op = cls(table_name, key_type, key_columns, schema=schema)
        return operations.invoke(op)


@Operations.register_operation("alter_table_partition")
class AlterTablePartitionOp(ops.AlterTableOp):
    """Represent an ALTER TABLE PARTITION BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        partition_method: str,
        schema: Optional[str] = None,
    ):
        """
        Invoke an ALTER TABLE PARTITION BY operation for StarRocks.
        Args:
            table_name: The name of the table.
            partition_method: The method of the partition, such as 'RANGE(dt)', without pre-created partitions.
            schema: The schema of the table.
        """
        super().__init__(table_name, schema=schema)
        self.partition_method = partition_method
    
    @property
    def partition_by(self) -> str:
        """
        Get the partition by string for the ALTER TABLE PARTITION BY operation.
        It DOES NOT include the pre-created partitions.
        Because pre-created partitions should be created with ALTER TABLE ADD PARTITION operation.
        """
        return self.partition_method
        
    @classmethod
    def alter_table_partition(
        cls,
        operations: Operations,
        table_name: str,
        partition_method: str,
        schema: Optional[str] = None,
    ):
        """
        Invoke an ALTER TABLE PARTITION BY operation for StarRocks.
        The same as __init__ method.
        """
        op = cls(table_name, partition_method, schema=schema)
        return operations.invoke(op)


@Operations.register_operation("alter_table_distribution")
class AlterTableDistributionOp(ops.AlterTableOp):
    """Represent an ALTER TABLE DISTRIBUTED BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        distribution_method: str,
        buckets: Optional[int] = None,
        schema: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE DISTRIBUTED BY operation for StarRocks.
        Args:
            table_name: The name of the table.
            distribution_method: The method of the distribution, without BUCKETS.
            buckets: The buckets of the distribution.
            schema: The schema of the table.
        """
        super().__init__(table_name, schema=schema)
        self.distribution_method = distribution_method
        self.buckets = buckets

    @property
    def distributed_by(self) -> str:
        """Get the integrated distributed by string for the ALTER TABLE DISTRIBUTED BY operation.
        It includes the BUCKETS if it's not None.
        """
        return f"{self.distribution_method}{f' BUCKETS {self.buckets}' if self.buckets else ''}"

    @classmethod
    def alter_table_distribution(
        cls,
        operations: Operations,
        table_name: str,
        distribution_method: str,
        buckets: Optional[int] = None,
        schema: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE DISTRIBUTED BY operation for StarRocks.
        The same as __init__ method.
        """
        op = cls(table_name, distribution_method, buckets, schema=schema)
        return operations.invoke(op)


@Operations.register_operation("alter_table_order")
class AlterTableOrderOp(ops.AlterTableOp):
    """Represent an ALTER TABLE ORDER BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        order_by: str,
        schema: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.order_by = order_by

    @classmethod
    def alter_table_order(
        cls,
        operations: Operations,
        table_name: str,
        order_by: str,
        schema: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE ORDER BY operation for StarRocks."""
        op = cls(table_name, order_by, schema=schema)
        return operations.invoke(op)


# Implementation functions ordered according to StarRocks grammar:
# engine → key → (comment) → partition → distribution → order by → properties
@Operations.implementation_for(AlterTableEngineOp)
def alter_table_engine(operations, op: AlterTableEngineOp):
    logger.error(
        "ALTER TABLE ENGINE is not currently supported for StarRocks. "
        "Table: %s, Engine: %s", op.table_name, op.engine
    )
    raise NotImplementedError("ALTER TABLE ENGINE is not yet supported")


@Operations.implementation_for(AlterTableKeyOp)
def alter_table_key(operations, op: AlterTableKeyOp):
    logger.error(
        "ALTER TABLE KEY is not currently supported for StarRocks. "
        "Table: %s, Key Type: %s, Columns: %s",
        op.table_name, op.key_type, op.key_columns
    )
    raise NotImplementedError("ALTER TABLE KEY is not yet supported")


@Operations.implementation_for(AlterTablePartitionOp)
def alter_table_partition(operations, op: AlterTablePartitionOp):
    logger.error(
        "ALTER TABLE PARTITION is not currently supported for StarRocks. "
        "Table: %s, Partition: %s", op.table_name, op.partition_method
    )
    raise NotImplementedError("ALTER TABLE PARTITION is not yet supported")


@Operations.implementation_for(AlterTableDistributionOp)
def alter_table_distribution(operations, op: AlterTableDistributionOp):
    from starrocks.sql.ddl import AlterTableDistribution
    operations.execute(
        AlterTableDistribution(
            op.table_name,
            op.distribution_method,
            buckets=op.buckets,
            schema=op.schema
        )
    )


@Operations.implementation_for(AlterTableOrderOp)
def alter_table_order(operations, op: AlterTableOrderOp):
    from starrocks.sql.ddl import AlterTableOrder
    operations.execute(
        AlterTableOrder(op.table_name, op.order_by, schema=op.schema)
    )


@Operations.implementation_for(AlterTablePropertiesOp)
def alter_table_properties(operations, op: AlterTablePropertiesOp):
    from starrocks.sql.ddl import AlterTableProperties
    operations.execute(
        AlterTableProperties(op.table_name, op.properties, schema=op.schema)
    )
