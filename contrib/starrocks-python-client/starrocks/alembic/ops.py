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

from __future__ import annotations

import logging
from typing import Dict, List, Optional, Union

from alembic.operations import Operations, ops
from sqlalchemy import MetaData, Table

from starrocks.common.params import (
    TableInfoKey,
    TableObjectInfoKey,
)
from starrocks.common.utils import get_dialect_option
from starrocks.sql.schema import MaterializedView, View, extract_view_columns


logger = logging.getLogger(__name__)


@Operations.register_operation("alter_view")
class AlterViewOp(ops.MigrateOperation):
    """Represent an ALTER VIEW operation."""
    def __init__(
        self,
        view_name: str,
        definition: Optional[str] = None,
        schema: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        comment: Optional[str] = None,
        security: Optional[str] = None,
        reverse_definition: Optional[str] = None,
        reverse_columns: Union[List[Dict], None] = None,
        reverse_comment: Optional[str] = None,
        reverse_security: Optional[str] = None,
        **kwargs,
    ):
        """
        Definition usually should not be None. But for future, we may want to support Alter View by only comment or security,
        so we keep the definition as Noneable.
        """
        self.view_name = view_name
        self.definition = definition
        self.schema = schema
        self.columns = columns
        self.comment = comment
        self.security = security
        self.reverse_definition = reverse_definition
        self.reverse_columns = reverse_columns
        self.reverse_comment = reverse_comment
        self.reverse_security = reverse_security
        self.kwargs = kwargs

    def to_view(self, metadata: Optional[MetaData] = None) -> "View":
        return View(
            self.view_name,
            MetaData(),
            definition=self.definition or '',
            schema=self.schema,
            columns=self.columns,
            comment=self.comment,
            starrocks_security=self.security,
            **self.kwargs,
        )

    @classmethod
    def alter_view(
        cls,
        operations: Operations,
        view_name: str,
        definition: Optional[str] = None,
        schema: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        comment: Optional[str] = None,
        security: Optional[str] = None,
        reverse_definition: Optional[str] = None,
        reverse_columns: Union[List[Dict], None] = None,
        reverse_comment: Optional[str] = None,
        reverse_security: Optional[str] = None,
        **kwargs,
    ):
        """Invoke an ALTER VIEW operation."""
        op = cls(
            view_name,
            definition,
            schema=schema,
            columns=columns,
            comment=comment,
            security=security,
            reverse_definition=reverse_definition,
            reverse_columns=reverse_columns,
            reverse_comment=reverse_comment,
            reverse_security=reverse_security,
            **kwargs,
        )
        return operations.invoke(op)

    def reverse(self) -> "AlterViewOp":
        # Reversing an ALTER is another ALTER, using the stored "reverse" attributes.
        logger.debug("reverse AlterViewOp for %s", self.view_name)

        return AlterViewOp(
            self.view_name,
            definition=self.reverse_definition,
            schema=self.schema,
            columns=self.reverse_columns,
            comment=self.reverse_comment,
            security=self.reverse_security,
            reverse_definition=self.definition,
            reverse_columns=self.columns,
            reverse_comment=self.comment,
            reverse_security=self.security,
            **self.kwargs,
        )

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"AlterViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"definition=({self.definition!r}), columns={self.columns!r}, comment={self.comment!r}, "
            f"security={self.security}, "
            f"reverse_definition=({self.reverse_definition}), "
            f"reverse_columns={self.reverse_columns!r}, "
            f"reverse_comment={self.reverse_comment!r}, "
            f"reverse_security={self.reverse_security})"
        )


@Operations.register_operation("create_view")
class CreateViewOp(ops.MigrateOperation):
    def __init__(
        self,
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        comment: Optional[str] = None,
        security: Optional[str] = None,
        or_replace: bool = False,
        if_not_exists: bool = False,
        **kwargs,
    ) -> None:
        self.view_name = view_name
        self.definition = definition
        self.schema = schema
        self.columns = columns
        self.comment = comment
        self.security = security
        self.or_replace = or_replace
        self.if_not_exists = if_not_exists
        self.kwargs = kwargs

    def to_view(self, metadata: Optional[MetaData] = None) -> "View":
        return View(
            self.view_name,
            MetaData(),
            definition=self.definition,
            schema=self.schema,
            columns=self.columns,
            comment=self.comment,
            starrocks_security=self.security,
            **self.kwargs,
        )

    @classmethod
    def from_view(cls, view: Table) -> "CreateViewOp":
        """Create Op from a View object (which is a Table)."""
        # Use case-insensitive lookup for security (handles both user-created and reflected views)
        security = get_dialect_option(view.dialect_options, TableInfoKey.SECURITY)
        logger.debug(f"CreateViewOp.from_view: view_name={view.name}, security={security}")
        return cls(
            view.name,
            definition=view.info.get(TableObjectInfoKey.DEFINITION),
            schema=view.schema,
            columns=extract_view_columns(view),
            comment=view.comment,
            security=security,
        )

    @classmethod
    def create_view(
        cls,
        operations: Operations,
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        comment: Optional[str] = None,
        security: Optional[str] = None,
        or_replace: bool = False,
        if_not_exists: bool = False,
        **kwargs,
    ) -> None:
        op = cls(
            view_name,
            definition,
            schema=schema,
            columns=columns,
            comment=comment,
            security=security,
            or_replace=or_replace,
            if_not_exists=if_not_exists,
            **kwargs,
        )
        return operations.invoke(op)

    def reverse(self) -> "DropViewOp":
        # logger.debug("reverse CreateViewOp for %s", self.view_name)
        return DropViewOp(
            self.view_name,
            schema=self.schema,
            reverse_definition=self.definition,
            reverse_columns=self.columns,
            reverse_comment=self.comment,
            reverse_security=self.security,
            **self.kwargs,
        )

    def __str__(self) -> str:
        return (
            f"CreateViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"definition=({self.definition!r}), columns={self.columns!r}, comment={self.comment!r}, "
            f"security={self.security}, or_replace={self.or_replace}, "
            f"if_not_exists={self.if_not_exists}, kwargs=({self.kwargs!r})"
        )


@Operations.register_operation("drop_view")
class DropViewOp(ops.MigrateOperation):
    def __init__(
        self,
        view_name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        reverse_definition: Optional[str] = None,
        reverse_columns: Optional[List[Dict]] = None,
        reverse_comment: Optional[str] = None,
        reverse_security: Optional[str] = None,
    ):
        self.view_name = view_name
        self.schema = schema
        self.if_exists = if_exists
        self.reverse_definition = reverse_definition
        self.reverse_columns = reverse_columns
        self.reverse_comment = reverse_comment
        self.reverse_security = reverse_security

    def to_view(self, metadata: Optional[MetaData] = None) -> "View":
        """Create a View object for DROP VIEW operation.

        This is used by toimpl to construct the DROP VIEW DDL statement.
        For DROP VIEW, we only need the view name and schema, not the full definition.
        """
        return View(
            self.view_name,
            MetaData(),
            definition='',  # Empty definition for DROP VIEW
            schema=self.schema,
        )

    @classmethod
    def from_view(cls, view: Table) -> "DropViewOp":
        """Create DropViewOp from a View object (which is a Table)."""
        # Use case-insensitive lookup for security (handles both user-created and reflected views)
        security = get_dialect_option(view.dialect_options, TableInfoKey.SECURITY)
        return cls(
            view.name,
            schema=view.schema,
            reverse_definition=view.info.get(TableObjectInfoKey.DEFINITION),
            reverse_columns=extract_view_columns(view),
            reverse_comment=view.comment,
            reverse_security=security,
        )

    @classmethod
    def drop_view(
        cls,
        operations: Operations,
        view_name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        **kwargs,
    ):
        op = cls(view_name, schema=schema, if_exists=if_exists, **kwargs)
        return operations.invoke(op)

    def reverse(self) -> "CreateViewOp":
        if self.reverse_definition is None:
            raise NotImplementedError("Cannot reverse a DropViewOp without the view's definition.")
        op = CreateViewOp(
            self.view_name,
            definition=self.reverse_definition,
            schema=self.schema,
            columns=self.reverse_columns,
            comment=self.reverse_comment,
            security=self.reverse_security,
        )
        logger.debug("reverse DropViewOp for %s, with op: (%s)", self.view_name, op)
        return op

    def __str__(self) -> str:
        return (
            f"DropViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"if_exists={self.if_exists}, reverse_definition=({self.reverse_definition!r}), "
            f"reverse_comment=({self.reverse_comment!r}), "
            f"reverse_security={self.reverse_security}, "
            f"reverse_columns={self.reverse_columns!r})"
        )


@Operations.register_operation("alter_materialized_view")
class AlterMaterializedViewOp(ops.MigrateOperation):
    def __init__(self, view_name: str, definition: str, properties: Union[dict, None] = None, schema: Union[str, None] = None,
                 reverse_definition: Union[str, None] = None,
                 reverse_properties: Union[dict, None] = None
                 ) -> None:
        self.view_name = view_name
        self.definition = definition
        self.properties = properties
        self.schema = schema
        self.reverse_definition = reverse_definition
        self.reverse_properties = reverse_properties

    @classmethod
    def alter_materialized_view(cls, operations, view_name: str, definition: str, properties: Union[dict, None] = None, schema: Union[str, None] = None,
                                reverse_definition: Union[str, None] = None,
                                reverse_properties: Union[dict, None] = None):
        op = cls(view_name, definition, properties=properties, schema=schema,
                 reverse_definition=reverse_definition,
                 reverse_properties=reverse_properties)
        return operations.invoke(op)

    def reverse(self) -> "AlterMaterializedViewOp":
        return AlterMaterializedViewOp(
            self.view_name,
            definition=self.reverse_definition,
            properties=self.reverse_properties,
            schema=self.schema,
            reverse_definition=self.definition,
            reverse_properties=self.properties
        )

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"AlterMaterializedViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"definition=({self.definition}), properties={self.properties!r}, "
            f"reverse_definition=({self.reverse_definition!r}), "
            f"reverse_properties={self.reverse_properties!r})"
        )


@Operations.register_operation("create_materialized_view")
class CreateMaterializedViewOp(CreateViewOp):
    def __init__(
        self,
        view_name: str,
        definition: str,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        columns: Union[List[Dict], None] = None,
        or_replace: bool = False,
        if_not_exists: bool = False,
        **kwargs,
    ):
        super().__init__(
            view_name,
            definition,
            schema=schema,
            comment=comment,
            columns=columns,
            or_replace=or_replace,
            if_not_exists=if_not_exists,
            **kwargs,
        )

    def to_materialized_view(self, metadata: Optional[MetaData] = None) -> "MaterializedView":
        return MaterializedView(
            self.view_name,
            MetaData(),
            definition=self.definition,
            schema=self.schema,
            comment=self.comment,
            columns=self.columns,
        )

    @classmethod
    def from_materialized_view(cls, mv: Table) -> "CreateMaterializedViewOp":
        """Create Op from a MaterializedView object (which is a Table)."""
        return cls(
            mv.name,
            mv.info.get(TableObjectInfoKey.DEFINITION),
            schema=mv.schema,
            comment=mv.comment,
            **mv.kwargs,
        )

    @classmethod
    def create_materialized_view(cls, operations, view_name: str, definition: str, schema: Union[str, None] = None, if_not_exists: bool = False, **kw):
        op = cls(view_name, definition, schema=schema, if_not_exists=if_not_exists, **kw)
        return operations.invoke(op)

    def reverse(self) -> "DropMaterializedViewOp":
        return DropMaterializedViewOp(
            self.view_name,
            schema=self.schema,
        )

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"CreateMaterializedViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"definition=({self.definition}), comment={self.comment!r}, "
            f"if_not_exists={self.if_not_exists}, kw=({self.kw!r})"
        )


@Operations.register_operation("drop_materialized_view")
class DropMaterializedViewOp(DropViewOp):
    def __init__(
        self,
        view_name: str,
        schema: Optional[str] = None,
        if_exists: bool = False,
        reverse_definition: Optional[str] = None,
        reverse_columns: Optional[List[Dict]] = None,
        reverse_comment: Optional[str] = None,
        reverse_security: Optional[str] = None,
        reverse_properties: Optional[Dict[str, str]] = None,
    ) -> None:
        # Call parent constructor with common reverse_* parameters
        super().__init__(
            view_name,
            schema=schema,
            if_exists=if_exists,
            reverse_definition=reverse_definition,
            reverse_columns=reverse_columns,
            reverse_comment=reverse_comment,
            reverse_security=reverse_security,
        )
        self.reverse_properties = reverse_properties

    @classmethod
    def drop_materialized_view(cls, operations, view_name: str, schema: Union[str, None] = None, if_exists: bool = False):
        op = cls(view_name, schema=schema, if_exists=if_exists)
        return operations.invoke(op)

    def reverse(self) -> "CreateMaterializedViewOp":
        if self.reverse_definition is None:
            raise NotImplementedError("Cannot reverse a DropMaterializedViewOp without the view's definition.")
        # Note: reverse_properties are stored in reverse_ attributes inherited from DropViewOp
        return CreateMaterializedViewOp(
            self.view_name,
            definition=self.reverse_definition,
            schema=self.schema,
            comment=self.reverse_comment,
            columns=self.reverse_columns,
        )

    def __str__(self) -> str:
        """String representation for debugging."""
        return (
            f"DropMaterializedViewOp(view_name={self.view_name!r}, schema={self.schema!r}, "
            f"if_exists={self.if_exists}, reverse_definition=({self.reverse_definition}), "
            f"reverse_properties=({self.reverse_properties!r})"
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
        reverse_engine: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.engine = engine
        self.reverse_engine = reverse_engine

    @classmethod
    def alter_table_engine(
        cls,
        operations: Operations,
        table_name: str,
        engine: str,
        schema: Optional[str] = None,
        reverse_engine: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE ENGINE operation for StarRocks."""
        op = cls(table_name, engine, schema=schema, reverse_engine=reverse_engine)
        return operations.invoke(op)

    def reverse(self) -> AlterTableEngineOp:
        if self.reverse_engine is None:
            raise NotImplementedError("Cannot reverse AlterTableEngineOp without reverse_engine")
        return AlterTableEngineOp(
            table_name=self.table_name,
            engine=self.reverse_engine,
            schema=self.schema,
            reverse_engine=self.engine,
        )


@Operations.register_operation("alter_table_key")
class AlterTableKeyOp(ops.AlterTableOp):
    """Represent an ALTER TABLE KEY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        key_type: str,
        key_columns: str,
        schema: Optional[str] = None,
        reverse_key_type: Optional[str] = None,
        reverse_key_columns: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.key_type = key_type
        self.key_columns = key_columns
        self.reverse_key_type = reverse_key_type
        self.reverse_key_columns = reverse_key_columns

    @classmethod
    def alter_table_key(
        cls,
        operations: Operations,
        table_name: str,
        key_type: str,
        key_columns: str,
        schema: Optional[str] = None,
        reverse_key_type: Optional[str] = None,
        reverse_key_columns: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE KEY operation for StarRocks."""
        op = cls(
            table_name,
            key_type,
            key_columns,
            schema=schema,
            reverse_key_type=reverse_key_type,
            reverse_key_columns=reverse_key_columns,
        )
        return operations.invoke(op)

    def reverse(self) -> "AlterTableKeyOp":
        if self.reverse_key_type is None or self.reverse_key_columns is None:
            raise NotImplementedError("Cannot reverse AlterTableKeyOp without reverse_key_type and reverse_key_columns")
        return AlterTableKeyOp(
            table_name=self.table_name,
            key_type=self.reverse_key_type,
            key_columns=self.reverse_key_columns,
            schema=self.schema,
            reverse_key_type=self.key_type,
            reverse_key_columns=self.key_columns,
        )


@Operations.register_operation("alter_table_partition")
class AlterTablePartitionOp(ops.AlterTableOp):
    """Represent an ALTER TABLE PARTITION BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        partition_method: str,
        schema: Optional[str] = None,
        reverse_partition_method: Optional[str] = None,
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
        self.reverse_partition_method = reverse_partition_method

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
        reverse_partition_method: Optional[str] = None,
    ):
        """
        Invoke an ALTER TABLE PARTITION BY operation for StarRocks.
        The same as __init__ method.
        """
        op = cls(table_name, partition_method, schema=schema, reverse_partition_method=reverse_partition_method)
        return operations.invoke(op)

    def reverse(self) -> "AlterTablePartitionOp":
        if self.reverse_partition_method is None:
            raise NotImplementedError("Cannot reverse AlterTablePartitionOp without reverse_partition_method")
        return AlterTablePartitionOp(
            table_name=self.table_name,
            partition_method=self.reverse_partition_method,
            schema=self.schema,
            reverse_partition_method=self.partition_method,
        )


@Operations.register_operation("alter_table_distribution")
class AlterTableDistributionOp(ops.AlterTableOp):
    """Represent an ALTER TABLE DISTRIBUTED BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        distribution_method: str,
        buckets: Optional[int] = None,
        schema: Optional[str] = None,
        reverse_distribution_method: Optional[str] = None,
        reverse_buckets: Optional[int] = None,
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
        self.reverse_distribution_method = reverse_distribution_method
        self.reverse_buckets = reverse_buckets

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
        reverse_distribution_method: Optional[str] = None,
        reverse_buckets: Optional[int] = None,
    ):
        """Invoke an ALTER TABLE DISTRIBUTED BY operation for StarRocks.
        The same as __init__ method.
        """
        op = cls(
            table_name,
            distribution_method,
            buckets,
            schema=schema,
            reverse_distribution_method=reverse_distribution_method,
            reverse_buckets=reverse_buckets,
        )
        return operations.invoke(op)

    def reverse(self) -> "AlterTableDistributionOp":
        if self.reverse_distribution_method is None:
            raise NotImplementedError("Cannot reverse AlterTableDistributionOp without reverse_distribution_method")
        return AlterTableDistributionOp(
            table_name=self.table_name,
            distribution_method=self.reverse_distribution_method,
            buckets=self.reverse_buckets,
            schema=self.schema,
            reverse_distribution_method=self.distribution_method,
            reverse_buckets=self.buckets,
        )


@Operations.register_operation("alter_table_order")
class AlterTableOrderOp(ops.AlterTableOp):
    """Represent an ALTER TABLE ORDER BY operation for StarRocks."""

    def __init__(
        self,
        table_name: str,
        order_by: Union[str, List[str]],
        schema: Optional[str] = None,
        reverse_order_by: Optional[str] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.order_by = order_by
        self.reverse_order_by = reverse_order_by

    @classmethod
    def alter_table_order(
        cls,
        operations: Operations,
        table_name: str,
        order_by: str,
        schema: Optional[str] = None,
        reverse_order_by: Optional[str] = None,
    ):
        """Invoke an ALTER TABLE ORDER BY operation for StarRocks."""
        op = cls(table_name, order_by, schema=schema, reverse_order_by=reverse_order_by)
        return operations.invoke(op)

    def reverse(self) -> "AlterTableOrderOp":
        if self.reverse_order_by is None:
            raise NotImplementedError("Cannot reverse AlterTableOrderOp without reverse_order_by")
        return AlterTableOrderOp(
            table_name=self.table_name,
            order_by=self.reverse_order_by,
            schema=self.schema,
            reverse_order_by=self.order_by,
        )



@Operations.register_operation("alter_table_properties")
class AlterTablePropertiesOp(ops.AlterTableOp):
    """Represent an ALTER TABLE SET (...) operation for StarRocks properties."""

    def __init__(
        self,
        table_name: str,
        properties: Dict[str, str],
        schema: Optional[str] = None,
        reverse_properties: Optional[Dict[str, str]] = None,
    ):
        super().__init__(table_name, schema=schema)
        self.properties = properties
        self.reverse_properties = reverse_properties

    @classmethod
    def alter_table_properties(
        cls,
        operations: Operations,
        table_name: str,
        properties: dict,
        schema: Optional[str] = None,
        reverse_properties: Optional[Dict[str, str]] = None,
    ):
        """Invoke an ALTER TABLE SET (...) operation for StarRocks properties."""
        op = cls(table_name, properties, schema=schema, reverse_properties=reverse_properties)
        return operations.invoke(op)

    def reverse(self) -> "AlterTablePropertiesOp":
        if self.reverse_properties is None:
            raise NotImplementedError("Cannot reverse AlterTablePropertiesOp without reverse_properties")
        return AlterTablePropertiesOp(
            table_name=self.table_name,
            properties=self.reverse_properties,
            schema=self.schema,
            reverse_properties=self.properties,
        )
