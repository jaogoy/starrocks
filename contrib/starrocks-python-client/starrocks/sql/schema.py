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

from typing import Any, Dict, List, Optional, Union

from sqlalchemy import Column, Table
from sqlalchemy.schema import MetaData
from sqlalchemy.sql.selectable import Selectable

from starrocks.common.params import TableKind, TableObjectInfoKey
from starrocks.datatype import STRING


DefinitionType = Union[str, "Selectable"]
ColumnDefinition = Union[Column, str, Dict[str, Any]]


def extract_view_columns(table: Table) -> Union[List[Dict[str, Any]], None]:
    """
    Extract column information from a View/Table object for serialization.

    This function works with both:
    - View objects (user-defined in metadata)
    - Table objects representing views (reflected from database)

    This is the inverse operation of View._normalize_columns():
    - _normalize_columns: dict/str -> Column objects (for View creation)
    - extract_view_columns: Column objects -> dict (for Alembic operations)

    Args:
        table: A View or Table object with columns

    Returns:
        List of dicts (``{"name": str, "comment": str}``), or None if no columns

    Note:
        StarRocks VIEW columns only support name and comment (not type/nullable).
        This is used by Alembic operations to serialize view columns for migration scripts.

    Example:
        >>> view = View('v1', metadata, Column('id', STRING(), comment='ID'))
        >>> extract_view_columns(view)
        [{'name': 'id', 'comment': 'ID'}]

        >>> # Also works with reflected Table objects
        >>> reflected_table = metadata.tables['my_view']
        >>> extract_view_columns(reflected_table)
        [{'name': 'col1', 'comment': 'Comment'}]
    """
    if not table.columns:
        return None
    return [
        {'name': col.name, 'comment': col.comment}
        for col in table.columns
    ]


class View(Table):
    """Represents a View object."""

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args,
        definition: Optional[DefinitionType] = None,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        columns: Optional[List[ColumnDefinition]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Create a View object.

        Args:
            name: Name of the view
            metadata: MetaData object to bind the view to
            *args: Column objects (optional). If provided, these columns will be used
                   to define the view's schema explicitly. This is useful for
                   Alembic autogenerate to compare column changes.
            definition: SQL string or SQLAlchemy Selectable object defining the view query (required)
            schema: Schema name (optional)
            comment: View comment (optional)
            columns: List of column definitions (optional). Can be:
                - Column objects: Column('id', Integer)
                - Strings: 'id' (just column name)
                - Dicts: {'name': 'id', 'comment': 'User ID'}
            **kwargs: Additional keyword arguments, including:
                - starrocks_security: Security mode (INVOKER or NONE). Note: DEFINER is not supported by StarRocks.
                - Other dialect-specific parameters with starrocks_ prefix for future use

        Examples:
            # String definition without explicit columns
            View('v1', metadata, definition='SELECT * FROM users')

            # With Column objects (standard way)
            View('v1', metadata,
                 Column('id', STRING()),
                 Column('name', STRING(), comment='User name'),
                 definition='SELECT id, name FROM users')

            # With simplified column definitions
            View('v1', metadata,
                 definition='SELECT id, name FROM users',
                 columns=['id', {'name': 'name', 'comment': 'User name'}])

            # Selectable (type-safe) definition
            stmt = select(users.c.id, users.c.name)
            View('v1', metadata, definition=stmt)

            # With security and comment
            View('v1', metadata,
                 definition='SELECT * FROM users',
                 comment='User view',
                 starrocks_security='INVOKER')

        Notes:
            - StarRocks VIEW columns only support name and comment (not type/nullable)
            - Column types are automatically inferred from the SELECT statement, but useless now
            - Type parameter in Column() is a placeholder for SQLAlchemy compatibility
        """
        # Validate definition is provided
        if definition is None:
            raise ValueError("View definition is required. Use definition='SELECT ...' parameter.")

        # Set up info dict with view-specific metadata
        info = kwargs.setdefault("info", {})
        info[TableObjectInfoKey.TABLE_KIND] = TableKind.VIEW

        # Handle both str and Selectable definitions
        if isinstance(definition, str):
            info[TableObjectInfoKey.DEFINITION] = definition
        else:
            # Compile Selectable to SQL string
            from sqlalchemy.sql import ClauseElement

            if isinstance(definition, ClauseElement):
                # TODO: Figure out how to get the dialect from the metadata
                compiled = definition.compile(compile_kwargs={"literal_binds": True})
                info[TableObjectInfoKey.DEFINITION] = str(compiled)
                info[TableObjectInfoKey.SELECTABLE] = definition  # Keep original for reference
            else:
                raise TypeError(f"definition must be str or Selectable, got {type(definition)}")

        # Convert simplified column definitions to Column objects
        normalized_columns = []
        if columns:
            normalized_columns.extend(self._normalize_columns(columns))

        # Merge with *args columns
        all_columns = list(args) + normalized_columns

        # Call Table.__init__, which automatically handles comment, columns, and starrocks_* parameters
        # The all_columns will be passed to Table as Column objects
        super().__init__(name, metadata, *all_columns, schema=schema, comment=comment, **kwargs)

    @staticmethod
    def _normalize_columns(columns: List[ColumnDefinition]) -> List[Column]:
        """
        Convert simplified column definitions to Column objects.

        Args:
            columns: List of column definitions (Column, str, or dict)

        Returns:
            List of Column objects

        Raises:
            TypeError: If column definition is invalid
        """
        result = []
        for col_def in columns:
            if isinstance(col_def, Column):
                # Already a Column object
                result.append(col_def)
            elif isinstance(col_def, str):
                # String: just column name
                result.append(Column(col_def, STRING()))
            elif isinstance(col_def, dict):
                # Dict: name + optional comment
                if 'name' not in col_def:
                    raise ValueError(f"Column dict must have 'name' key: {col_def}")
                result.append(Column(
                    col_def['name'],
                    STRING(),
                    comment=col_def.get('comment')
                ))
            else:
                raise TypeError(
                    f"Invalid column definition: {col_def}. "
                    f"Expected Column object, str, or dict with 'name' key."
                )
        return result

    @property
    def definition(self) -> str:
        return self.info.get(TableObjectInfoKey.DEFINITION, "")

    @property
    def selectable(self) -> Optional["Selectable"]:
        """Get original Selectable object if created from one"""
        return self.info.get(TableObjectInfoKey.SELECTABLE, None)

    @property
    def security(self) -> Optional[str]:
        return self.dialect_options.get("starrocks", {}).get("security")


class MaterializedView(View):
    """Represents a Materialized View object in Python."""

    def __init__(
        self,
        name: str,
        metadata: MetaData,
        *args,
        definition: Optional[DefinitionType] = None,
        schema: Optional[str] = None,
        comment: Optional[str] = None,
        columns: Optional[List[ColumnDefinition]] = None,
        **kwargs: Any,
    ) -> None:
        """
        Create a Materialized View object.

        Args:
            name: Name of the materialized view
            metadata: MetaData object to bind the materialized view to
            *args: Column objects (optional)
            definition: SQL string or SQLAlchemy Selectable object defining the MV query (required)
            schema: Schema name (optional)
            comment: Materialized view comment (optional)
            columns: List of column definitions (optional), same format as View
            **kwargs: Additional keyword arguments, including:
                - starrocks_partition_by: Partition expression
                - starrocks_refresh: Refresh mode (ASYNC or MANUAL)
                - starrocks_distributed_by: Distribution method
                - starrocks_order_by: Order by columns
                - starrocks_properties: Additional properties
                - Other dialect-specific parameters with starrocks_ prefix

        Examples:
            # Basic materialized view
            MaterializedView('mv1', metadata, definition='SELECT * FROM users')

            # With partition and refresh
            MaterializedView('mv1', metadata,
                           definition='SELECT * FROM users',
                           starrocks_partition_by='date_trunc("day", created_at)',
                           starrocks_refresh='ASYNC')

            # With columns
            MaterializedView('mv1', metadata,
                           definition='SELECT id, name FROM users',
                           columns=['id', {'name': 'name', 'comment': 'User name'}])
        """
        # First, call the parent View's __init__ to handle the definition
        # and other common parameters.
        super().__init__(name, metadata, *args, definition=definition, schema=schema,
                        comment=comment, columns=columns, **kwargs)

        # Then, override the table_kind to be specific to MaterializedView.
        self.info[TableObjectInfoKey.TABLE_KIND] = TableKind.MATERIALIZED_VIEW

    @property
    def partition_by(self) -> Optional[str]:
        return self.dialect_options.get("starrocks", {}).get("partition_by")

    @property
    def refresh(self) -> Optional[str]:
        return self.dialect_options.get("starrocks", {}).get("refresh")
