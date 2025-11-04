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

import logging
from typing import Any, Dict, List, Optional, Union

from sqlalchemy import Column, Table
from sqlalchemy.schema import MetaData
from sqlalchemy.sql.selectable import Selectable

from starrocks.common.params import TableKind, TableObjectInfoKey
from starrocks.datatype import STRING


logger = logging.getLogger(__name__)


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
        keep_existing: bool = False,
        extend_existing: bool = False,
        _no_init: bool = True,
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
                - Column objects: Column('id', Integer) as standard SQLAlchemy Column objects
                - Strings: 'id' (just column name)
                - Dicts: {'name': 'id', 'comment': 'User ID'}
            keep_existing: When True, if the view already exists in the MetaData,
                ignore further arguments and return the existing View object.
                This allows defining a view that may already be defined elsewhere.
            extend_existing: When True, if the view already exists in the MetaData,
                apply further arguments to update the existing View object.
                This allows modifying an existing view definition.
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

            # Update existing view definition
            View('v1', metadata,
                 definition='SELECT id, name FROM users',
                 comment='Updated view',
                 extend_existing=True)

            # Get existing view reference (or create if not exists)
            View('v1', metadata,
                 definition='SELECT * FROM users',
                 keep_existing=True)

        Notes:
            - StarRocks VIEW columns only support name and comment (not type/nullable)
            - Column types are automatically inferred from the SELECT statement, but useless now
            - Type parameter in Column() is a placeholder for SQLAlchemy compatibility
        """
        # Follow Table's pattern: skip initialization if _no_init=True
        # This happens when:
        # 1. Python automatically calls __init__ after __new__ returns (for existing tables)
        # 2. Getting an existing view without extend_existing
        if _no_init:
            return

        # Validate definition is provided
        if definition is None:
            raise ValueError("View definition is required. Use definition='SELECT ...' parameter.")

        # Prepare view-specific info
        view_info = {TableObjectInfoKey.TABLE_KIND: TableKind.VIEW}
        # Process definition (handles both str and Selectable)
        view_info.update(self._process_definition(definition))

        # Prepare view info for Table.__init__
        kwargs.setdefault("info", {}).update(view_info)

        # Convert simplified column definitions to Column objects
        normalized_columns = []
        if columns:
            normalized_columns.extend(self._normalize_columns(columns))
        # Merge with *args columns
        args = list(args) + normalized_columns

        # Let Table to handle comment, columns, and starrocks_* parameters
        # Pass keep_existing and extend_existing to Table for proper singleton behavior
        # logger.debug(f"View.__init__('{name}'): view_info={view_info}, kwargs['info'] id={id(kwargs.get('info'))}")
        super().__init__(name, metadata, *args, schema=schema, comment=comment,
                        keep_existing=keep_existing, extend_existing=extend_existing,
                        _no_init=False, **kwargs)
        # logger.debug(f"  After Table.__init__: self.info id={id(self.info)}, self.info={self.info}")

    def _init_existing(self, *args, **kwargs):
        """
        Override Table._init_existing to handle View-specific parameters.

        This is called when extend_existing=True and the view already exists in metadata.
        We need to extract View-specific parameters before passing to Table._init_existing.
        """
        # Extract View-specific parameters
        definition = kwargs.pop('definition', None)
        columns = kwargs.pop('columns', None)

        # Update view definition if provided
        if definition is not None:
            view_info = self._process_definition(definition)
            self.info.update(view_info)

        # Handle columns parameter (View-specific simplified syntax)
        # Convert to Column objects and prepend to args so Table._init_existing can process them
        if columns:
            normalized_columns = self._normalize_columns(columns)
            args = args + tuple(normalized_columns)

        super()._init_existing(*args, **kwargs)

    @staticmethod
    def _process_definition(definition: DefinitionType) -> Dict[str, Any]:
        """
        Process view definition and return view_info dict.

        This is a helper method to avoid code duplication between __init__ and _init_existing.

        Args:
            definition: SQL string or SQLAlchemy Selectable object

        Returns:
            Dict containing DEFINITION and optionally SELECTABLE keys

        Raises:
            TypeError: If definition is not str or Selectable
        """
        view_info = {}

        if isinstance(definition, str):
            view_info[TableObjectInfoKey.DEFINITION] = definition
        else:
            # Compile Selectable to SQL string
            from sqlalchemy.sql import ClauseElement

            if isinstance(definition, ClauseElement):
                compiled = definition.compile(compile_kwargs={"literal_binds": True})
                view_info[TableObjectInfoKey.DEFINITION] = str(compiled)
                view_info[TableObjectInfoKey.SELECTABLE] = definition
            else:
                raise TypeError(f"definition must be str or Selectable, got {type(definition)}")

        return view_info

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
        keep_existing: bool = False,
        extend_existing: bool = False,
        _no_init: bool = True,
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
            keep_existing: When True, if the materialized view already exists, ignore further
                arguments and return the existing object.
            extend_existing: When True, if the materialized view already exists, apply further
                arguments to update the existing object.
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
                        comment=comment, columns=columns,
                        keep_existing=keep_existing, extend_existing=extend_existing,
                        _no_init=_no_init, **kwargs)

        # Then, override the table_kind to be specific to MaterializedView.
        # Only do this if we're actually initializing (not skipping due to _no_init)
        if not _no_init:
            self.info[TableObjectInfoKey.TABLE_KIND] = TableKind.MATERIALIZED_VIEW

    @property
    def partition_by(self) -> Optional[str]:
        return self.dialect_options.get("starrocks", {}).get("partition_by")

    @property
    def refresh(self) -> Optional[str]:
        return self.dialect_options.get("starrocks", {}).get("refresh")
