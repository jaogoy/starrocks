#! /usr/bin/python3
# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https:#www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
import re
from textwrap import dedent
import logging
from typing import Any, Dict, Final, Optional, List, Set

from alembic.ddl.base import format_column_name, format_table_name
from sqlalchemy import Column, Connection, Table, exc, schema as sa_schema, util, log, text, Row

from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql

from sqlalchemy.dialects.mysql.base import (
    MySQLDDLCompiler,
    MySQLTypeCompiler,
    MySQLCompiler,
    MySQLIdentifierPreparer,
    _DecodingRow,
)
from sqlalchemy.orm import properties
from sqlalchemy.sql import quoted_name, sqltypes
from sqlalchemy.sql.expression import Delete, Select
from sqlalchemy.engine import reflection

from starrocks.types import ColumnAggType, SystemRunMode, TableType
from starrocks.utils import CaseInsensitiveDict, TableAttributeNormalizer

from .datatype import (
    TINYINT, SMALLINT, INTEGER, BIGINT, LARGEINT, BOOLEAN,
    DECIMAL, DOUBLE, FLOAT,
    DATETIME, DATE,
    CHAR, VARCHAR, STRING, BINARY, VARBINARY,
    HLL, BITMAP, PERCENTILE, 
    ARRAY, MAP, STRUCT, JSON
)
from . import reflection as _reflection
from .sql.ddl import (
    AlterMaterializedView, CreateView, DropView, AlterView, CreateMaterializedView, DropMaterializedView,
    AlterTableEngine,
    AlterTableKey,
    AlterTablePartition,
    AlterTableDistribution,
    AlterTableOrder,
    AlterTableProperties,
)
from .sql.schema import View
from .reflection import StarRocksInspector
from .engine.interfaces import ReflectedViewState, ReflectedPartitionInfo, ReflectedState
from .defaults import ReflectionViewDefaults
from .params import ColumnAggInfoKey, ColumnSROptionsKey, DialectName, SRKwargsPrefix, TableInfoKey, TableInfoKeyWithPrefix, ColumnAggInfoKeyWithPrefix
from alembic.operations.ops import AlterColumnOp

# Register the compiler methods
# The @compiles decorator is the public API for registering new SQL constructs.
# However, we are now using the internal `__visit_name__` attribute on the
# DDLElement classes themselves to hook into the visitor pattern, which is
# consistent with how SQLAlchemy's own constructs are implemented.
# compiles(CreateView)(StarRocksDDLCompiler.visit_create_view)
# compiles(DropView)(StarRocksDDLCompiler.visit_drop_view)
# compiles(CreateMaterializedView)(StarRocksDDLCompiler.visit_create_materialized_view)
# compiles(DropMaterializedView)(StarRocksDDLCompiler.visit_drop_materialized_view)


##############################################################################################
# NOTES - INCOMPLETE/UNFINISHED
# There are a number of items in here marked as ToDo
# In terms of table creation, the Partition, Distribution and OrderBy clauses need to be addressed from table options
# Tests `test_has_index` and `test_has_index_schema` are failing, this is because the CREATE INDEX statement appears to
# work async and only when it's finished does it appear in the table definition
# Other tests are failing, need to fix or figure out how to suppress
# Review some skipped test suite requirements
#
#
#
##############################################################################################

logger = logging.getLogger(__name__)

# starrocks supported data types
ischema_names = {
    # === Boolean ===
    "boolean": BOOLEAN,
    # === Integer ===
    "tinyint": TINYINT,
    "smallint": SMALLINT,
    "int": INTEGER,
    "bigint": BIGINT,
    "largeint": LARGEINT,
    # === Floating-point ===
    "float": FLOAT,
    "double": DOUBLE,
    # === Fixed-precision ===
    "decimal": DECIMAL,
    "decimal32": DECIMAL,
    "decimal64": DECIMAL,
    "decimal128": DECIMAL,
    # === String ===
    "varchar": VARCHAR,
    "char": CHAR,
    "string": STRING,
    "json": JSON,
    # === Date and time ===
    "date": DATE,
    "datetime": DATETIME,
    "timestamp": DATETIME,
    # == binary ==
    "binary": BINARY,
    "varbinary": VARBINARY,
    # === Structural ===
    "array": ARRAY,
    "map": MAP,
    "struct": STRUCT,
    "hll": HLL,
    "percentile": PERCENTILE,
    "bitmap": BITMAP,
}


class StarRocksTypeCompiler(MySQLTypeCompiler):

    def visit_BOOLEAN(self, type_, **kw):
        return "BOOLEAN"

    def visit_FLOAT(self, type_, **kw):
        return "FLOAT"

    def visit_TINYINT(self, type_, **kw):
        return "TINYINT"

    def visit_SMALLINT(self, type_, **kw):
        return "SMALLINT"

    def visit_INTEGER(self, type_, **kw):
        return "INTEGER"

    def visit_BIGINT(self, type_, **kw):
        return "BIGINT"

    def visit_LARGEINT(self, type_, **kw):
        return "LARGEINT"

    def visit_STRING(self, type_, **kw):
        # return "STRING"
        return "VARCHAR(65533)"

    def visit_BINARY(self, type_, **kw):
        return "BINARY"

    def visit_VARBINARY(self, type_, **kw):
        return "VARBINARY"

    def visit_ARRAY(self, type_, **kw):
        """Compiles the ARRAY type into the correct StarRocks syntax."""
        inner_type_sql = self.process(type_.item_type, **kw)
        return f"ARRAY<{inner_type_sql}>"

    def visit_MAP(self, type_, **kw):
        return "MAP<keytype,valuetype>"

    def visit_STRUCT(self, type_, **kw):
        return "STRUCT<name, type>"

    def visit_HLL(self, type_, **kw):
        return "HLL"

    def visit_BITMAP(self, type_, **kw):
        return "BITMAP"


class StarRocksSQLCompiler(MySQLCompiler):
    def visit_delete(self, delete_stmt: Delete, **kw: Any) -> str:
        result: str = super().visit_delete(delete_stmt, **kw)
        compile_state: Any = delete_stmt._compile_state_factory(
            delete_stmt, self, **kw
        )
        delete_stmt = compile_state.statement
        table: str = self.delete_table_clause(
            delete_stmt, delete_stmt.table, False
        )
        if not delete_stmt._where_criteria:
            return "TRUNCATE TABLE " + table
        return result

    def limit_clause(self, select: Select, **kw: Any) -> str:
        # StarRocks supports:
        #   LIMIT <limit>
        #   LIMIT <limit> OFFSET <offset>
        text = ""
        if select._limit_clause is not None:
            text += "\n LIMIT " + self.process(select._limit_clause, **kw)
        if select._offset_clause is not None:
            # if select._limit_clause is None:
            #     text += "\n LIMIT -1"
            text += " OFFSET " + self.process(select._offset_clause, **kw)
        return text


class StarRocksDDLCompiler(MySQLDDLCompiler):
    def __init__(self, *args, **kwargs):
        self.indent: Final[str] = "    "
        super().__init__(*args, **kwargs)

    def visit_alter_column(self, alter: AlterColumnOp, **kw):
        """
        Compile ALTER COLUMN statements.

        StarRocks does not support altering the aggregation type of a column.
        This method checks for such attempts and raises a NotImplementedError.
        """
        # The `starrocks_AGG_TYPE` is added to kwargs only when a change is detected
        # by the comparator. See `compare_starrocks_column_agg_type`.
        if ColumnAggInfoKeyWithPrefix.AGG_TYPE in alter.kwargs:
            raise NotImplementedError(
                f"StarRocks does not support changing the aggregation type of column '{alter.column_name}'."
            )
        return super().visit_alter_column(alter, **kw)

    def visit_create_table(self, create: sa_schema.CreateTable, **kw: Any) -> str:
        table = create.element
        preparer = self.preparer

        text = "\nCREATE "
        if table._prefixes:
            text += " ".join(table._prefixes) + " "

        text += "TABLE "
        if create.if_not_exists:
            text += "IF NOT EXISTS "

        text += preparer.format_table(table) + " "

        # StarRocks-specific validation for all key types
        self._validate_key_definitions(table)

        if create_table_suffix := self.create_table_suffix(table):
            text += create_table_suffix + " "

        text += f"(\n{self.indent}"

        column_text_list = list()
        # if only one primary key, specify it along with the column
        primary_keys: List[str] = list()
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(create_column)
                # logger.debug(f"column desc for column: {column.name} is '{processed}'")
                if processed is not None:
                    column_text_list.append(processed)
                if column.primary_key:
                    primary_keys.append(column.name)
            except exc.CompileError as ce:
                raise exc.CompileError(
                    "(in table '%s', column '%s'): %s"
                    % (self._get_simple_full_table_name(table.name, table.schema), column.name, ce.args[0])
                ) from ce
        text += f", \n{self.indent}".join(column_text_list)

        # N.B. Primary Key is specified in post_create_table
        #  Indexes are created by SQLA after the creation of the table using CREATE INDEX
        # const = self.create_table_constraints(
        #     table,
        #     _include_foreign_key_constraints=create.include_foreign_key_constraints,  # noqa
        # )
        # if const:
        #     text += separator + self.indent + const

        text += "\n)\n%s\n" % self.post_create_table(table, primary_keys=primary_keys, **kw)
        logger.debug(f"create table text for table: {table.name}, schema: {table.schema}, text: {text}")

        return text
    
    def _get_simple_full_table_name(self, table_name: str, schema: Optional[str] = None) -> str:
        """Directly get the full table name without adding any quotes."""
        return f"{schema}.{table_name}" if schema else table_name

    def _validate_key_definitions(self, table: sa_schema.Table) -> None:
        """
        Validates key definitions for all StarRocks table types.

        This performs two checks:
        1. (All key types) Ensures that all columns specified in the table's key
           (e.g., `starrocks_primary_key`) are actually defined in the table.
        2. (AGGREGATE KEY only) Enforces StarRocks' strict column ordering rules.

        Args:
            table: The SQLAlchemy Table object to validate.

        Raises:
            CompileError: If any validation rule is violated.
        """
        key_kwarg_map = TableInfoKeyWithPrefix.KEY_KWARG_MAP

        key_str = None
        key_type = None

        for kwarg, name in key_kwarg_map.items():
            if kwarg in table.kwargs:
                key_str = table.kwargs[kwarg]
                key_type = name
                break  # Found the key, no need to check for others

        if not key_str:
            return  # No key defined, nothing to validate

        key_column_names = [k.strip() for k in key_str.split(',')]
        table_column_names = {c.name for c in table.columns}

        # 1. Generic Check: Ensure all key columns exist in the table definition.
        missing_keys = set(key_column_names) - table_column_names
        if missing_keys:
            raise exc.CompileError(
                f"Columns specified in {key_type} ('{key_str}') not found in table: {', '.join(missing_keys)}"
            )

        # 2. Specific Check: For AGGREGATE KEY tables, validate column order.
        if key_type == TableType.AGGREGATE_KEY:
            self._validate_aggregate_key_order(table, key_column_names)

    def _validate_aggregate_key_order(self, table: sa_schema.Table, key_column_names: List[str]) -> None:
        """
        Validates column order for AGGREGATE KEY tables.

        In StarRocks, for an AGGREGATE KEY table:
        1. All key columns must be defined before any value (aggregate) columns.
        2. The order of key columns in the table definition must match the order
           specified in the `starrocks_aggregate_key` argument.

        Args:
            table: The SQLAlchemy Table object to validate.
            key_column_names: The list of key column names from the kwarg.
        """
        key_cols_from_table = []

        # Separate table columns into key and value lists
        for col in table.columns:
            # Note: value columns are any columns not in the key list.
            if col.name in key_column_names:
                key_cols_from_table.append(col.name)

        # Rule 2: The order of key columns in the table definition must match
        if key_cols_from_table != key_column_names:
            raise exc.CompileError(
                "For AGGREGATE KEY tables, the order of key columns in the table definition "
                f"must match the order in starrocks_aggregate_key. "
                f"Expected order: {key_column_names}, Actual order: {key_cols_from_table}"
            )

        # Rule 1: All key columns must be defined before any value columns.
        last_key_col_index = -1
        col_list = list(table.columns)
        for i, col in enumerate(col_list):
            if col.name in key_column_names:
                last_key_col_index = i

        for i in range(last_key_col_index):
            if col_list[i].name not in key_column_names:
                raise exc.CompileError(
                    "For AGGREGATE KEY tables, all key columns must be defined before any value columns. "
                    f"Value column '{col_list[i].name}' appears before "
                    f"key column '{col_list[last_key_col_index].name}'."
                )

    def post_create_table(self, table: sa_schema.Table, **kw: Any) -> str:
        """
        Builds table-level clauses for a CREATE TABLE statement.

        This method compiles StarRocks-specific table options provided as `starrocks_`
        kwargs on the `Table` object. It is responsible for constructing clauses
        that appear after the column definitions, such as:
        - `ENGINE`
        - `KEY`: `PRIMARY KEY`, `DUPLICATE KEY`, `AGGREGATE KEY`, `UNIQUE KEY`
        - `COMMENT`
        - `PARTITION BY`
        - `DISTRIBUTED BY`
        - `ORDER BY`
        - `PROPERTIES`

        Args:
            table: The `sqlalchemy.schema.Table` object being compiled.
            **kw: Additional keyword arguments from the compiler.
                primary_keys: The list of primary key column names. We need to check it with a talbe's KEY attribute.

        Returns:
            A string containing all the compiled table-level DDL clauses.
        """

        table_opts: list[str] = []

        # Extract StarRocks-specific table options from dialect_options without the prefix `starrocks_`.
        # opts: dict[str, Any] = self._extract_table_options(table)
        # And item with value being None should be removed, because the `defaults` has all the keys.
        opts = {k.upper(): v for k, v in table.dialect_options[DialectName].items() if v is not None}
        logger.debug(f"table original opts for table: {table.name}, schema: {table.schema}, opts: {opts!r}")

        # ENGINE
        if engine := opts.get(TableInfoKey.ENGINE):
            table_opts.append(f'ENGINE={engine}')

        # Key / Table Type (Primary key, Duplicate key, Aggregate key, Unique key)
        primary_keys = kw.get("primary_keys", [])
        if key_desc := self._get_create_table_key_desc(primary_keys, opts):
            table_opts.append(key_desc)

        # Comment
        if table.comment is not None:
            opts[TableInfoKey.COMMENT] = table.comment
        if comment := opts.get(TableInfoKey.COMMENT):
            comment = self.sql_compiler.render_literal_value(comment, sqltypes.String())
            table_opts.append(f"COMMENT {comment}")

        # Partition
        # TODO: there are 3 types of partitioning with different restrictions, we need to support all of them.
        if partition_by := opts.get(TableInfoKey.PARTITION_BY):
            table_opts.append(f'PARTITION BY {partition_by}')

        # Distribution
        if distributed_by := opts.get(TableInfoKey.DISTRIBUTED_BY):
            table_opts.append(f'DISTRIBUTED BY {distributed_by}')

        # Order By
        if order_by := opts.get(TableInfoKey.ORDER_BY):
            order_by = TableAttributeNormalizer.remove_outer_parentheses(order_by)
            table_opts.append(f'ORDER BY ({order_by})')

        # Properties
        if properties := opts.get(TableInfoKey.PROPERTIES):
            if isinstance(properties, dict):
                props_items = properties.items()
            elif isinstance(properties, list):
                props_items = properties
            else:
                raise exc.CompileError(
                    f"Unsupported type for PROPERTIES: {type(properties)}"
                )

            props = ",\n".join([f'{self.indent}"{k}"="{v}"' for k, v in props_items])
            table_opts.append(f"PROPERTIES(\n{props}\n)")

        logger.debug(f"table opts for table: {table.name}, schema: {table.schema}, processed opts: {table_opts!r}")

        return "\n".join(table_opts)
    
    def _extract_table_options(self, table: sa_schema.Table) -> dict[str, Any]:
        """Extract table options, with the prefix `starrocks_` removed.
        It seems useless. Because we retrieve the options from dialect_options 
        by removing all the defaults with value 'None'.
        """
        opts: dict[str, Any] = dict(
            (k[len(SRKwargsPrefix):].upper(), v) for k, v in table.dialect_kwargs.items()
                if k.startswith(SRKwargsPrefix)
        )
        logger.debug(f"extract table options from table: {table.name}, schema: {table.schema}, options: {opts}")
        for k in opts.keys():
            if k not in TableInfoKey.ALL:
                raise exc.CompileError(f"Unsupported table option: {k}")

        return opts

    def _get_create_table_key_desc(self, primary_keys: List[str], opts: dict[str, Any]) -> Optional[str]:
        """Visit create table key description.
        Args:
            primary_keys: The list of primary key column names. We need to check it with a talbe's KEY attribute.
            opts: The table options.

        Returns:
            The table key description. like "PRIMARY KEY (id, name)"
        """
        # Key / Table Type (Primary key, Duplicate key, Aggregate key, Unique key)
        key_type = None
        key_desc = None
        for tbl_type_key_str, table_type in TableInfoKey.KEY_KWARG_MAP.items():
            kwarg_upper = tbl_type_key_str.upper()
            if kwarg_upper in opts:
                if key_type:
                    raise exc.CompileError(f"Multiple key types found: {tbl_type_key_str}, first_key_type: {key_type}")
                key_type = table_type
                key_columns_str: str = TableAttributeNormalizer.remove_outer_parentheses(opts[kwarg_upper])
                logger.debug(f"get table key info: key_type: {key_type}, key_columns: {key_columns_str}")
                # check if the key columns are valid
                if primary_keys:
                    key_columns_set = set(k.strip().strip('`') for k in key_columns_str.split(','))
                    primary_keys_set = set(k.strip().strip('`') for k in primary_keys)
                    logger.debug(f"check constraint keys. primary_key_set: {primary_keys}, key_columns_set: [{key_columns_str}]")
                    if primary_keys_set != key_columns_set:
                        raise exc.CompileError(f"Primary key columns doesn't equal to the table KEY columns. "
                                            f"primary_keys: {primary_keys}, SR's key_columns: ({key_columns_str})")
                key_desc = f"{key_type} ({key_columns_str})"
        return key_desc


    def _has_column_info_key(self, column: sa_schema.Column, key: str) -> bool:
        """Check if column has a specific info key (case-insensitive)."""
        return any(k.lower() == key.lower() for k in column.info.keys())
    
    def _get_column_info_value(self, column: sa_schema.Column, key: str, default: Any = None) -> Any:
        """Get column info value by key (case-insensitive)."""
        for k, v in column.info.items():
            if k.lower() == key.lower():
                return v
        return default

    def get_column_specification(self, column: sa_schema.Column, **kw: Any) -> str:
        """Builds column DDL for StarRocks, handling StarRocks-specific features.

        This method extends the base MySQL compiler to support:
        - **KEY specifier**: For AGGREGATE KEY tables, key columns can be marked
          with `info={'starrocks_is_agg_key': True}`. The compiler validates that
          a column is not both a key and an aggregate.
        - **Aggregate Functions**: For AGGREGATE KEY tables, value columns can have
          an aggregate function (e.g., 'SUM', 'REPLACE') specified via the
          `info={'starrocks_agg': '...'}` dictionary on a Column.
        - **AUTO_INCREMENT**: Automatically renders `AUTO_INCREMENT` for columns
          with `autoincrement=True`. It also ensures these columns are `BIGINT`
          and `NOT NULL` as required by StarRocks.
        - **Generated Columns**: Compiles `sqlalchemy.Computed` constructs into
          StarRocks' `AS (...)` syntax.

        Args:
            column: The `sqlalchemy.schema.Column` object to process.
            **kw: Additional keyword arguments from the compiler.

        Returns:
            The full DDL string for the column definition.
        """
        # Name, type, others of a column for the output colspec
        _, idx_type = 0, 1

        # Get name and type first
        colspec: list[str] = [
            self.preparer.format_column(column),
            self.dialect.type_compiler.process(
                column.type, type_expression=column
            ),
        ]

        # Get and set column-level aggregate information
        # logger.debug(f"get column agg info for column: {column.name}")
        if agg_info := self._get_column_agg_info(column):
            logger.debug(f"agg info for column: {column.name} is '{agg_info}'")
            colspec.append(agg_info)

        # NULL or NOT NULL. AUTO_INCREMENT columns must be NOT NULL
        if not column.nullable or column.autoincrement is True:
            colspec.append("NOT NULL")
        # else: omit explicit NULL (default)

        # AUTO_INCREMENT or default value or computed column
        if column.autoincrement is True:
            colspec[idx_type] = "BIGINT"  # AUTO_INCREMENT column must be BIGINT
            colspec.append("AUTO_INCREMENT")
        else:
            default = self.get_column_default_string(column)
            if default == "AUTO_INCREMENT":
                colspec[1] = "BIGINT"
                colspec.append("AUTO_INCREMENT")

            elif default is not None:
                colspec.append("DEFAULT " + default)
        
        # Computed
        if column.computed is not None:
            colspec.append(self.process(column.computed))

        # Comment
        if column.comment is not None:
            literal = self.sql_compiler.render_literal_value(
                column.comment, sqltypes.String()
            )
            colspec.append("COMMENT " + literal)

        return " ".join(colspec)

    def _get_column_agg_info(self, column: sa_schema.Column) -> str | None:
        """Get aggregate information for a column.
        Args:
            column: The `sqlalchemy.schema.Column` object to process.

        Returns:
            The aggregate information for the column (`KEY` or `agg_type`, such as `SUM`, or None).
        """
        
        table = column.table

        # Determine whether the target table is an AGGREGATE KEY table.
        # In CREATE TABLE, table.dialect_options will contain StarRocks options.
        # In ALTER TABLE ADD/MODIFY COLUMN, Alembic/SQLAlchemy often provides a lightweight
        # Table placeholder without dialect options; in that case we treat the table type
        # as unknown and avoid raising on presence of KEY/agg markers so users can specify
        # them explicitly in ADD/MODIFY statements.
        is_agg_table: bool | None = None
        try:
            is_agg_table = table.info.get(TableInfoKeyWithPrefix.AGGREGATE_KEY)
            # logger.debug(f"Cached is_agg_table for column: {column.name} is '{is_agg_table}'")
        except Exception:
            # Defensive: if table/info is not accessible, keep unknown
            is_agg_table = None

        if is_agg_table is None:
            try:
                # Remove items with value being None, because the `defaults` has all the keys.
                table_opt_upper_keys: set[str] = {k.upper() for k, v in table.dialect_options[DialectName].items() 
                    if v is not None
                }
                if table_opt_upper_keys:
                    is_agg_table = TableInfoKey.AGGREGATE_KEY in table_opt_upper_keys
                    table.info[TableInfoKeyWithPrefix.AGGREGATE_KEY] = is_agg_table
                    # logger.debug(f"Cache is_agg_table for column: {column.name} to '{is_agg_table}'")
                else:
                    # Unknown table options in ALTER context; leave as None
                    is_agg_table = None
            except Exception:
                # No dialect options available; leave as unknown
                is_agg_table = None

        # check agg key/type in the column
        opt_dict: dict[str, Any] = {k.upper(): v for k, v in column.dialect_options[DialectName].items() if v is not None}
        has_is_agg_key = ColumnAggInfoKey.IS_AGG_KEY in opt_dict
        has_agg_type = ColumnAggInfoKey.AGG_TYPE in opt_dict

        # If we can determine the table is NOT AGGREGATE KEY, disallow column-level
        # KEY/agg markers. If unknown (ALTER context), allow rendering markers.
        if is_agg_table is False and (has_is_agg_key or has_agg_type):
            raise exc.CompileError(
                "Column-level KEY/aggregate markers are only valid for AGGREGATE KEY tables; "
                "declare starrocks_aggregate_key at table level first."
            )

        # Disallow specifying both KEY and agg_type simultaneously.
        if has_is_agg_key and has_agg_type:
            raise exc.CompileError(
                f"Column '{column.name}' cannot be both KEY and aggregated "
                f"(has {ColumnAggInfoKey.AGG_TYPE})."
            )

        if has_is_agg_key:
            return ColumnAggType.KEY
        elif has_agg_type:
            agg_val = str(opt_dict[ColumnAggInfoKey.AGG_TYPE]).upper()
            if agg_val not in ColumnAggType.ALLOWED_ITEMS:
                raise exc.CompileError(
                    f"Unsupported aggregate type for column '{column.name}': {agg_val}"
                )
            return agg_val
        return None

    def visit_computed_column(self, generated: sa_schema.Computed, **kw: Any) -> str:
        text = "AS %s" % self.sql_compiler.process(
            generated.sqltext, include_table=False, literal_binds=True
        )
        return text

    def visit_set_table_comment(self, create: sa_schema.SetTableComment, **kw: Any) -> str:
        return "ALTER TABLE %s COMMENT=%s" % (
            self.preparer.format_table(create.element),
            self.sql_compiler.render_literal_value(
                create.element.comment, sqltypes.String()
            ),
        )

    def visit_drop_table_comment(self, create: sa_schema.DropTableComment, **kw: Any) -> str:
        return "ALTER TABLE %s COMMENT=''" % (
            self.preparer.format_table(create.element)
        )

    def visit_alter_view(self, alter: AlterView, **kw: Any) -> str:
        view = alter.element
        text = f"ALTER VIEW {self.preparer.format_table(view)}\n"

        if view.columns:
            text += self._get_view_column_clauses(view)

        # StarRocks does not support altering COMMENT or SECURITY via ALTER VIEW.
        # TODO: we can optimize it when StarRocks supports it in the future
        # Only redefine the SELECT statement.
        text += f"AS\n{view.definition}"

        logger.debug("Compiled SQL for AlterView: \n%s", text)
        return text

    def _get_view_column_clauses(self, view: View) -> str:
        """Helper method to format the column clauses for a CREATE VIEW statement."""
        column_clauses: list[str] = []
        for c in view.columns:
            if isinstance(c, dict):
                col_name: str = self.preparer.quote(c['name'])
                if 'comment' in c:
                    comment: str = self.sql_compiler.render_literal_value(
                        c['comment'], sqltypes.String()
                    )
                    column_clauses.append(f'{self.indent}{col_name} COMMENT {comment}')
                else:
                    column_clauses.append(f'{self.indent}{col_name}')
            else:
                column_clauses.append(f'{self.indent}{self.preparer.quote(c)}')
        return " (\n%s\n)" % ",\n".join(column_clauses)

    def visit_create_view(self, create: CreateView, **kw: Any) -> str:
        view = create.element
        text = "CREATE "
        if create.or_replace:
            text += "OR REPLACE "
        text += "VIEW "
        if create.if_not_exists:
            text += "IF NOT EXISTS "

        text += self.preparer.format_table(view) + "\n"

        if view.columns:
            text += self._get_view_column_clauses(view)

        if view.comment:
            comment = self.sql_compiler.render_literal_value(
                view.comment, sqltypes.String()
            )
            text += f"COMMENT {comment}\n"

        if create.security:
            text += f"SECURITY {create.security.upper()}\n"

        text += f"AS\n{view.definition}"

        logger.debug("Compiled SQL for CreateView: \n%s", text)
        return text

    def visit_drop_view(self, drop: DropView, **kw: Any) -> str:
        view = drop.element
        return f"DROP VIEW IF EXISTS {self.preparer.format_table(view)}"

    def visit_alter_materialized_view(self, alter: AlterMaterializedView, **kw: Any) -> str:
        mv = alter.element
        return f"ALTER MATERIALIZED VIEW {self.preparer.format_table(mv)} {alter.alter_type} {alter.alter_by}"

    def visit_create_materialized_view(self, create: CreateMaterializedView, **kw: Any) -> str:
        mv = create.element
        properties = ""
        if mv.properties:
            prop_clauses: list[str] = [
                f'"{k}" = "{v}"' for k, v in mv.properties.items()
            ]
            properties = f"PROPERTIES ({', '.join(prop_clauses)})"

        return (
            f"CREATE MATERIALIZED VIEW {self.preparer.format_table(mv)} "
            f"{properties} AS {mv.definition}"
        )

    def visit_drop_materialized_view(self, drop: DropMaterializedView, **kw: Any) -> str:
        mv = drop.element
        return f"DROP MATERIALIZED VIEW IF EXISTS {self.preparer.format_table(mv)}"

    # Visit methods ordered according to StarRocks grammar:
    # engine → key → partition → distribution → order by → properties

    def visit_alter_table_engine(self, alter: AlterTableEngine, **kw: Any) -> str:
        """Compile ALTER TABLE ENGINE DDL for StarRocks.
        Not supported in StarRocks.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        return f"ALTER TABLE {table_name} ENGINE = {alter.engine}"

    def visit_alter_table_key(self, alter: AlterTableKey, **kw: Any) -> str:
        """Compile ALTER TABLE KEY DDL for StarRocks.
        Not supported in StarRocks yet.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        return f"ALTER TABLE {table_name} {alter.key_type} KEY ({alter.key_columns})"

    def visit_alter_table_partition(self, alter: AlterTablePartition, **kw: Any) -> str:
        """Compile ALTER TABLE PARTITION BY DDL for StarRocks.
        Not supported in StarRocks yet.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        return f"ALTER TABLE {table_name} PARTITION BY {alter.partition_by}"

    def visit_alter_table_distribution(self, alter: AlterTableDistribution, **kw: Any) -> str:
        """Compile ALTER TABLE DISTRIBUTED BY DDL for StarRocks."""
        # TODO:
        table_name = format_table_name(self, alter.table_name, alter.schema)
        distribution_clause = f"DISTRIBUTED BY {alter.distribution_method}"
        if alter.buckets is not None:
            distribution_clause += f" BUCKETS {alter.buckets}"
        return f"ALTER TABLE {table_name} {distribution_clause}"

    def visit_alter_table_order(self, alter: AlterTableOrder, **kw: Any) -> str:
        """Compile ALTER TABLE ORDER BY DDL for StarRocks."""

        table_name = format_table_name(self, alter.table_name, alter.schema)
        return f"ALTER TABLE {table_name} ORDER BY {alter.order_by}"

    def visit_alter_table_properties(self, alter: AlterTableProperties, **kw: Any) -> str:
        """Compile ALTER TABLE SET (...) DDL for StarRocks.

        Note:
            Currently, SR only support one property in an ALTER TABLE SET statement.
            So, we will generate multiple ALTER TABLE SET statements if there are multiple properties.
        """
        table_name = format_table_name(self, alter.table_name, alter.schema)
        # logger.debug(f"ALTER TABLE '{table_name}' SET ({alter.properties})")
        
        # Escape double quotes in property values
        def escape_value(value: str) -> str:
            return value.replace('"', '\\"')
        
        multi_set_statement = "; ".join([f'ALTER TABLE {table_name} SET ("{k}" = "{escape_value(v)}")' for k, v in alter.properties.items()])
        logger.debug(f"Compiled SQL for AlterTableProperties: \n{multi_set_statement}")
        return multi_set_statement


class StarRocksIdentifierPreparer(MySQLIdentifierPreparer):
    """
    We can add some starrocks specific identifier behavior here if needed.

    Currently, we don't force to use quote for identifier.

    for reserved words, we can use the same as MySQL, and add some starrocks specific reserved words later.
    such as: reserved_words = STARROCKS_RESERVED_WORDS | RESERVED_WORDS_MYSQL
    """

    def __init__(self, dialect):
        super().__init__(dialect, initial_quote='`')

    # We don't force to use quote for identifier. we can uncomment this if needed.
    # def _requires_quotes(self, ident):
    #     return True


@log.class_logger
class StarRocksDialect(MySQLDialect_pymysql):
    # Dialect name
    name: Final[str] = "starrocks"

    # Supported/Permitted StarRocks's dialect construct arguments for Table and Column
    # Supports both lower and upper case variants for the arguments (eaiser for users' usages).
    construct_arguments = [
        (Table, {variant: None for k in TableInfoKey.ALL for variant in (k.lower(), k.upper())}),
        (Column, {variant: None for k in ColumnAggInfoKey.ALL for variant in (k.lower(), k.upper())}),
    ]

    # Used to get the partition info from the SHOW CREATE TABLE statement
    # Use regex to find the PARTITION BY clause. It can be multi-line.
    _PARTITION_BY_PATTERN = re.compile(r"PARTITION BY\s+(.*?)(?:\s+DISTRIBUTED BY|\s+ORDER BY|\s+PROPERTIES|;|$)", re.DOTALL | re.IGNORECASE)

    # Caching
    # Warnings are generated by SQLAlchemy if this flag is not explicitly set
    # and tests are needed before being enabled
    supports_statement_cache = True
    supports_server_side_cursors = False
    supports_empty_insert = False

    ischema_names = ischema_names
    inspector = StarRocksInspector

    statement_compiler = StarRocksSQLCompiler
    ddl_compiler = StarRocksDDLCompiler
    type_compiler = StarRocksTypeCompiler
    preparer = StarRocksIdentifierPreparer

    def __init__(self, *args, **kwargs):
        super(StarRocksDialect, self).__init__(*args, **kwargs)
        self.run_mode: Optional[str] = None
        # Explicitly instantiate the preparer here, ensuring it's an instance
        self.preparer = self.preparer(self)

    def initialize(self, connection: Connection) -> None:
        super().initialize(connection)
        if self.run_mode is None:
            self.run_mode = self._get_run_mode(connection)

    def _get_server_version_info(self, connection: Connection) -> tuple[int, ...]:
        # get database server version info explicitly over the wire
        # to avoid proxy servers like MaxScale getting in the
        # way with their own values, see #4205
        dbapi_con = connection.connection
        cursor = dbapi_con.cursor()
        cursor.execute("SELECT CURRENT_VERSION()")
        val = cursor.fetchone()[0]
        cursor.close()
        if isinstance(val, bytes):
            val = val.decode()

        return self._parse_server_version(val)

    def _parse_server_version(self, val: str) -> tuple[int, ...]:
        server_version_info: tuple[int, ...] = tuple()
        m = re.match(r"(\d+)\.?(\d+)?(?:\.(\d+))?(?:\.\d+)?(?:[-\s])?(?P<commit>.*)?", val)
        if m is not None:
            server_version_info = tuple([int(x) for x in m.group(1, 2, 3) if x is not None])

        # setting it here to help w the test suite
        self.server_version_info = server_version_info
        return server_version_info

    def _get_run_mode(self, connection: Connection) -> str:
        """Get the StarRocks system run_mode (shared_data or shared_nothing).
        
        Args:
            connection: The SQLAlchemy connection object.
            
        Returns:
            The run_mode as a string ('shared_data' or 'shared_nothing').
            
        Raises:
            exc.DBAPIError: If the query fails.
        """
        try:
            result = connection.execute(text("ADMIN SHOW FRONTEND CONFIG LIKE 'run_mode'"))
            rows = result.fetchall()
            if rows and len(rows) > 0:
                # The result format is: | Key | AliasNames | Value | Type | IsMutable | Comment |
                return rows[0][2]  # Value column
            else:
                # Default to shared_nothing if not found
                return SystemRunMode.SHARED_NOTHING
        except exc.DBAPIError as e:
            # Log the error but don't fail the entire operation
            logger.warning(f"Failed to get run_mode: {e}")
            # Default to shared_nothing if query fails
            return SystemRunMode.SHARED_NOTHING

    @util.memoized_property
    def _tabledef_parser(self) -> _reflection.StarRocksTableDefinitionParser:
        """return the StarRocksTableDefinitionParser, generate if needed.

        The deferred creation ensures that the dialect has
        retrieved server version information first.

        """
        preparer = self.identifier_preparer
        return _reflection.StarRocksTableDefinitionParser(self, preparer)

    def _read_from_information_schema(
        self, connection: Connection, inf_sch_table: str, charset: Optional[str] = None, **kwargs: Any
    ) -> list[_DecodingRow]:
        def escape_single_quote(s: str) -> str:
            return s.replace("'", "\\'")
        
        st: str = dedent(f"""
            SELECT * 
            FROM information_schema.{inf_sch_table} 
            WHERE {" AND ".join([f"{k} = '{escape_single_quote(v)}'"
                                 for k, v in kwargs.items()
                                 if k and v])}
        """)
        # logger.debug(f"query for information_schema.{inf_sch_table}: {st}")
        rp: Any = None
        try:
            rp = connection.execution_options(
                skip_user_error_events=False
            ).exec_driver_sql(st)
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) == 1146:
                raise exc.NoSuchTableError(
                    f"information_schema.{inf_sch_table}"
                ) from e
            else:
                raise
        rows: list[_DecodingRow] = [_DecodingRow(
            row, charset) for row in rp.mappings().fetchall()]
        if not rows:
            raise exc.NoSuchTableError(
                f"Empty response for query: '{st}'"
            )
        return rows
    
    @reflection.cache
    def _setup_parser(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> ReflectedState:
        charset: Optional[str] = self._connection_charset
        parser: _reflection.StarRocksTableDefinitionParser = self._tabledef_parser

        if not schema:
            schema = connection.dialect.default_schema_name

        table_rows: list[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="tables",
            charset=charset,
            table_schema=schema,
            table_name=table_name,
        )
        if not table_rows:
            raise exc.NoSuchTableError(table_name)
        if len(table_rows) > 1:
            raise exc.InvalidRequestError(
                f"Multiple tables found with name {table_name} in schema {schema}"
            )
        logger.debug(f"reflected table row for table: {table_name}, info: {dict(table_rows[0])}")

        table_config_rows: list[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="tables_config",
            charset=charset,
            table_schema=schema,
            table_name=table_name,
        )
        if len(table_config_rows) > 1:
            raise exc.InvalidRequestError(
                f"Multiple tables found with name {table_name} in schema {schema}"
            )
        table_config_row = table_config_rows[0]
        logger.debug(f"reflected table config for table: {table_name}, table_config: {dict(table_config_row)}")

        column_rows: list[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="columns",
            charset=charset,
            table_schema=schema,
            table_name=table_name,
        )

        # Get aggregate info from `SHOW FULL COLUMNS`
        full_column_rows: list[Row] = self._get_show_full_columns(
            connection, table_name=table_name, schema=schema
        )
        column_2_agg_type: dict[str, str] = {
            row.Field: row.Extra.upper()
            for row in full_column_rows
            if row.Extra
        }

        partition_clause = self._get_partition_clause_from_create_table(connection, table_name, schema)
        # Add the partition info into table_config row for convenience
        # But the row object is immutable, so we convert it to a dictionary to modify it.
        table_config_dict = dict(table_config_rows[0])
        if partition_clause:
            table_config_dict['PARTITION_CLAUSE'] = partition_clause

        return parser.parse(
            table=table_rows[0],
            table_config=table_config_dict,
            columns=column_rows,
            column_2_agg_type=column_2_agg_type,
            charset=charset,
        )

    def _get_quote_full_table_name(
        self, table_name: str, schema: Optional[str] = None
    ) -> str:
        """Get the fully quoted table name."""
        full_table_name = self.preparer.quote_identifier(str(table_name))
        if schema:
            full_table_name = f"{self.preparer.quote_identifier(str(schema))}.{full_table_name}"
        return full_table_name

    def _get_partition_clause_from_create_table(self, connection: Connection, table_name: str,
                                                schema: Optional[str] = None) -> Optional[str]:
        """
        Get the PARTITION BY clause from the SHOW CREATE TABLE statement.
        Because we can't get the partition info from any information_schema views.
        """
        full_table_name = self._get_quote_full_table_name(table_name, schema)
        try:
            st = f"SHOW CREATE TABLE {full_table_name}"
            result = connection.execute(text(st)).first()
            if not result:
                return None

            create_table_str = result[1]  # 'Create Table' column

            # Use regex to find the PARTITION BY clause. It can be multi-line.
            match = self._PARTITION_BY_PATTERN.search(create_table_str)
            if match:
                partition_clause = match.group(1).strip()
                return partition_clause
            return None
        except exc.DBAPIError as e:
            logger.warning(f"Could not get partition info from SHOW CREATE TABLE for table {full_table_name}: {e}")
            return None

    def _get_show_full_columns(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> list[Row]:
        """Run SHOW FULL COLUMNS to get detailed column information.
        Currently, it's only used to get aggregate type of columns.
        Other column info are still mainly extracted from information_schema.columns.
        """
        full_table_name = self._get_quote_full_table_name(table_name, schema)
        try:
            st: str = f"SHOW FULL COLUMNS FROM {full_table_name}"
            # logger.debug(f"query special column info by using: {st}")
            return connection.execute(text(st)).fetchall()
        except exc.DBAPIError as e:
            # 1146: Table ... doesn't exist
            if e.orig and e.orig.args[0] == 1146:
                raise exc.NoSuchTableError(table_name) from e
            raise

    @reflection.cache
    def get_indexes(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> list[dict[str, Any]]:

        parsed_state: Any = self._parsed_state_or_create(
            connection, table_name, schema, **kwargs
        )

        indexes: list[dict[str, Any]] = []

        # TODO: same logic as MySQL?
        for spec in parsed_state.keys:
            
            dialect_options: dict[str, Any] = {}
            unique = False
            flavor: Optional[str] = spec["type"]
            if flavor == "PRIMARY":
                continue
            if flavor == "DUPLICATE":
                continue
            if flavor == "UNIQUE":
                unique = True
            elif flavor in ("FULLTEXT", "SPATIAL"):
                dialect_options["%s_prefix" % self.name] = flavor
            elif flavor is None:
                pass
            else:
                logger.warning(
                    "Converting unknown KEY type %s to a plain KEY", flavor
                )
                pass

            if spec["parser"]:
                dialect_options["%s_with_parser" % (self.name)] = spec[
                    "parser"
                ]

            index_d: dict[str, Any] = {}

            index_d["name"] = spec["name"]
            index_d["column_names"] = [s[0] for s in spec["columns"]]
            mysql_length: dict[str, Any] = {
                s[0]: s[1] for s in spec["columns"] if s[1] is not None
            }
            if mysql_length:
                dialect_options["%s_length" % self.name] = mysql_length

            index_d["unique"] = unique
            if flavor:
                index_d["type"] = flavor

            if dialect_options:
                index_d[ColumnSROptionsKey] = dialect_options

            indexes.append(index_d)
        return indexes

    def has_table(
        self, connection: Connection, table_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> bool:
        try:
            return super().has_table(connection, table_name, schema, **kwargs)
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) in (5501, 5502):
                return False
            raise

    def get_view_names(self, connection: Connection, schema: Optional[str] = None, **kwargs: Any) -> List[str]:
        """Return all view names in a given schema."""
        if schema is None:
            schema = self.default_schema_name
        try:
            rows = self._read_from_information_schema(
                connection,
                "views",
                table_schema=schema,
            )
            return [row.TABLE_NAME for row in rows]
        except Exception:
            return []

    def get_views(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> Dict[tuple[str | None, str], "ReflectedViewState"]:
        """Batch reflection: return all views mapping to ReflectedViewState by (schema, name).

        Prototype: not used by autogenerate yet, provided for potential optimization.
        """
        if schema is None:
            schema = self.default_schema_name
        results: Dict[tuple[str | None, str], ReflectedViewState] = {}
        try:
            rows = self._read_from_information_schema(
                connection,
                "views",
                table_schema=schema,
            )
            for row in rows:
                rv = ReflectionViewDefaults.apply(
                    name=row.TABLE_NAME,
                    definition=row.VIEW_DEFINITION,
                    comment="",
                    security=row.SECURITY_TYPE,
                )
                results[(schema, rv.name)] = rv
            return results
        except Exception:
            return results

    @reflection.cache
    def _get_view_info(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> Optional[ReflectedViewState]:
        """Gets all information about a view.

        Note: comment is currently not fetched from information_schema and defaults to empty.
        """
        if schema is None:
            schema = self.default_schema_name
        try:
            view_details = self._read_from_information_schema(
                connection,
                "views",
                table_schema=schema,
                table_name=view_name,
            )[0]
            rv = ReflectedViewState(
                name=view_details.TABLE_NAME,
                definition=view_details.VIEW_DEFINITION,
                # TODO: comment is not queried for now, it's not in information_schema.views
                comment="",
                security=view_details.SECURITY_TYPE.upper(),
            )
            logger.debug(
                "_get_view_info fetched: schema=%s, name=%s, security=%s, definition=(%s)",
                schema, rv.name, rv.security, rv.definition
            )
            return rv
        except Exception:
            return None

    def get_view(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> Optional[ReflectedViewState]:
        """Return all information about a view."""
        view_info = self._get_view_info(connection, view_name, schema, **kwargs)
        if not view_info:
            return None
        logger.debug(
            "get_view normalized: schema=%s, name=%s, security=%s, definition=(%s)",
            schema, view_info.name, view_info.security, view_info.definition
        )
        return ReflectionViewDefaults.apply_info(view_info)

    def get_view_definition(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> Optional[str]:
        """Return the definition of a view (delegates to get_view)."""
        rv = self.get_view(connection, view_name, schema=schema, **kwargs)
        return rv.definition if rv else None

    def get_view_comment(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> Optional[str]:
        """Return the comment of a view (delegates to get_view)."""
        rv = self.get_view(connection, view_name, schema=schema, **kwargs)
        return rv.comment if rv else None

    def get_view_security(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> Optional[str]:
        """Return the security type of a view (delegates to get_view)."""
        rv = self.get_view(connection, view_name, schema=schema, **kwargs)
        return rv.security if rv else None

    def get_materialized_view_names(
        self, connection: Connection, schema: Optional[str] = None, **kwargs: Any
    ) -> List[str]:
        """Return all materialized view names in a given schema."""
        if schema is None:
            schema = self.default_schema_name
        try:
            rows: list[_DecodingRow] = self._read_from_information_schema(
                connection,
                "materialized_views",
                table_schema=schema,
            )
            return [row.TABLE_NAME for row in rows]
        except Exception:
            return []

    def get_materialized_view_definition(
        self, connection: Connection, view_name: str, schema: Optional[str] = None, **kwargs: Any
    ) -> Optional[str]:
        """Return the definition of a materialized view."""
        if schema is None:
            schema = self.default_schema_name
        try:
            rows: list[_DecodingRow] = self._read_from_information_schema(
                connection,
                "materialized_views",
                table_schema=schema,
                table_name=view_name,
            )
            return rows[0].VIEW_DEFINITION
        except Exception:
            return None



