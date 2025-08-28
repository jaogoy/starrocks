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
import time
from typing import Union

from sqlalchemy import Connection, exc, schema as sa_schema
from sqlalchemy.dialects.mysql.pymysql import MySQLDialect_pymysql
from sqlalchemy.dialects.mysql.base import MySQLDDLCompiler, MySQLTypeCompiler, MySQLCompiler, MySQLIdentifierPreparer, _DecodingRow
from sqlalchemy.sql import sqltypes
from sqlalchemy.sql.expression import Delete, Select
from sqlalchemy.util import topological
from sqlalchemy import util
from sqlalchemy import log
from sqlalchemy.engine import reflection
from sqlalchemy.dialects.mysql.types import TINYINT, SMALLINT, INTEGER, BIGINT, DECIMAL, DOUBLE, FLOAT, CHAR, VARCHAR, DATETIME
from sqlalchemy.dialects.mysql.json import JSON
from .datatype import (
    LARGEINT, HLL, BITMAP, PERCENTILE, ARRAY, MAP, STRUCT,
    DATE
)

from . import reflection as _reflection
from .sql.ddl import CreateView, DropView, AlterView, CreateMaterializedView, DropMaterializedView
from .sql.schema import View
from .reflection import ReflectionViewInfo, StarRocksInspector, ReflectionViewDefaults
from sqlalchemy.ext.compiler import compiles
from sqlalchemy import text
from sqlalchemy.engine import Connection
from typing import List, Optional, Any, Dict
from .types import TableType, ColumnAggType
from .params import ColumnAggInfoKey, TableInfoKey, TableInfoKeyWithPrefix, ColumnAggInfoKeyWithPrefix

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
## NOTES - INCOMPLETE/UNFINISHED
# There are a number of items in here marked as ToDo
# In terms of table creation, the Partition, Distribution and OrderBy clauses need to be addressed from table options
# Tests `test_has_index` and `test_has_index_schema` are failing, this is because the CREATE INDEX statement appears to work async
#  and only when it's finished does it appear in the table definition
# Other tests are failing, need to fix or figure out how to suppress
# Review some skipped test suite requirements
#
#
#
##############################################################################################

# starrocks supported data types
ischema_names = {
    # === Boolean ===
    "boolean": sqltypes.BOOLEAN,
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
    "json": JSON,
    # === Date and time ===
    "date": DATE,
    "datetime": DATETIME,
    "timestamp": sqltypes.DATETIME,
    # == binary ==
    "binary": sqltypes.BINARY,
    "varbinary": sqltypes.VARBINARY,
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
        super().__init__(*args, **kwargs)
        self.logger = logging.getLogger(f"{__name__}.StarRocksDDLCompiler")

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

        create_table_suffix: str = self.create_table_suffix(table)
        if create_table_suffix:
            text += create_table_suffix + " "

        text += "("

        separator = "\n"

        # if only one primary key, specify it along with the column
        first_pk = False
        for create_column in create.columns:
            column = create_column.element
            try:
                processed = self.process(
                    create_column, first_pk=column.primary_key and not first_pk
                )
                if processed is not None:
                    text += separator
                    separator = ", \n"
                    text += "\t" + processed
                if column.primary_key:
                    first_pk = True
            except exc.CompileError as ce:
                raise exc.CompileError(
                    "(in table '%s', column '%s'): %s"
                    % (table.description, column.name, ce.args[0])
                ) from ce

        # N.B. Primary Key is specified in post_create_table
        #  Indexes are created by SQLA after the creation of the table using CREATE INDEX
        # const = self.create_table_constraints(
        #     table,
        #     _include_foreign_key_constraints=create.include_foreign_key_constraints,  # noqa
        # )
        # if const:
        #     text += separator + "\t" + const

        text += "\n)%s\n\n" % self.post_create_table(table)
        return text

    def _validate_key_definitions(self, table: sa_schema.Table):
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
                break # Found the key, no need to check for others
        
        if not key_str:
            return # No key defined, nothing to validate

        key_column_names = [k.strip() for k in key_str.split(',')]
        table_column_names = {c.name for c in table.columns}

        # 1. Generic Check: Ensure all key columns exist in the table definition.
        missing_keys = set(key_column_names) - table_column_names
        if missing_keys:
            raise exc.CompileError(
                f"Columns specified in {key_type} ('{key_str}') not found in table: {', '.join(missing_keys)}"
            )

        # 2. Specific Check: For AGGREGATE KEY tables, validate column order.
        if key_type == 'AGGREGATE KEY':
            self._validate_aggregate_key_order(table, key_column_names)


    def _validate_aggregate_key_order(self, table: sa_schema.Table, key_column_names: list[str]):
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
                    f"Value column '{col_list[i].name}' appears before key column '{col_list[last_key_col_index].name}'."
                )

    def post_create_table(self, table: sa_schema.Table) -> str:
        """
        Builds table-level clauses for a CREATE TABLE statement.

        This method compiles StarRocks-specific table options provided as `starrocks_`
        kwargs on the `Table` object. It is responsible for constructing clauses
        that appear after the column definitions, such as:
        - `ENGINE`
        - `PRIMARY KEY`, `DUPLICATE KEY`, `AGGREGATE KEY`, `UNIQUE KEY`
        - `COMMENT`
        - `PARTITION BY`
        - `DISTRIBUTED BY`
        - `ORDER BY`
        - `PROPERTIES`

        Args:
            table: The `sqlalchemy.schema.Table` object being compiled.

        Returns:
            A string containing all the compiled table-level DDL clauses.
        """

        table_opts: list[str] = []

        # Extract StarRocks-specific table options from kwargs without the dialect prefix (starrocks_)
        opts: dict[str, Any] = dict(
            (k[len(self.dialect.name) + 1 :].upper(), v)
            for k, v in table.kwargs.items()
            if k.startswith("%s_" % self.dialect.name)
        )

        if table.comment is not None:
            opts["COMMENT"] = table.comment

        if 'ENGINE' in opts:
            table_opts.append(f'ENGINE={opts["ENGINE"]}')

        # Key Types (Primary, Duplicate, Aggregate, Unique)
        for tbl_type_key_str, table_type in TableInfoKey.KEY_KWARG_MAP.items():
            kwarg_upper = tbl_type_key_str.upper()
            if kwarg_upper in opts:
                table_opts.append(f'{table_type}({opts[kwarg_upper]})')

        if "COMMENT" in opts:
            comment = self.sql_compiler.render_literal_value(
                opts["COMMENT"], sqltypes.String()
            )
            table_opts.append(f"COMMENT {comment}")

        # Partition
        if 'PARTITION_BY' in opts:
            table_opts.append(f'PARTITION BY {opts["PARTITION_BY"]}')

        # Distribution
        if 'DISTRIBUTED_BY' in opts:
            dist_str = f'DISTRIBUTED BY {opts["DISTRIBUTED_BY"]}'
            if 'BUCKETS' in opts:
                dist_str += f' BUCKETS {opts["BUCKETS"]}'
            table_opts.append(dist_str)
        
        # Order By
        if 'ORDER_BY' in opts:
            table_opts.append(f'ORDER BY({opts["ORDER_BY"]})')

        if "PROPERTIES" in opts:
            props_val = opts["PROPERTIES"]
            if isinstance(props_val, dict):
                props_items = props_val.items()
            elif isinstance(props_val, list):
                props_items = props_val
            else:
                raise exc.CompileError(f"Unsupported type for PROPERTIES: {type(props_val)}")
            
            props = ",\n".join([f'\t"{k}"="{v}"' for k, v in props_items])
            table_opts.append(f"PROPERTIES(\n{props}\n)")

        return " ".join(table_opts)

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
        # name, type, others of a column for the output colspec
        idx_name, idx_type, idx_others = 0, 1, 2

        # name and type 
        colspec: list[str] = [
            self.preparer.format_column(column),
            self.dialect.type_compiler.process(
                column.type, type_expression=column
            ),
        ]

        # aggregation type is only valid for AGGREGATE KEY tables
        is_agg_table: bool = (
            TableInfoKeyWithPrefix.AGGREGATE_KEY in (column.table.kwargs or {})
        )
        if not is_agg_table and (
            ColumnAggInfoKeyWithPrefix.is_agg_key in column.info
            or ColumnAggInfoKeyWithPrefix.agg_type in column.info
        ):
            raise exc.CompileError(
                "Column-level KEY/aggregate markers are only valid for AGGREGATE KEY tables; "
                "declare starrocks_aggregate_key at table level first."
            )
        if ColumnAggInfoKeyWithPrefix.is_agg_key in column.info:
            colspec.append(ColumnAggType.KEY)
            if ColumnAggInfoKeyWithPrefix.agg_type in column.info:
                raise exc.CompileError(f"Column '{column.name}' cannot be both KEY and aggregated (has {ColumnAggInfoKeyWithPrefix.agg_type}).")
        elif ColumnAggInfoKeyWithPrefix.agg_type in column.info:
            agg_val = str(column.info[ColumnAggInfoKeyWithPrefix.agg_type]).upper()
            if agg_val not in ColumnAggType.ALLOWED:
                raise exc.CompileError(f"Unsupported aggregate type for column '{column.name}': {agg_val}")
            colspec.append(agg_val)

        # NULL or NOT NULL. AUTO_INCREMENT columns must be NOT NULL
        if not column.nullable or column.autoincrement is True:
            colspec.append("NOT NULL")
        # else: omit explicit NULL (default)

        # see: https://docs.sqlalchemy.org/en/latest/dialects/mysql.html#mysql_timestamp_null  # noqa
        # elif column.nullable and is_timestamp:
        #     colspec.append("NULL") # ToDo - remove this, find way to fix the test

        # is_timestamp = isinstance(
        #     column.type._unwrapped_dialect_impl(self.dialect),
        #     sqltypes.TIMESTAMP,
        # )

        if column.comment is not None:
            literal = self.sql_compiler.render_literal_value(
                column.comment, sqltypes.String()
            )
            colspec.append("COMMENT " + literal)

        # ToDo >= version 3.0
        # if (
        #     column.table is not None
        #     and column is column.table._autoincrement_column
        #     and (
        #         column.server_default is None
        #         or isinstance(column.server_default, sa_schema.Identity)
        #     )
        #     and not (
        #         self.dialect.supports_sequences
        #         and isinstance(column.default, sa_schema.Sequence)
        #         and not column.default.optional
        #     )
        # ):
        #     colspec[1] = "BIGINT" # ToDo - remove this, find way to fix the test

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
        if column.computed is not None:
            colspec.append(self.process(column.computed))

        return " ".join(colspec)

    def visit_computed_column(self, generated: sa_schema.Computed, **kw: Any) -> str:
        text = "AS %s" % self.sql_compiler.process(
            generated.sqltext, include_table=False, literal_binds=True
        )
        return text

    def visit_primary_key_constraint(self, constraint: sa_schema.PrimaryKeyConstraint, **kw: Any) -> str:
        if len(constraint) == 0:
            return ""
        text = ""
        # if constraint.name is not None:
        #     formatted_name = self.preparer.format_constraint(constraint)
        #     if formatted_name is not None:
        #         text += "CONSTRAINT %s " % formatted_name
        text += "PRIMARY KEY "
        text += "(%s)" % ", ".join(
            self.preparer.quote(c.name)
            for c in (
                constraint.columns_autoinc_first
                if constraint._implicit_generated
                else constraint.columns
            )
        )
        text += self.define_constraint_deferrability(constraint)
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

        self.dialect.logger.debug("Compiled SQL for CreateView: \n%s", text)
        return text

    def visit_alter_view(self, alter: AlterView, **kw: Any) -> str:
        view = alter.element
        text = f"ALTER VIEW {self.preparer.format_table(view)}\n"

        if view.columns:
            text += self._get_view_column_clauses(view)

        # StarRocks does not support altering COMMENT or SECURITY via ALTER VIEW.
        # TODO: we can optimize it when StarRocks supports it in the future
        # Only redefine the SELECT statement.
        text += f"AS\n{view.definition}"

        self.dialect.logger.debug("Compiled SQL for AlterView: \n%s", text)
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
                    column_clauses.append(f'\t{col_name} COMMENT {comment}')
                else:
                    column_clauses.append(f'\t{col_name}')
            else:
                column_clauses.append(f'\t{self.preparer.quote(c)}')
        return " (\n%s\n)" % ",\n".join(column_clauses)

    def visit_drop_view(self, drop: DropView, **kw: Any) -> str:
        view = drop.element
        return f"DROP VIEW IF EXISTS {self.preparer.format_table(view)}"

    def visit_create_materialized_view(self, create: CreateMaterializedView, **kw: Any) -> str:
        mv = create.element
        properties = ""
        if mv.properties:
            prop_clauses: list[str] = [f'"{k}" = "{v}"' for k, v in mv.properties.items()]
            properties = f"PROPERTIES ({', '.join(prop_clauses)})"

        return (
            f"CREATE MATERIALIZED VIEW {self.preparer.format_table(mv)} "
            f"{properties} AS {mv.definition}"
        )

    def visit_drop_materialized_view(self, drop: DropMaterializedView, **kw: Any) -> str:
        mv = drop.element
        return f"DROP MATERIALIZED VIEW IF EXISTS {self.preparer.format_table(mv)}"


class StarRocksIdentifierPreparer(MySQLIdentifierPreparer):
    # reserved_words = RESERVED_WORDS
    pass


@log.class_logger
class StarRocksDialect(MySQLDialect_pymysql):
    name = "starrocks"
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

    def __init__(self, *args, **kw):
        super(StarRocksDialect, self).__init__(*args, **kw)
        self.logger = logging.getLogger(f"{__name__}.StarRocksDialect")

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
            WHERE {" AND ".join([f"{k} = '{escape_single_quote(v)}'" for k, v in kwargs.items()])}
        """)
        rp: Any = None
        try:
            rp = connection.execution_options(
                skip_user_error_events=False
            ).exec_driver_sql(st)
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) == 1146:
                raise exc.NoSuchTableError(f"information_schema.{inf_sch_table}") from e
            else:
                raise
        rows: list[_DecodingRow] = [_DecodingRow(row, charset) for row in rp.mappings().fetchall()]
        if not rows:
            raise exc.NoSuchTableError(f"Empty response for query: '{st}'")
        return rows
    
    @reflection.cache
    def _setup_parser(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw: Any) -> Any:
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
        if len(table_rows) > 1:
            raise exc.InvalidRequestError(
                f"Multiple tables found with name {table_name} in schema {schema}"
            )

        table_config_rows: list[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="tables_config",
            charset=charset,
            table_schema=schema,
            table_name=table_name,
        )
        if len(table_rows) > 1:
            raise exc.InvalidRequestError(
                f"Multiple tables found with name {table_name} in schema {schema}"
            )

        column_rows: list[_DecodingRow] = self._read_from_information_schema(
            connection=connection,
            inf_sch_table="columns",
            charset=charset,
            table_schema=schema,
            table_name=table_name,
        )

        return parser.parse(table=table_rows[0], table_config=table_config_rows[0], columns=column_rows, charset=charset)

    def _show_table_indexes(
        self, connection: Connection, table: sa_schema.Table, charset: Optional[str] = None, full_name: Optional[str] = None
    ) -> list[Any]:
        """Run SHOW INDEX FROM for a ``Table``."""

        if full_name is None:
            full_name = self.identifier_preparer.format_table(table)
        st = "SHOW INDEX FROM %s" % full_name

        rp: Any = None
        try:
            rp = connection.execution_options(
                skip_user_error_events=True
            ).exec_driver_sql(st)
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) == 1146:
                raise exc.NoSuchTableError(full_name) from e
            else:
                raise
        index_results: list[Any] = self._compat_fetchall(rp, charset=charset)
        return index_results

    @reflection.cache
    def get_indexes(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw: Any) -> list[dict[str, Any]]:

        parsed_state: Any = self._parsed_state_or_create(
            connection, table_name, schema, **kw
        )

        indexes: list[dict[str, Any]] = []

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
                self.logger.info(
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
                index_d["dialect_options"] = dialect_options

            indexes.append(index_d)
        return indexes

    def has_table(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw: Any) -> bool:
        try:
            return super().has_table(connection, table_name, schema, **kw)
        except exc.DBAPIError as e:
            if self._extract_error_code(e.orig) in (5501, 5502):
                return False
            raise

    def get_table_state(self, connection: Connection, table_name: str, schema: Optional[str] = None, **kw: Any):
        """Return StarRocks parsed table state (ReflectedState)."""
        return self._setup_parser(connection, table_name, schema, **kw)

    def get_view_names(self, connection: Connection, schema: Optional[str] = None, **kw: Any) -> List[str]:
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

    def get_views(self, connection: Connection, schema: Optional[str] = None, **kw: Any) -> Dict[tuple[str | None, str], "ReflectionViewInfo"]:
        """Batch reflection: return all views mapping to ReflectionViewInfo by (schema, name).

        Prototype: not used by autogenerate yet, provided for potential optimization.
        """
        if schema is None:
            schema = self.default_schema_name
        results: Dict[tuple[str | None, str], ReflectionViewInfo] = {}
        try:
            rows = self._read_from_information_schema(
                connection,
                "views",
                table_schema=schema,
            )
            for row in rows:
                info = {
                    "name": row.TABLE_NAME,
                    "definition": row.VIEW_DEFINITION,
                    "comment": "",
                    "security": row.SECURITY_TYPE,
                }
                rv = ReflectionViewDefaults.apply(
                    name=info["name"],
                    definition=self._strip_identifier_backticks(info["definition"]),
                    comment=info["comment"],
                    security=info["security"],
                )
                results[(schema, rv.name)] = rv
            return results
        except Exception:
            return results

    @reflection.cache
    def _get_view_info(self, connection: Connection, view_name: str, schema: Optional[str] = None, **kw: Any) -> Optional[Dict[str, Any]]:
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
            info = {
                "name": view_details.TABLE_NAME,
                "definition": view_details.VIEW_DEFINITION,
                # comment intentionally defaulted; not queried for now
                "comment": "",
                "security": view_details.SECURITY_TYPE,
            }
            self.logger.debug("_get_view_info fetched: schema=%s name=%s security=%s", schema, info["name"], info["security"])
            return info
        except Exception:
            return None

    def _strip_identifier_backticks(self, sql_text: str) -> str:
        """Remove MySQL-style identifier quotes (`) while preserving those inside string literals.

        This handles backslash-escaped characters within single-quoted strings.
        """
        in_single_quote = False
        escaped = False
        output_chars: list[str] = []
        for ch in sql_text:
            if in_single_quote:
                output_chars.append(ch)
                if escaped:
                    escaped = False
                elif ch == "\\":
                    escaped = True
                elif ch == "'":
                    in_single_quote = False
                continue
            if ch == "'":
                in_single_quote = True
                output_chars.append(ch)
            elif ch == "`":
                # Skip identifier quote
                continue
            else:
                output_chars.append(ch)
        return "".join(output_chars)

    def get_view(self, connection: Connection, view_name: str, schema: Optional[str] = None, **kw: Any) -> Optional[ReflectionViewInfo]:
        """Return all information about a view."""
        view_info = self._get_view_info(connection, view_name, schema, **kw)
        if not view_info:
            return None
        
        # Apply defaults and normalization
        name = view_info["name"]
        definition = self._strip_identifier_backticks(view_info["definition"])
        comment = (view_info.get("comment") or "")
        security = (view_info.get("security") or "").upper()
        self.logger.debug("get_view normalized: schema=%s name=%s security=%s", schema, name, security)
        return ReflectionViewDefaults.apply(
            name=name,
            definition=definition,
            comment=comment,
            security=security,
        )

    def get_view_definition(self, connection: Connection, view_name: str, schema: Optional[str] = None, **kw: Any) -> Optional[str]:
        """Return the definition of a view (delegates to get_view)."""
        rv = self.get_view(connection, view_name, schema=schema, **kw)
        return rv.definition if rv else None

    def get_view_comment(self, connection: Connection, view_name: str, schema: Optional[str] = None, **kw: Any) -> Optional[str]:
        """Return the comment of a view (delegates to get_view)."""
        rv = self.get_view(connection, view_name, schema=schema, **kw)
        return rv.comment if rv else None

    def get_view_security(self, connection: Connection, view_name: str, schema: Optional[str] = None, **kw: Any) -> Optional[str]:
        """Return the security type of a view (delegates to get_view)."""
        rv = self.get_view(connection, view_name, schema=schema, **kw)
        return rv.security if rv else None

    def get_materialized_view_names(self, connection: Connection, schema: Optional[str] = None, **kw: Any) -> List[str]:
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

    def get_materialized_view_definition(self, connection: Connection, view_name: str, schema: Optional[str] = None, **kw: Any) -> Optional[str]:
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
