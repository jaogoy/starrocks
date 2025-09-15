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

from __future__ import annotations
import json
import logging
import re
from typing import Any, Optional, Union

from sqlalchemy.dialects.mysql.types import DATETIME, TIME, TIMESTAMP
from sqlalchemy.dialects.mysql.base import _DecodingRow
from sqlalchemy.dialects.mysql.reflection import _re_compile
from sqlalchemy import log, types as sqltypes, util
from sqlalchemy.engine.interfaces import ReflectedColumn
from sqlalchemy.engine.reflection import Inspector

from starrocks.defaults import ReflectionTableDefaults

from . import utils
from .utils import SQLParseError

from .params import ColumnAggInfoKeyWithPrefix, ColumnSROptionsKey, DialectName, SRKwargsPrefix, TableInfoKeyWithPrefix, TableInfoKey
from .reflection_info import ReflectedState, ReflectedViewState, ReflectedDistributionInfo, ReflectedPartitionInfo
from .types import PartitionType, TableModel, TableType, TableEngine
from .consts import TableConfigKey

logger = logging.getLogger(__name__)


class StarRocksInspector(Inspector):
    """
    The StarRocksInspector provides a custom inspector for the StarRocks dialect,
    allowing for reflection of StarRocks-specific database objects like views.
    """
    def __init__(self, bind):
        super().__init__(bind)

    def get_view(self, view_name: str, schema: Optional[str] = None, **kwargs: Any) -> Optional[ReflectedViewState]:
        """
        Retrieves information about a specific view.

        :param view_name: The name of the view to inspect.
        :param schema: The schema of the view; defaults to the default schema name if None.
        :param kwargs: Additional arguments passed to the dialect's get_view method.
        :return: A ReflectedViewState object, or None if the view does not exist.
        """
        return self.dialect.get_view(self.bind, view_name, schema=schema, **kwargs)

    def get_views(self, schema: str | None = None) -> dict[tuple[str | None, str], ReflectedViewState]:
        """
        Retrieves a dictionary of all views in a given schema.

        :param schema: The schema to inspect; defaults to the default schema name if None.
        :return: A dictionary mapping (schema, view_name) to ReflectedViewState objects.
        """
        return self.dialect.get_views(self.bind, schema=schema)


@log.class_logger
class StarRocksTableDefinitionParser(object):
    """
    This parser is responsible for interpreting the raw data returned from
    StarRocks' `information_schema` and `SHOW` commands.
    
    For columns, the base attributes (name, type, nullable, default) are
    parsed here, leveraging the underlying MySQL dialect where possible.
    This dialect-specific implementation adds logic to parse StarRocks-specific
    attributes that are not present in standard MySQL, such as the aggregation
    type on a column (e.g., 'SUM', 'REPLACE', 'KEY'). This is achieved by
    querying `SHOW FULL COLUMNS` and processing the 'Extra' field.
    
    Other standard column attributes are assumed to be handled correctly by
    the base MySQL dialect's reflection mechanisms.

    MySQLTableDefinitionParser uses regex to parse information, so it's not
    used here.
    """

    _BUCKETS_PATTERN = re.compile(r'\sBUCKETS\s+(\d+)', re.IGNORECASE)
    _BUCKETS_REPLACE_PATTERN = re.compile(r'\s+BUCKETS\s+\d+', re.IGNORECASE)

    def __init__(self, dialect, preparer):
        self.dialect = dialect
        self.preparer = preparer
        self._re_csv_int = _re_compile(r"\d+")
        self._re_csv_str = _re_compile(r"\x27(?:\x27\x27|[^\x27])*\x27")

    def parse(
        self,
        table: _DecodingRow,
        table_config: dict[str, Any],
        columns: list[_DecodingRow],
        column_2_agg_type: dict[str, str],
        charset: str,
    ) -> ReflectedState:
        """
        Parses the raw reflection data into a structured ReflectedState object.

        :param table: A row from `information_schema.tables`.
        :param table_config: A dictionary representing a row from `information_schema.tables_config`,
                             augmented with the 'PARTITION_CLAUSE'.
        :param columns: A list of rows from `information_schema.columns`.
        :param column_2_agg_type: A dictionary mapping column names to their aggregation types.
        :param charset: The character set of the table.
        :return: A ReflectedState object containing the parsed table information.
        """
        return ReflectedState(
            table_name=table.TABLE_NAME,
            columns=[
                self._parse_column(column=column, 
                    **{ColumnAggInfoKeyWithPrefix.AGG_TYPE: column_2_agg_type.get(column.COLUMN_NAME)})
                for column in columns
            ],
            table_options=self._parse_table_options(
                table=table, table_config=table_config, columns=columns
            ),
            keys=[{
                "type": self._get_key_type(table_config=table_config),
                "columns": [(c, None, None) for c in self._get_key_columns(columns=columns)],
                "parser": None,
                "name": None,
            }],
        )

    def _parse_column(self, column: _DecodingRow, **kwargs: Any) -> dict:
        """
        Parse column from information_schema.columns table.
        It returns dictionary with column informations expected by sqlalchemy.

        Args:
            column: A row from `information_schema.columns`.
            kwargs: Additional keyword arguments, with prefix `starrocks_`, passed to the dialect.
                currently only support:
                    - starrocks_IS_AGG_KEY: Whether the column is a key column.
                    - starrocks_AGG_TYPE: The aggregate type of the column.

        Returns:
            A dictionary with column information expected by sqlalchemy.
            It's the same as the `ReflectedColumn` object.
        """
        computed = {"sqltext": column.GENERATION_EXPRESSION} if column.GENERATION_EXPRESSION else None
        col_info = {
            "name": column.COLUMN_NAME,
            "type": self._parse_column_type(column=column),
            "nullable": column.IS_NULLABLE == "YES",
            "default": column.COLUMN_DEFAULT,
            "autoincrement": None,  # TODO: This is not specified
            "comment": column.COLUMN_COMMENT,
            "dialect_options": kwargs
        }
        if computed:
            col_info["computed"] = computed
        return col_info

    def _parse_column_type(self, column: _DecodingRow) -> Any:
        """
        Parse column type from information_schema.columns table.
        It splits column type into type and arguments.
        After that it creates instance of column type.
        """
        pattern = r"^(?P<type>\w+)(?:\s*\((?P<args>.*?)\))?$"
        match = re.match(pattern, column.COLUMN_TYPE)
        type_ = match.group("type")
        args = match.group("args")
        try:
            col_type = self.dialect.ischema_names[type_]
        except KeyError:
            util.warn(
                "Did not recognize type '%s' of column '%s'" % (type_, column.COLUMN_NAME)
            )
            col_type = sqltypes.NullType

        # Column type positional arguments eg. varchar(32)
        if args is None or args == "":
            type_args = []
        elif args[0] == "'" and args[-1] == "'":
            type_args = self._re_csv_str.findall(args)
        else:
            type_args = [int(v) for v in self._re_csv_int.findall(args)]

        # Column type keyword options
        type_kw = {}

        if issubclass(col_type, (DATETIME, TIME, TIMESTAMP)):
            if type_args:
                type_kw["fsp"] = type_args.pop(0)

        if col_type.__name__ == "LARGEINT":
            type_instance = col_type()
        else:
            type_instance = col_type(*type_args, **type_kw)
        return type_instance

    def _get_key_desc(self, table_config: dict[str, Any], columns: list[_DecodingRow]) -> str:
        """
        Get key description from information_schema.columns table.
        It returns string representation of key description.
        """
        quoted_cols = [
            self.preparer.quote_identifier(col)
            for col in self._get_key_columns(columns=columns)
        ]
        key_type = self._get_key_type(table_config=table_config)
        return f"{key_type}({', '.join(quoted_cols)})"

    def _get_key_type(self, table_config: dict[str, Any]) -> str:
        """
        Get key type from information_schema.tables_config table.
        """
        return TableModel.TO_TYPE_MAP.get(table_config.get(TableConfigKey.TABLE_MODEL), "")

    def _get_key_columns(self, columns: list[_DecodingRow]) -> list[str]:
        """
        Get list of key columns (COLUMN_KEY) from information_schema.columns table.
        It returns list of column names that are part of key.

        Currently, we can't extract the key columns from information_schema.tables_config.
        """
        sorted_columns = sorted(columns, key=lambda col: col.ORDINAL_POSITION)
        return [c.COLUMN_NAME for c in sorted_columns if c.COLUMN_KEY]

    @staticmethod
    def parse_partition_clause(partition_clause: str) -> ReflectedPartitionInfo | None:
        """
        Parses a raw PARTITION BY clause string into a structured ReflectedPartitionInfo object.

        This method handles RANGE, LIST, and expression partitioning schemes. It
        extracts the partition type, or expression used for partitioning,
        and any pre-defined partition clauses
        (e.g., `(PARTITION p1 VALUES LESS THAN ('100'), PARTITION p2 VALUES LESS THAN ('200'))`).

        Args:
            partition_clause: The raw string of the PARTITION BY clause from a
                `SHOW CREATE TABLE` statement.

        Returns:
            A `ReflectedPartitionInfo` object containing the parsed details.
        """
        if not partition_clause:
            return None

        clause_upper = partition_clause.strip().upper()
        partition_method: str
        pre_created_partitions: Optional[str] = None

        # Check for RANGE or LIST partitioning
        if clause_upper.startswith(PartitionType.RANGE) or clause_upper.startswith(PartitionType.LIST):
            partition_type = PartitionType.RANGE if clause_upper.startswith(PartitionType.RANGE) else PartitionType.LIST

            # Find the end of the RANGE/LIST(...) part using robust parenthesis matching
            open_paren_index = partition_clause.find('(')
            if open_paren_index != -1:
                close_paren_index = utils.find_matching_parenthesis(partition_clause, open_paren_index)
                if close_paren_index != -1:
                    partition_method = partition_clause[:close_paren_index + 1].strip()
                    rest = partition_clause[close_paren_index + 1:].strip()
                    if rest:
                        pre_created_partitions = rest
                else:  # Fallback for mismatched parentheses
                    raise SQLParseError(f"Invalid partition clause, mismatched parentheses: {partition_clause}")
            else:  # Fallback for no parentheses
                raise SQLParseError(f"Invalid partition clause, no columns specified: {partition_clause}")
        else:
            # If not RANGE or LIST, it's an expression-based partition
            partition_type = PartitionType.EXPRESSION
            partition_method = partition_clause

        return ReflectedPartitionInfo(
            type=partition_type,
            partition_method=partition_method,
            pre_created_partitions=pre_created_partitions
        )

    @staticmethod
    def parse_distribution_clause(distribution: str) -> ReflectedDistributionInfo | None:
        """Parse DISTRIBUTED BY string to extract distribution method and buckets.
        Args:
            distribution: String like "HASH(id) BUCKETS 8" or "HASH(id)"
        Returns:
            ReflectedDistributionInfo object
        """
        if not distribution:
            return None
        buckets_match = StarRocksTableDefinitionParser._BUCKETS_PATTERN.search(distribution)

        if buckets_match:
            buckets = int(buckets_match.group(1))
            # Remove BUCKETS part to get pure distribution
            distribution_method = StarRocksTableDefinitionParser._BUCKETS_REPLACE_PATTERN.sub('', distribution).strip()
        else:
            buckets = None
            distribution_method = distribution
        
        return ReflectedDistributionInfo(
            type=None,
            columns=None,
            distribution_method=distribution_method,
            buckets=buckets,
        )
    
    @staticmethod
    def parse_distribution(distribution: Optional[Union[ReflectedDistributionInfo, str]]
                           ) -> ReflectedDistributionInfo | None:
        if not distribution or isinstance(distribution, ReflectedDistributionInfo):
            return distribution
        return StarRocksTableDefinitionParser.parse_distribution_clause(distribution)

    def _get_distribution_info(self, table_config: dict[str, Any]) -> ReflectedDistributionInfo:
        """
        Get distribution from information_schema.tables_config table.
        It returns ReflectedDistributionInfo representation of distribution option.
        """
        return ReflectedDistributionInfo(
            type=table_config.get(TableConfigKey.DISTRIBUTE_TYPE),
            columns=table_config.get(TableConfigKey.DISTRIBUTE_KEY),
            distribution_method=None,
            buckets=table_config.get(TableConfigKey.DISTRIBUTE_BUCKET),
        )

    def _parse_table_options(self, table: _DecodingRow, table_config: dict[str, Any], columns: list[_DecodingRow]) -> dict:
        """
        Parse table options from `information_schema` views,
        and generate the table options with `starrocks_` prefix, which will be used to reflect a Table().
        Then, these options will be exactly the same as the options of a sqlalchemy.Table()
        which is created by users manually, for both sqlalchemy.Table() or ORM styles.

        :param table: A row from `information_schema.tables`.
        :param table_config: A dictionary representing a row from `information_schema.tables_config`,
                             augmented with the 'PARTITION_CLAUSE'.
        :param columns: A list of rows from `information_schema.columns`.
        :return: A dictionary of StarRocks-specific table options with the 'starrocks_' prefix.
        """
        opts = {}

        if table_engine := table_config.get(TableConfigKey.TABLE_ENGINE):
            logger.debug(f"table_config.{TableConfigKey.TABLE_ENGINE}: {table_engine}")
            # if table_engine != TableEngine.OLAP:
            #     raise NotImplementedError(f"Table engine {table_engine} is not supported now.")
            opts[TableInfoKeyWithPrefix.ENGINE] = table_engine.upper()

        if table.TABLE_COMMENT:
            logger.debug(f"table.TABLE_COMMENT: {table.TABLE_COMMENT}")
            opts[TableInfoKeyWithPrefix.COMMENT] = table.TABLE_COMMENT

        # Get key type from information_schema.tables_config.TABLE_MODEL,
        # and key columns from information_schema.columns.COLUMN_KEY
        if table_model := table_config.get(TableConfigKey.TABLE_MODEL):
            logger.debug(f"table_config.{TableConfigKey.TABLE_MODEL}: {table_model}")
            key_type_str = TableInfoKey.MODEL_TO_KEY_MAP.get(table_model)
            if key_type_str:
                key_columns_str = ", ".join(self._get_key_columns(columns))
                prefixed_key = f"{SRKwargsPrefix}{key_type_str}"
                opts[prefixed_key] = key_columns_str

        if partition_clause := table_config.get(TableConfigKey.PARTITION_CLAUSE):
            logger.debug(f"table_config.{TableConfigKey.PARTITION_CLAUSE}: {partition_clause}")
            opts[TableInfoKeyWithPrefix.PARTITION_BY] = self.parse_partition_clause(partition_clause)

        if distribute_key := table_config.get(TableConfigKey.DISTRIBUTE_KEY):
            logger.debug(f"table_config.{TableConfigKey.DISTRIBUTE_KEY}: {distribute_key}")
            opts[TableInfoKeyWithPrefix.DISTRIBUTED_BY] = str(self._get_distribution_info(table_config))

        if sort_key := table_config.get(TableConfigKey.SORT_KEY):
            logger.debug(f"table_config.{TableConfigKey.SORT_KEY}: {sort_key}")
            # columns = [c.strip() for c in table_config.get('SORT_KEY').split(",")]
            opts[TableInfoKeyWithPrefix.ORDER_BY] = sort_key

        if properties := table_config.get(TableConfigKey.PROPERTIES):
            logger.debug(f"table_config.{TableConfigKey.PROPERTIES}: {properties}")
            try:
                opts[TableInfoKeyWithPrefix.PROPERTIES] = dict(
                    json.loads(properties or "{}").items()
                )
            except json.JSONDecodeError:
                logger.info(f"properties are not valid JSON: {properties}")

        return opts
