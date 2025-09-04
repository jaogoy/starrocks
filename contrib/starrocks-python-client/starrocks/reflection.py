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
from typing import Any, Optional

from sqlalchemy.dialects.mysql.types import DATETIME, TIME, TIMESTAMP
from sqlalchemy.dialects.mysql.base import _DecodingRow
from sqlalchemy.dialects.mysql.reflection import _re_compile
from sqlalchemy import log, types as sqltypes, util
from sqlalchemy.engine.reflection import Inspector

from .params import ColumnAggInfoKeyWithPrefix, ColumnSROptionsKey, TableInfoKeyWithPrefix
from .reflection_info import ReflectedState, ReflectionViewInfo, ReflectionDistributionInfo
from .types import TableModel

logger = logging.getLogger(__name__)


class StarRocksInspector(Inspector):
    def __init__(self, bind):
        super().__init__(bind)

    def get_view(self, view_name: str, schema: Optional[str] = None, **kwargs: Any) -> Optional[ReflectionViewInfo]:
        return self.dialect._get_view_info(self.bind, view_name, schema=schema, **kwargs)

    def get_views(self, schema: str | None = None) -> dict[tuple[str | None, str], ReflectionViewInfo]:
        """Batch reflection wrapper for dialect.get_views (prototype)."""
        return self.dialect.get_views(self.bind, schema=schema)


@log.class_logger
class StarRocksTableDefinitionParser(object):
    """Parses content of information_schema tables to get table information."""

    _BUCKETS_PATTERN = re.compile(r'\sBUCKETS\s+(\d+)', re.IGNORECASE)
    _BUCKETS_REPLACE_PATTERN = re.compile(r'\s+BUCKETS\s+\d+', re.IGNORECASE)

    def __init__(self, dialect, preparer):
        self.dialect = dialect
        self.preparer = preparer
        self._re_csv_int = _re_compile(r"\d+")
        self._re_csv_str = _re_compile(r"\x27(?:\x27\x27|[^\x27])*\x27")

    @staticmethod
    def parse_distribution_string(distribution: str) -> tuple[str, int | None]:
        """Parse DISTRIBUTED BY string to extract distribution and buckets.
        Args:
            distribution: String like "HASH(id) BUCKETS 8" or "HASH(id)"
        Returns:
            Tuple of (distributed_by, buckets)
        """
        if not distribution:
            return distribution, None

        buckets_match = StarRocksTableDefinitionParser._BUCKETS_PATTERN.search(distribution)

        if buckets_match:
            buckets = int(buckets_match.group(1))
            # Remove BUCKETS part to get pure distribution
            distributed_by = StarRocksTableDefinitionParser._BUCKETS_REPLACE_PATTERN.sub('', distribution).strip()
            return distributed_by, buckets
        else:
            return distribution, None

    def parse(
        self,
        table: _DecodingRow,
        table_config: _DecodingRow,
        columns: list[_DecodingRow],
        agg_types: dict[str, str],
        charset: str,
    ) -> ReflectedState:
        return ReflectedState(
            table_name=table.TABLE_NAME,
            columns=[
                self._parse_column(column=column, agg_type=agg_types.get(column.COLUMN_NAME))
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

    def _parse_column_type(self, column: _DecodingRow) -> Any:
        """
        Parse column type from information_schema.columns table.
        It splits column type into type and arguments.
        After that it creates instance of column type.
        """
        pattern = r"^(?P<type>\w+)(?:\s*\((?P<args>.*?)\))?$"
        match = re.match(pattern, column["COLUMN_TYPE"])
        type_ = match.group("type")
        args = match.group("args")
        try:
            col_type = self.dialect.ischema_names[type_]
        except KeyError:
            util.warn(
                "Did not recognize type '%s' of column '%s'" % (type_, column["COLUMN_NAME"])
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

    def _parse_column(self, column: _DecodingRow, agg_type: str | None = None) -> dict:
        """
        Parse column from information_schema.columns table.
        It returns dictionary with column informations expected by sqlalchemy.
        """
        col_info = {
            "name": column["COLUMN_NAME"],
            "type": self._parse_column_type(column=column),
            "nullable": column["IS_NULLABLE"] == "YES",
            "default": column["COLUMN_DEFAULT"],
            "autoincrement": None,  # TODO: This is not specified
            "computed": {"sqltext": column["GENERATION_EXPRESSION"]},
            "comment": column["COLUMN_COMMENT"],
        }
        if agg_type:
            col_info[ColumnSROptionsKey] = {ColumnAggInfoKeyWithPrefix.AGG_TYPE: agg_type}
        return col_info

    def _get_key_columns(self, columns: list[_DecodingRow]) -> list[str]:
        """
        Get list of key columns (COLUMN_KEY) from information_schema.columns table.
        It returns list of column names that are part of key.
        """
        sorted_columns = sorted(columns, key=lambda col: col["ORDINAL_POSITION"])
        return [c["COLUMN_NAME"] for c in sorted_columns if c["COLUMN_KEY"]]

    def _get_key_type(self, table_config: _DecodingRow) -> str:
        """
        Get key type from information_schema.tables_config table.
        """
        return TableModel.TO_TYPE_MAP.get(table_config.TABLE_MODEL, "")

    def _get_key_desc(self, columns: list[_DecodingRow]) -> str:
        """
        Get key description from information_schema.columns table.
        It returns string representation of key description.
        """
        quoted_cols = [
            self.preparer.quote_identifier(col)
            for col in self._get_key_columns(columns=columns)
        ]
        key_type = self._get_key_type(columns=columns)
        return f"{key_type}({', '.join(quoted_cols)})"

    def _get_distribution_info(self, table_config: _DecodingRow) -> ReflectionDistributionInfo:
        """
        Get distribution from information_schema.tables_config table.
        It returns ReflectionDistributionInfo representation of distribution option.
        """
        return ReflectionDistributionInfo(
            type=table_config.DISTRIBUTE_TYPE,
            keys=table_config.DISTRIBUTE_KEY,
            buckets=table_config.DISTRIBUTE_BUCKET,
        )

    def _parse_table_options(self, table: _DecodingRow, table_config: _DecodingRow, columns: list[_DecodingRow]) -> dict:
        """Parse table options from `information_schema` views."""
        opts = {}

        # TODO: check whether there need be a `starrocks_primary_key = (c1, c2)` in the opts

        if table_config.TABLE_ENGINE:
            opts[TableInfoKeyWithPrefix.ENGINE] = table_config.TABLE_ENGINE.upper()

        if table.TABLE_COMMENT:
            opts[TableInfoKeyWithPrefix.COMMENT] = table.TABLE_COMMENT

        if table_config.PARTITION_KEY:
            opts[TableInfoKeyWithPrefix.PARTITION_BY] = table_config.PARTITION_KEY

        if table_config.DISTRIBUTE_KEY:
            opts[TableInfoKeyWithPrefix.DISTRIBUTED_BY] = self._get_distribution_info(table_config)

        if table_config.SORT_KEY:
            columns = [c.strip() for c in table_config.SORT_KEY.split(",")]
            opts[TableInfoKeyWithPrefix.ORDER_BY] = columns

        if table_config.PROPERTIES:
            try:
                opts[TableInfoKeyWithPrefix.PROPERTIES] = dict(
                    json.loads(table_config.PROPERTIES or "{}").items()
                )
            except json.JSONDecodeError:
                logger.info(f"properties are not valid JSON: {table_config.PROPERTIES}")

        return opts
