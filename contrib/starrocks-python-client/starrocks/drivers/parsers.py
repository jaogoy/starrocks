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

import importlib.resources
import logging
from typing import Any, List, Tuple

from lark import Lark, Transformer, v_args, Token
from sqlalchemy.sql import sqltypes

from .. import datatype


logger = logging.getLogger(__name__)


class DataTypeTransformer(Transformer):
    """
    Use lark-parser to parse the data type string.
    And generate the corresponding SQLAlchemy type object.
    """

    def __init__(self, visit_tokens: bool = True) -> None:
        super().__init__(visit_tokens)
        import starrocks.dialect
        self.type_map = starrocks.dialect.StarRocksDialect.ischema_names.copy()
        # `int` is a keyword in lark grammar, but we want to map it to INTEGER
        self.type_map["int"] = self.type_map["integer"]

    def CNAME(self, t: Token) -> str:
        return t.value

    def NUMBER(self, n: Token) -> int:
        return int(n)

    @v_args(inline=True)
    def simple_type(self, type_name: str, args: List[int] = None, unsigned: Token = None) -> sqltypes.TypeEngine:
        type_name_lower = type_name.lower()
        # logger.debug(f"type_name_lower: {type_name_lower}, args: {args}, unsigned: {unsigned}")
        # There may be a bug of parsing the type and unsigned keyword together
        if args and (isinstance(args, Token) and args.value == "unsigned" or args == "unsigned"):
            args = None
            unsigned = True
        if type_name_lower == "bigint" and unsigned:
            return self.type_map["largeint"]

        type_class = self.type_map.get(type_name_lower)
        if type_class is None:
            # logger.error(f"Unsupported data type: {type_name}")
            raise TypeError(f"Unsupported data type: {type_name}")
        if args:
            return type_class(*args)
        return type_class

    def type_args(self, args: List[Any]) -> List[Any]:
        # logger.debug(f"args: {args}")
        return args

    @v_args(inline=True)
    def array_type(self, subtype: Any) -> datatype.ARRAY:
        # logger.debug(f"subtype: {subtype}")
        return datatype.ARRAY(subtype)

    @v_args(inline=True)
    def map_type(self, key_type: Any, value_type: Any) -> datatype.MAP:
        # logger.debug(f"key_type: {key_type}, value_type: {value_type}")
        return datatype.MAP(key_type, value_type)

    @v_args(inline=True)
    def struct_field(self, name: Any, field_type: Any) -> Tuple[Any, Any]:
        # logger.debug(f"struct field_name: {name}, field_type: {field_type}")
        return (name, field_type)

    def struct_type(self, fields: List[Tuple[Any, Any]]) -> datatype.STRUCT:
        # logger.debug(f"struct fields: {fields}")
        return datatype.STRUCT(*fields)

    @v_args(inline=True)
    def data_type(self, item: Any) -> Any:
        return item


# For singleton pattern
_grammar_text = None
# For singleton pattern
_data_type_parser = None


def _get_grammar_text() -> str:
    global _grammar_text
    if _grammar_text is None:
        with importlib.resources.open_text("starrocks.drivers", "grammar.lark") as f:
            _grammar_text = f.read()
    return _grammar_text


def get_data_type_parser() -> Lark:
    """
    Returns a singleton instance of the data type Lark parser.
    """
    global _data_type_parser
    if _data_type_parser is None:
        grammar_text = _get_grammar_text()
        _data_type_parser = Lark(grammar_text, parser="lalr", start="data_type", transformer=DataTypeTransformer())
    return _data_type_parser


def parse_data_type(type_str: str) -> Any:
    """
    Parses a StarRocks type string and returns a SQLAlchemy type object.

    Args:
        type_str: The type string to parse.

    Returns:
        A SQLAlchemy type object.
    """
    return get_data_type_parser().parse(type_str)
