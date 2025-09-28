from alembic.ddl.mysql import MySQLImpl
from sqlalchemy import Column, MetaData, Table
from typing import Any, Optional
import logging

from starrocks import datatype
from starrocks.alembic import compare
from starrocks.datatype import BIGINT, BOOLEAN, STRING, TINYINT, VARCHAR


logger = logging.getLogger(__name__)


class StarrocksImpl(MySQLImpl):
    """Alembic DDL implementation for StarRocks."""

    __dialect__ = "starrocks"

    def version_table_impl(
        self,
        *,
        version_table: str,
        version_table_schema: Optional[str],
        version_table_pk: bool,  # ignored as StarRocks requires a primary key
        **kw,
    ) -> Table:
        version_table_kwargs = self.context_opts.get("version_table_kwargs", {}) if self.context_opts else {}
        if version_table_kwargs:
            logger.info(f"There are extra kwargs for version_table: {version_table_kwargs}")
        return Table(
            version_table,
            MetaData(),
            Column("id", BIGINT, autoincrement=True, primary_key=True),
            Column("version_num", VARCHAR(32), primary_key=False),
            schema=version_table_schema,
            starrocks_primary_key="id",
            **version_table_kwargs,
            **kw,
        )

    def compare_type(self, inspector_column: Column[Any], metadata_column: Column[Any]) -> bool:
        """
        Set StarRocks' specific type comparison logic for some special cases.

        For some special cases:
            - complex type comparison: ARRAY, MAP, STRUCT
            - simple type comparison:
                - meta.BOOLEAN equals to conn.TINYINT(1)
                - meta.STRING equals to conn.VARCHAR(65533)
        
        Args:   
            inspector_column: The column from the inspector.
            metadata_column: The column from the metadata.

        Returns:
            True if the types are different, False if the types are the same.
        """
        inspector_type = inspector_column.type
        metadata_type = metadata_column.type
        
        # Handle complex type comparison.
        if isinstance(metadata_type, datatype.StructuredType):
            # If the inspector found a different base type, they are different.
            if not isinstance(inspector_type, type(metadata_type)):
                return True

            # Perform deep, recursive comparison.
            # Returns True if different, False if same.
            return compare.compare_complex_type(self, inspector_type, metadata_type)

        return compare.compare_simple_type(self, inspector_column, metadata_column)
