from alembic.ddl.mysql import MySQLImpl
from sqlalchemy import Column, MetaData, Table
from typing import Any, Optional
import logging

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
            - meta.BOOLEAN equals to conn.TINYINT(1)
            - meta.STRING equals to conn.VARCHAR(65533)
        """
        inspector_type = inspector_column.type
        metadata_type = metadata_column.type
        
        # Scenario 1.a: model defined BOOLEAN, database stored TINYINT(1)
        if (isinstance(metadata_type, BOOLEAN) and 
            isinstance(inspector_type, TINYINT) and
            getattr(inspector_type, 'display_width', None) == 1):
            return False
            
        # Scenario 1.b: model defined TINYINT(1), database may display as Boolean (theoretically not possible, but for safety)
        if (isinstance(metadata_type, TINYINT) and 
            getattr(metadata_type, 'display_width', None) == 1 and
            isinstance(inspector_type, BOOLEAN)):
            return False
        
        # Scenario 2.a: model defined STRING, database stored VARCHAR(65533)
        if (isinstance(metadata_type, STRING) and
            isinstance(inspector_type, VARCHAR) and
            getattr(inspector_type, 'length', None) == 65533):
            return False
        
        # Scenario 2.b: model defined VARCHAR(65533), database stored STRING (theoretically not possible, but for safety)
        if (isinstance(metadata_type, VARCHAR) and
            isinstance(inspector_type, STRING) and
            getattr(inspector_type, 'length', None) == 65533):
            return False
            
        # Other cases use default comparison logic
        return super().compare_type(inspector_column, metadata_column)
