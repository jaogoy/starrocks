import logging
from sqlalchemy import text, Table, MetaData, Column, Integer, String, inspect
from sqlalchemy.engine import Engine

from starrocks.params import TableInfoKeyWithPrefix
from starrocks.reflection_info import ReflectionPartitionInfo

logger = logging.getLogger(__name__)


class TestReflectionTablesIntegration:
    """Integration tests for StarRocks table reflection from information_schema."""

    def test_reflect_table_options(self, engine: Engine):
        """Test that `get_table_options` correctly reflects all StarRocks table options."""
        table_name = "test_reflect_table_options"
        metadata = MetaData()

        # Define a table with various StarRocks-specific options
        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("name", String(32)),
            comment="Test table with all StarRocks options",
            starrocks_engine='OLAP',
            starrocks_primary_key='id',
            starrocks_partition_by="""RANGE(id) (
                PARTITION p1 VALUES LESS THAN ("10"),
                PARTITION p2 VALUES LESS THAN ("20"),
                PARTITION p3 VALUES LESS THAN (MAXVALUE)
            )""",
            starrocks_distributed_by='HASH(id) BUCKETS 8',
            starrocks_order_by='id, name',
            starrocks_properties={"replication_num": "1", "storage_medium": "SSD"},
        )

        with engine.connect() as connection:
            # Ensure the table is dropped before creation for a clean test environment
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            # Create the table in the test database
            table.create(connection)

            try:
                # Inspect the database to get table options
                inspector = inspect(engine)
                table_options = inspector.get_table_options(table_name)
                logger.info("table_options: %s", table_options)

                from test.test_utils import normalize_sql
                # Assertions for all expected StarRocks table options
                assert table_options[TableInfoKeyWithPrefix.ENGINE] == 'OLAP'
                assert normalize_sql(table_options[TableInfoKeyWithPrefix.PRIMARY_KEY]) == 'id'
                partition_info = table_options[TableInfoKeyWithPrefix.PARTITION_BY]
                assert partition_info is not None and isinstance(partition_info, ReflectionPartitionInfo)
                assert normalize_sql(partition_info.partition_method) == normalize_sql('RANGE(id)')
                assert "(PARTITION p1 VALUES" in normalize_sql(partition_info.pre_created_partitions)
                assert normalize_sql(table_options[TableInfoKeyWithPrefix.DISTRIBUTED_BY]) == normalize_sql('HASH(id) BUCKETS 8')
                assert normalize_sql(table_options[TableInfoKeyWithPrefix.ORDER_BY]) == normalize_sql("id, name")
                assert table_options[TableInfoKeyWithPrefix.PROPERTIES]['replication_num'] == '1'
                assert table_options[TableInfoKeyWithPrefix.PROPERTIES]['storage_medium'] == 'SSD'
                assert table_options[TableInfoKeyWithPrefix.COMMENT] == normalize_sql('Test table with all StarRocks options')

            finally:
                # Clean up: Drop the table after the test
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
