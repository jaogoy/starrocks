from sqlalchemy import text, Table, MetaData, Column, Integer, String, inspect
from sqlalchemy.engine import Engine

from starrocks.datatype import BITMAP, HLL
from starrocks.params import ColumnAggInfoKeyWithPrefix, ColumnSROptionsKey
from starrocks.types import ColumnAggType


class TestReflectionAggIntegration:

    def test_reflect_aggregate_key_table(self, engine: Engine):
        table_name = "test_reflect_agg"
        metadata = MetaData()

        table = Table(
            table_name,
            metadata,
            Column("id", Integer, primary_key=True),
            Column("key1", String(32)),
            Column("val_sum", Integer, info={ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.SUM}),
            # Column("val_count", Integer, info={ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.COUNT}),
            Column("val_min", Integer, info={ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.MIN}),
            Column("val_max", Integer, info={ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.MAX}),

            Column("val_replace", String(32), info={ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.REPLACE}),
            Column(
                "val_replace_if_not_null",
                Integer,
                info={ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.REPLACE_IF_NOT_NULL},
            ),
            Column("val_hll", HLL, info={ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.HLL_UNION}),
            Column("val_bitmap", BITMAP, info={ColumnAggInfoKeyWithPrefix.AGG_TYPE: ColumnAggType.BITMAP_UNION}),
            starrocks_engine='OLAP',
            starrocks_aggregate_key='id, key1',
            starrocks_distributed_by='HASH(id)',
            starrocks_properties={"replication_num": "1"},
        )

        with engine.connect() as connection:
            connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
            table.create(connection)

            try:
                inspector = inspect(engine)
                reflected_columns = inspector.get_columns(table_name)

                # Create a map of column name to its reflected info dict
                reflected_map = {col["name"]: col for col in reflected_columns}

                # Assertions
                assert ColumnAggInfoKeyWithPrefix.AGG_TYPE not in reflected_map["id"].get(ColumnSROptionsKey, {})
                assert ColumnAggInfoKeyWithPrefix.AGG_TYPE not in reflected_map["key1"].get(ColumnSROptionsKey, {})
                assert reflected_map["val_sum"][ColumnSROptionsKey][ColumnAggInfoKeyWithPrefix.AGG_TYPE] == ColumnAggType.SUM
                assert reflected_map["val_replace"][ColumnSROptionsKey][ColumnAggInfoKeyWithPrefix.AGG_TYPE] == ColumnAggType.REPLACE
                assert (
                    reflected_map["val_replace_if_not_null"][ColumnSROptionsKey][ColumnAggInfoKeyWithPrefix.AGG_TYPE]
                    == ColumnAggType.REPLACE_IF_NOT_NULL
                )
                assert reflected_map["val_min"][ColumnSROptionsKey][ColumnAggInfoKeyWithPrefix.AGG_TYPE] == ColumnAggType.MIN
                assert reflected_map["val_max"][ColumnSROptionsKey][ColumnAggInfoKeyWithPrefix.AGG_TYPE] == ColumnAggType.MAX
                assert reflected_map["val_hll"][ColumnSROptionsKey][ColumnAggInfoKeyWithPrefix.AGG_TYPE] == ColumnAggType.HLL_UNION
                assert (
                    reflected_map["val_bitmap"][ColumnSROptionsKey][ColumnAggInfoKeyWithPrefix.AGG_TYPE]
                    == ColumnAggType.BITMAP_UNION
                )

            finally:
                # Clean up
                connection.execute(text(f"DROP TABLE IF EXISTS {table_name}"))
