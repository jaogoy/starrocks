import logging
from sqlalchemy.dialects import registry

from starrocks.sql.ddl import CreateMaterializedView, DropMaterializedView
from starrocks.sql.schema import MaterializedView
from test.test_utils import _normalize_sql


class TestMaterializedViewCompiler:
    @classmethod
    def setup_class(cls):
        cls.logger = logging.getLogger(__name__)
        cls.dialect = registry.load("starrocks")()

    def test_create_materialized_view(self):
        mv = MaterializedView("my_mv", "SELECT * FROM my_table")
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = "CREATE MATERIALIZED VIEW my_mv AS SELECT * FROM my_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

    def test_create_materialized_view_with_properties(self):
        properties = {"replication_num": "1"}
        mv = MaterializedView("my_mv", "SELECT * FROM my_table", properties=properties)
        sql = str(CreateMaterializedView(mv).compile(dialect=self.dialect))
        expected = 'PROPERTIES ("replication_num" = "1")'
        assert expected in sql

    def test_drop_materialized_view(self):
        mv = MaterializedView("my_mv", "SELECT * FROM my_table")
        sql = str(DropMaterializedView(mv).compile(dialect=self.dialect))
        expected = "DROP MATERIALIZED VIEW IF EXISTS my_mv"
        assert _normalize_sql(sql) == _normalize_sql(expected)


