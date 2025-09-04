import logging

from starrocks.alembic.render import _add_view, _add_materialized_view
from starrocks.alembic.ops import CreateViewOp, CreateMaterializedViewOp

logger = logging.getLogger(__name__)


class TestRender:
    def setup_method(self, method):
        self.context = None  # Mock or simple object if needed

    def test_render_create_view(self):
        op = CreateViewOp("my_view", "SELECT 1", schema="my_schema")
        rendered = _add_view(self.context, op)
        expected = "op.create_view('my_view', 'SELECT 1', schema='my_schema')"
        assert rendered == expected

    def test_render_create_materialized_view(self):
        op = CreateMaterializedViewOp(
            "my_mv",
            "SELECT 1",
            properties={"replication_num": "1"},
            schema="my_schema"
        )
        rendered = _add_materialized_view(self.context, op)
        expected = "op.create_materialized_view('my_mv', 'SELECT 1', properties={'replication_num': '1'}, schema='my_schema')"
        assert rendered == expected
