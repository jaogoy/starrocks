from starrocks.alembic.ops import (
    CreateViewOp, DropViewOp, AlterViewOp,
    CreateMaterializedViewOp, DropMaterializedViewOp,
    AlterTablePropertiesOp, AlterTableDistributionOp, AlterTableOrderOp
)
from starrocks.alembic.render import (
    _create_view, _drop_view, _alter_view,
    _create_materialized_view, _drop_materialized_view,
    _render_alter_table_properties, _render_alter_table_distribution, _render_alter_table_order
)
from unittest.mock import Mock
import re


def _normalize_py_call(s: str) -> str:
    # strip whitespace and collapse multiple spaces, ignore line breaks
    s = re.sub(r"\s+", " ", s).strip()
    s = re.sub(r" , ", ", ", s)
    s = re.sub(r" \)", ")", s)
    return s


class TestViewRendering:
    def test_render_create_view_basic(self):
        ctx = Mock()
        op = CreateViewOp("v1", "SELECT 1", schema=None, security=None, comment=None)
        rendered = _create_view(ctx, op)
        assert rendered == "op.create_view('v1', 'SELECT 1')"

    def test_render_create_view_with_schema(self):
        ctx = Mock()
        op = CreateViewOp("v1", "SELECT 1", schema="s1", security="DEFINER", comment="A test view")
        rendered = _create_view(ctx, op)
        # Use contains checks to be less sensitive to argument order
        assert "op.create_view('v1', 'SELECT 1'" in rendered
        assert "schema='s1'" in rendered
        assert "security='DEFINER'" in rendered
        assert "comment='A test view'" in rendered

    def test_render_drop_view_basic(self):
        ctx = Mock()
        op = DropViewOp("v1", schema="s1")
        rendered = _drop_view(ctx, op)
        assert rendered == "op.drop_view('v1', schema='s1')"

    def test_render_drop_view_no_schema(self):
        ctx = Mock()
        op = DropViewOp("v1", schema=None)
        rendered = _drop_view(ctx, op)
        assert rendered == "op.drop_view('v1')"

    def test_render_alter_view_with_options(self):
        ctx = Mock()
        op = AlterViewOp("v1", "SELECT 2", schema="s1", comment="cmt", security="DEFINER")
        rendered = _normalize_py_call(_alter_view(ctx, op))
        expected = _normalize_py_call("op.alter_view('v1', 'SELECT 2', schema='s1', comment='cmt', security='DEFINER')")
        assert rendered == expected

    def test_render_alter_view_minimal(self):
        ctx = Mock()
        op = AlterViewOp("v1", "SELECT 2")
        rendered = _normalize_py_call(_alter_view(ctx, op))
        expected = _normalize_py_call("op.alter_view('v1', 'SELECT 2')")
        assert rendered == expected

    def test_render_view_definition_with_special_chars(self):
        """Tests that a complex view definition with quotes, backticks, etc., is rendered correctly."""
        ctx = Mock()
        complex_sql = "SELECT `c1`, 'some_string', \"another_string\\n\" FROM `my_table` WHERE c2 = 'it\\'s complex'"
        op = CreateViewOp("v_complex", complex_sql, schema="s'1")
        rendered = _create_view(ctx, op)
        # repr() will handle all the escaping.
        expected_sql_repr = repr(complex_sql)
        expected = f"op.create_view('v_complex', {expected_sql_repr}, schema='s\\'1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)


class TestMaterializedViewRendering:
    def test_render_create_materialized_view(self):
        ctx = Mock()
        op = CreateMaterializedViewOp(
            "mv1",
            "SELECT id, name FROM t1",
            properties={"replication_num": "1"},
            schema="s1"
        )
        rendered = _create_materialized_view(ctx, op)
        expected = "op.create_materialized_view('mv1', 'SELECT id, name FROM t1', properties={'replication_num': '1'}, schema='s1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_create_materialized_view_no_options(self):
        ctx = Mock()
        op = CreateMaterializedViewOp(
            "mv1",
            "SELECT id, name FROM t1",
            properties=None,
            schema=None
        )
        rendered = _create_materialized_view(ctx, op)
        expected = "op.create_materialized_view('mv1', 'SELECT id, name FROM t1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_drop_materialized_view(self):
        ctx = Mock()
        op = DropMaterializedViewOp("mv1", schema="s1")
        rendered = _drop_materialized_view(ctx, op)
        assert rendered == "op.drop_materialized_view('mv1', schema='s1')"

        # without schema
        op = DropMaterializedViewOp("mv1", schema=None)
        rendered = _drop_materialized_view(ctx, op)
        assert rendered == "op.drop_materialized_view('mv1')"


class TestTableRendering:
    def test_render_alter_table_distribution(self):
        ctx = Mock()
        op = AlterTableDistributionOp("t1", "HASH(k1, k2)", buckets=10, schema="s1")
        rendered = _render_alter_table_distribution(ctx, op)
        expected = "op.alter_table_distribution('t1', 'HASH(k1, k2)', buckets=10, schema='s1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        # No schema
        op = AlterTableDistributionOp("t1", "HASH(k1, k2)", buckets=10, schema=None)
        rendered = _render_alter_table_distribution(ctx, op)
        expected = "op.alter_table_distribution('t1', 'HASH(k1, k2)', buckets=10)"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        # No buckets
        op = AlterTableDistributionOp("t1", "RANDOM", buckets=None, schema="s1")
        rendered = _render_alter_table_distribution(ctx, op)
        expected = "op.alter_table_distribution('t1', 'RANDOM', schema='s1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        # Buckets is 0
        op = AlterTableDistributionOp("t1", "RANDOM", buckets=0, schema="s1")
        rendered = _render_alter_table_distribution(ctx, op)
        expected = "op.alter_table_distribution('t1', 'RANDOM', buckets=0, schema='s1')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

        # No buckets and no schema
        op = AlterTableDistributionOp("t1", "RANDOM", buckets=None, schema=None)
        rendered = _render_alter_table_distribution(ctx, op)
        expected = "op.alter_table_distribution('t1', 'RANDOM')"
        assert _normalize_py_call(rendered) == _normalize_py_call(expected)

    def test_render_alter_table_order(self):
        ctx = Mock()
        op = AlterTableOrderOp("t1", "k1, k2", schema="s1")
        rendered = _render_alter_table_order(ctx, op)
        assert rendered == "op.alter_table_order('t1', 'k1, k2', schema='s1')"

        op = AlterTableOrderOp("t1", "k1", schema=None)
        rendered = _render_alter_table_order(ctx, op)
        assert rendered == "op.alter_table_order('t1', 'k1')"

    def test_render_alter_table_properties(self):
        ctx = Mock()
        op = AlterTablePropertiesOp("t1", {"replication_num": "1", "storage_medium": "SSD"}, schema="s1")
        rendered = _render_alter_table_properties(ctx, op)
        # repr() on dict is order-sensitive in older pythons, so check keys/values
        assert "op.alter_table_properties('t1', " in rendered
        assert "'replication_num': '1'" in rendered
        assert "'storage_medium': 'SSD'" in rendered
        assert "schema='s1'" in rendered

        # With properties and no schema
        op = AlterTablePropertiesOp("t1", {"replication_num": "1"}, schema=None)
        rendered = _render_alter_table_properties(ctx, op)
        assert "op.alter_table_properties('t1', {'replication_num': '1'})" in rendered

        # With schema and no properties
        op = AlterTablePropertiesOp("t1", {}, schema="s1")
        rendered = _render_alter_table_properties(ctx, op)
        assert "op.alter_table_properties('t1', {}, schema='s1')" in rendered

        # With no properties and no schema
        op = AlterTablePropertiesOp("t1", {}, schema=None)
        rendered = _render_alter_table_properties(ctx, op)
        assert "op.alter_table_properties('t1', {})" in rendered
