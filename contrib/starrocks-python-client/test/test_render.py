from starrocks.alembic.ops import CreateViewOp, DropViewOp, AlterViewOp
from starrocks.alembic.render import _add_view, _drop_view, _alter_view
from unittest.mock import Mock
import re


def test_render_create_view_basic():
    ctx = Mock()
    op = CreateViewOp("v1", "SELECT 1", schema=None, security=None, comment=None)
    rendered = _add_view(ctx, op)
    assert rendered.startswith("op.create_view('v1', 'SELECT 1'")


def test_render_drop_view_basic():
    ctx = Mock()
    op = DropViewOp("v1", schema=None)
    rendered = _drop_view(ctx, op)
    assert rendered == "op.drop_view('v1', schema='None')"


def _normalize_py_call(s: str) -> str:
    # strip whitespace and collapse multiple spaces, ignore line breaks
    return re.sub(r"\s+", " ", s).strip()


def test_render_alter_view_with_options():
    ctx = Mock()
    op = AlterViewOp("v1", "SELECT 2", schema="s1", comment="cmt", security="DEFINER")
    rendered = _normalize_py_call(_alter_view(ctx, op))
    expected = _normalize_py_call("op.alter_view('v1', 'SELECT 2', schema='s1', comment='cmt', security='DEFINER')")
    assert rendered == expected


