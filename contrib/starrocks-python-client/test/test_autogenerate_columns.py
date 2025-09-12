import logging
import pytest
from alembic.autogenerate.api import AutogenContext
from unittest.mock import Mock

from sqlalchemy.exc import NotSupportedError

from starrocks.alembic.compare import (
    compare_starrocks_column_agg_type,
    compare_starrocks_column_autoincrement,
)
from starrocks.params import ColumnAggInfoKey, DialectName
from starrocks.types import ColumnAggType

logger = logging.getLogger(__name__)


class TestAutogenerateColumnsAggType:
    def test_column_compare_agg_type_logs_options(self, caplog):
        autogen_context = Mock(spec=AutogenContext)
        autogen_context.dialect = Mock()
        autogen_context.dialect.name = DialectName
        caplog.set_level("WARNING")

        # --- Test Case 1: Columns match ---
        conn_col = Mock(
            name="val1",
            dialect_options={
                DialectName: {
                    ColumnAggInfoKey.AGG_TYPE: ColumnAggType.SUM
                }
            }
        )
        meta_col = Mock(
            name="val1",
            dialect_options={
                DialectName: {
                    ColumnAggInfoKey.AGG_TYPE: ColumnAggType.SUM
                }
            }
        )

        caplog.clear()
        compare_starrocks_column_agg_type(autogen_context, None, "test_schema", "t1", "val1", conn_col, meta_col)
        assert not caplog.records

        # --- Test Case 2: Columns differ ---
        meta_col_diff = Mock(
            name="val1",
            dialect_options={
                DialectName: {
                    ColumnAggInfoKey.AGG_TYPE: ColumnAggType.REPLACE
                }
            }
        )

        caplog.clear()
        with pytest.raises(NotSupportedError) as exc_info:
            compare_starrocks_column_agg_type(autogen_context, None, "test_schema", "t1", "val1", conn_col, meta_col_diff)
        logger.debug(f"exc_info.value: {exc_info.value}")
        assert (
            "StarRocks does not support changing the aggregation type of a column"
            in str(exc_info.value)
        )

class TestAutogenerateColumnsAutoIncrement:
    def _mk_context(self) -> AutogenContext:
        ctx = Mock(spec=AutogenContext)
        ctx.dialect = Mock()
        ctx.dialect.name = DialectName
        return ctx

    def test_autoincrement_same_noop(self):
        ctx = self._mk_context()

        # conn and meta both True
        conn_col = Mock(autoincrement=True)
        meta_col = Mock(autoincrement=True)

        # should not raise
        compare_starrocks_column_autoincrement(ctx, None, "sch", "t1", "id", conn_col, meta_col)

        # conn and meta both False
        conn_col2 = Mock(autoincrement=False)
        meta_col2 = Mock(autoincrement=False)

        # should not raise
        compare_starrocks_column_autoincrement(ctx, None, "sch", "t1", "id", conn_col2, meta_col2)

    def test_autoincrement_diff_raises(self):
        ctx = self._mk_context()

        # conn True -> meta False
        conn_col = Mock(autoincrement=True)
        meta_col = Mock(autoincrement=False)

        with pytest.raises(NotSupportedError) as ei1:
            compare_starrocks_column_autoincrement(ctx, None, "sch", "t1", "id", conn_col, meta_col)
        assert "does not support changing the autoincrement" in str(ei1.value)

        # conn False -> meta True
        conn_col2 = Mock(autoincrement=False)
        meta_col2 = Mock(autoincrement=True)

        with pytest.raises(NotSupportedError) as ei2:
            compare_starrocks_column_autoincrement(ctx, None, "sch", "t1", "id", conn_col2, meta_col2)
        assert "does not support changing the autoincrement" in str(ei2.value)
