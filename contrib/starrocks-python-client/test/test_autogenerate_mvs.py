from alembic.testing import eq_
from alembic.operations import ops
from sqlalchemy import MetaData
from starrocks.sql.schema import MaterializedView
from starrocks.alembic.compare import autogen_for_materialized_views
from starrocks.alembic.ops import (
    CreateMaterializedViewOp, DropMaterializedViewOp
)
from unittest.mock import Mock


class TestAutogenerateMV:
    def setup_method(self, method):
        self.mock_inspector = Mock()
        self.mock_autogen_context = Mock()
        self.mock_autogen_context.inspector = self.mock_inspector
        self.mock_autogen_context.opts = {
            'include_object': None,
            'include_name': None,
        }
        self.mock_autogen_context.dialect = Mock()
        self.mock_autogen_context.dialect.default_schema_name = None

    def test_create_mv_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = []
        m2 = MetaData()
        mv = MaterializedView('my_test_mv', 'SELECT 1')
        m2.info['materialized_views'] = {(mv, None): mv}
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op: CreateMaterializedViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'CreateMaterializedViewOp')
        eq_(op.view_name, 'my_test_mv')
        eq_(op.definition, 'SELECT 1')

    def test_drop_mv_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = ['my_test_mv']
        m2 = MetaData()
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op: DropMaterializedViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'DropMaterializedViewOp')
        eq_(op.view_name, 'my_test_mv')

    def test_modify_mv_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_materialized_view_names.return_value = ['my_test_mv']
        self.mock_inspector.get_materialized_view_definition.return_value = 'SELECT 1'
        m2 = MetaData()
        mv2 = MaterializedView('my_test_mv', 'SELECT 2')
        m2.info['materialized_views'] = {(mv2, None): mv2}
        self.mock_autogen_context.metadata = m2
        autogen_for_materialized_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op_tuple = upgrade_ops.ops[0]
        eq_(len(op_tuple), 2)
        drop_op, create_op = op_tuple
        eq_(drop_op.__class__.__name__, 'DropMaterializedViewOp')
        eq_(create_op.__class__.__name__, 'CreateMaterializedViewOp')
        eq_(create_op.view_name, 'my_test_mv')
        eq_(create_op.definition, 'SELECT 2')
