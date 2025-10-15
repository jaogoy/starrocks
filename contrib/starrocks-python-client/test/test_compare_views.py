from alembic.testing import eq_
from alembic.operations import ops
from sqlalchemy import MetaData
from starrocks.sql.schema import View
from starrocks.alembic.compare import autogen_for_views
from starrocks.engine.interfaces import ReflectedViewState
from starrocks.alembic.ops import (
    CreateViewOp, DropViewOp, AlterViewOp,
)
from unittest.mock import Mock


class TestAutogenerateViews:
    def setup_method(self, method):
        self.mock_inspector = Mock()
        self.mock_autogen_context = Mock()
        self.mock_autogen_context.inspector = self.mock_inspector
        self.mock_autogen_context.opts = {
            'include_object': None,
            'include_name': None,
        }
        self.mock_autogen_context.dialect = Mock()
        self.mock_autogen_context.dialect.name = 'starrocks'
        self.mock_autogen_context.dialect.default_schema_name = None

    def test_create_view_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = []
        m2 = MetaData()
        view = View('my_test_view', 'SELECT 1', m2)
        m2.info['views'] = {(view.schema, view.name): view}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op: CreateViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'CreateViewOp')
        eq_(op.view_name, 'my_test_view')
        eq_(op.definition, 'SELECT 1')

    def test_drop_view_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        m2 = MetaData()
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op: DropViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'DropViewOp')
        eq_(op.view_name, 'my_test_view')

    def test_modify_view_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        self.mock_inspector.get_view.return_value = ReflectedViewState(
            name="my_test_view", definition="SELECT 1"
        )
        m2 = MetaData()
        view2 = View('my_test_view', 'SELECT 2', m2)
        m2.info['views'] = {(view2.schema, view2.name): view2}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op: AlterViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'AlterViewOp')
        eq_(op.view_name, 'my_test_view')
        eq_(op.definition, 'SELECT 2')

    def test_create_view_with_security(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = []
        m2 = MetaData()
        view = View('my_secure_view', 'SELECT 1', m2, security='INVOKER')
        m2.info['views'] = {(view.schema, view.name): view}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 1)
        op: CreateViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'CreateViewOp')
        eq_(op.view_name, 'my_secure_view')
        eq_(op.security, 'INVOKER')

    def test_modify_view_add_security(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_secure_view']
        self.mock_inspector.get_view.return_value = ReflectedViewState(
            name="my_secure_view", definition="SELECT 1"
        )
        m2 = MetaData()
        view2 = View('my_secure_view', 'SELECT 1', m2, security='INVOKER')
        m2.info['views'] = {(view2.schema, view2.name): view2}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        # StarRocks cannot alter security via ALTER VIEW; expect no ops
        eq_(len(upgrade_ops.ops), 0)

    def test_no_change_view_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        self.mock_inspector.get_view.return_value = ReflectedViewState(
            name="my_test_view", definition="SELECT 1 AS `val`"
        )
        m2 = MetaData()
        view = View('my_test_view', 'SELECT 1 AS val', m2, comment=None, security=None)
        m2.info['views'] = {(view.schema, view.name): view}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 0)

    def test_modify_view_comment_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        self.mock_inspector.get_view.return_value = ReflectedViewState(
            name="my_test_view", definition="SELECT 1"
        )
        m2 = MetaData()
        view2 = View('my_test_view', 'SELECT 1', m2, comment='New comment')
        m2.info['views'] = {(view2.schema, view2.name): view2}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        # StarRocks cannot alter comment via ALTER VIEW; expect no ops
        eq_(len(upgrade_ops.ops), 0)

    def test_modify_view_security_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        self.mock_inspector.get_view.return_value = ReflectedViewState(
            name="my_test_view", definition="SELECT 1", security='INVOKER'
        )
        m2 = MetaData()
        view2 = View('my_test_view', 'SELECT 1', m2, security='DEFINER')
        m2.info['views'] = {(view2.schema, view2.name): view2}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        # StarRocks cannot alter security via ALTER VIEW; expect no ops
        eq_(len(upgrade_ops.ops), 0)

    def test_remove_view_security_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        self.mock_inspector.get_view.return_value = ReflectedViewState(
            name="my_test_view", definition="SELECT 1", security='INVOKER'
        )
        m2 = MetaData()
        view2 = View('my_test_view', 'SELECT 1', m2, security=None)
        m2.info['views'] = {(view2.schema, view2.name): view2}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        # StarRocks cannot alter security via ALTER VIEW; expect no ops
        eq_(len(upgrade_ops.ops), 0)
