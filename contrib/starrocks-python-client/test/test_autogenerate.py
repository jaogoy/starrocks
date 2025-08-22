import pytest
from alembic.testing import eq_
from alembic.operations import ops
from sqlalchemy import MetaData
from starrocks.sql.schema import View, MaterializedView
from starrocks.alembic.compare import autogen_for_views, autogen_for_materialized_views
from starrocks.alembic.ops import (
    CreateViewOp, DropViewOp,
    CreateMaterializedViewOp, DropMaterializedViewOp
)
from unittest.mock import Mock
from alembic.command import revision, upgrade, downgrade
from alembic.config import Config
from alembic.script import ScriptDirectory
from sqlalchemy import create_engine, inspect
import os

class TestAutogenerate:
    def setup_method(self, method):
        """Set up a mock Alembic environment."""
        self.mock_inspector = Mock()
        
        self.mock_autogen_context = Mock()
        self.mock_autogen_context.inspector = self.mock_inspector
        self.mock_autogen_context.opts = {
            'include_object': None,
            'include_name': None,
        }
        self.mock_autogen_context.dialect = Mock()
        self.mock_autogen_context.dialect.default_schema_name = None

    def test_create_view_autogenerate(self):
        """Test that autogen_for_views detects a new view."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = []
        
        m2 = MetaData()
        view = View('my_test_view', 'SELECT 1')
        m2.info['views'] = {(view, None): view}
        self.mock_autogen_context.metadata = m2

        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        
        eq_(len(upgrade_ops.ops), 1)
        op: CreateViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'CreateViewOp')
        eq_(op.view_name, 'my_test_view')
        eq_(op.definition, 'SELECT 1')

    def test_drop_view_autogenerate(self):
        """Test that autogen_for_views detects a dropped view."""
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
        """Test that autogen_for_views detects a modified view."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        self.mock_inspector.get_view_definition.return_value = 'SELECT 1'

        m2 = MetaData()
        view2 = View('my_test_view', 'SELECT 2')
        m2.info['views'] = {(view2, None): view2}
        self.mock_autogen_context.metadata = m2

        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        
        eq_(len(upgrade_ops.ops), 1)
        op_tuple = upgrade_ops.ops[0]
        eq_(len(op_tuple), 2)
        
        drop_op, create_op = op_tuple  # TODO: here is a tuple of two ops, need to change it to an ALTER VIEW stmt
        
        eq_(drop_op.__class__.__name__, 'DropViewOp')
        eq_(drop_op.view_name, 'my_test_view')
        
        eq_(create_op.__class__.__name__, 'CreateViewOp')
        eq_(create_op.view_name, 'my_test_view')
        eq_(create_op.definition, 'SELECT 2')

    def test_create_view_with_security(self):
        """Test autogen_for_views detects a new view with a SECURITY clause."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = []
        
        m2 = MetaData()
        view = View('my_secure_view', 'SELECT 1', security='INVOKER')
        m2.info['views'] = {(view, None): view}
        self.mock_autogen_context.metadata = m2

        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        
        eq_(len(upgrade_ops.ops), 1)
        op: CreateViewOp = upgrade_ops.ops[0]
        eq_(op.__class__.__name__, 'CreateViewOp')
        eq_(op.view_name, 'my_secure_view')
        eq_(op.security, 'INVOKER')

    def test_modify_view_add_security(self):
        """Test autogen_for_views detects adding a SECURITY clause to a view."""
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_secure_view']
        self.mock_inspector.get_view_definition.return_value = 'SELECT 1'

        m2 = MetaData()
        view2 = View('my_secure_view', 'SELECT 1', security='INVOKER')
        m2.info['views'] = {(view2, None): view2}
        self.mock_autogen_context.metadata = m2

        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        
        eq_(len(upgrade_ops.ops), 1)
        op_tuple = upgrade_ops.ops[0]
        eq_(len(op_tuple), 2)
        
        drop_op, create_op = op_tuple
        
        eq_(drop_op.__class__.__name__, 'DropViewOp')
        eq_(create_op.__class__.__name__, 'CreateViewOp')
        eq_(create_op.view_name, 'my_secure_view')
        eq_(create_op.security, 'INVOKER')

# This is a placeholder for more advanced tests that require a live DB
# We will need to set up a proper testing database and configuration for this.
@pytest.mark.skip(reason="Requires live database connection")
def test_full_autogenerate_and_upgrade():
    # 1. Setup a testing database
    # 2. Define initial state (e.g., empty or with some tables)
    # 3. Define target state in MetaData (e.g., add a View)
    # 4. Run alembic revision --autogenerate
    # 5. Check the generated script's content
    # 6. Run alembic upgrade head
    # 7. Use Inspector to check if the view now exists in the database
    pass


class TestAutogenerateMV:
    # TODO: currently, it's very simple, need to add more tests
    def setup_method(self, method):
        """Set up a mock Alembic environment for MVs."""
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
        """Test that autogen_for_materialized_views detects a new MV."""
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
        """Test that autogen_for_materialized_views detects a dropped MV."""
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
        """Test that autogen_for_materialized_views detects a modified MV."""
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
