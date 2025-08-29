import pytest
from alembic.testing import eq_
from alembic.operations import ops
from sqlalchemy import MetaData
from starrocks.datatype import logger
from starrocks.sql.schema import View
from starrocks.alembic.compare import autogen_for_views
from starrocks.reflection import ReflectionViewInfo
from starrocks.alembic.ops import (
    CreateViewOp, DropViewOp, AlterViewOp,
)
from unittest.mock import Mock
from alembic.config import Config
from sqlalchemy import create_engine, inspect, text
import os
from alembic.autogenerate import api
from alembic.operations import Operations
from alembic.runtime.migration import MigrationContext
from typing import Generator, Any
from sqlalchemy import Engine

from test import conftest_sr
from test.test_utils import _normalize_sql


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
        self.mock_autogen_context.dialect.default_schema_name = None

    def test_create_view_autogenerate(self):
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
        self.mock_inspector.get_view.return_value = ReflectionViewInfo(
            name="my_test_view", definition="SELECT 1"
        )
        m2 = MetaData()
        view2 = View('my_test_view', 'SELECT 2')
        m2.info['views'] = {(view2, None): view2}
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
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_secure_view']
        self.mock_inspector.get_view.return_value = ReflectionViewInfo(
            name="my_secure_view", definition="SELECT 1"
        )
        m2 = MetaData()
        view2 = View('my_secure_view', 'SELECT 1', security='INVOKER')
        m2.info['views'] = {(view2, None): view2}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        # StarRocks cannot alter security via ALTER VIEW; expect no ops
        eq_(len(upgrade_ops.ops), 0)

    def test_no_change_view_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        self.mock_inspector.get_view.return_value = ReflectionViewInfo(
            name="my_test_view", definition="SELECT 1 AS `val`"
        )
        m2 = MetaData()
        view = View('my_test_view', 'SELECT 1 AS val', comment=None, security=None)
        m2.info['views'] = {(view, None): view}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        eq_(len(upgrade_ops.ops), 0)

    def test_modify_view_comment_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        self.mock_inspector.get_view.return_value = ReflectionViewInfo(
            name="my_test_view", definition="SELECT 1"
        )
        m2 = MetaData()
        view2 = View('my_test_view', 'SELECT 1', comment='New comment')
        m2.info['views'] = {(view2, None): view2}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        # StarRocks cannot alter comment via ALTER VIEW; expect no ops
        eq_(len(upgrade_ops.ops), 0)

    def test_modify_view_security_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        self.mock_inspector.get_view.return_value = ReflectionViewInfo(
            name="my_test_view", definition="SELECT 1", security='INVOKER'
        )
        m2 = MetaData()
        view2 = View('my_test_view', 'SELECT 1', security='DEFINER')
        m2.info['views'] = {(view2, None): view2}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        # StarRocks cannot alter security via ALTER VIEW; expect no ops
        eq_(len(upgrade_ops.ops), 0)

    def test_remove_view_security_autogenerate(self):
        upgrade_ops = ops.UpgradeOps([])
        self.mock_inspector.get_view_names.return_value = ['my_test_view']
        self.mock_inspector.get_view.return_value = ReflectionViewInfo(
            name="my_test_view", definition="SELECT 1", security='INVOKER'
        )
        m2 = MetaData()
        view2 = View('my_test_view', 'SELECT 1', security=None)
        m2.info['views'] = {(view2, None): view2}
        self.mock_autogen_context.metadata = m2
        autogen_for_views(self.mock_autogen_context, upgrade_ops, [None])
        # StarRocks cannot alter security via ALTER VIEW; expect no ops
        eq_(len(upgrade_ops.ops), 0)


class TestIntegrationViews:
    STARROCKS_URI = conftest_sr._get_starrocks_url()
    engine: Engine = create_engine(STARROCKS_URI)

    @classmethod
    def teardown_class(cls):
        cls.engine.dispose()

    @pytest.fixture(scope="function")
    def alembic_env(self) -> Generator[Config, Any, None]:
        script_dir_path = "test_alembic_env"
        import shutil
        if os.path.exists(script_dir_path):
            shutil.rmtree(script_dir_path)
        os.makedirs(script_dir_path)
        shutil.copy("test/data/autogen_env.py", os.path.join(script_dir_path, "env.py"))
        config = Config()
        config.set_main_option("script_location", script_dir_path)
        config.set_main_option("sqlalchemy.url", TestIntegrationViews.STARROCKS_URI)
        yield config
        shutil.rmtree(script_dir_path)

    def test_full_autogenerate_and_upgrade(self, alembic_env: Config) -> None:
        config: Config = alembic_env
        engine = self.engine
        view_name = "integration_test_view"
        with engine.connect() as conn:
            conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
            try:
                target_metadata = MetaData()
                view = View(view_name, "SELECT 1 AS val", comment="Integration test view")
                target_metadata.info['views'] = {(view, None): view}
                mc: MigrationContext = MigrationContext.configure(connection=conn)
                migration_script: api.MigrationScript = api.produce_migrations(mc, target_metadata)
                assert len(migration_script.upgrade_ops.ops) == 1
                create_op: CreateViewOp = migration_script.upgrade_ops.ops[0]
                assert isinstance(create_op, CreateViewOp)
                assert create_op.view_name == view_name
                op = Operations(mc)
                for op_item in migration_script.upgrade_ops.ops:
                    op.invoke(op_item)
                inspector = inspect(conn)
                views: list[str] = inspector.get_view_names()
                logger.info(f"inspected created views : {views}")
                assert view_name in views
                for op_item in migration_script.downgrade_ops.ops:
                    op.invoke(op_item)
                inspector = inspect(conn)
                views = inspector.get_view_names()
                assert view_name not in views
            finally:
                conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))

    def test_full_autogenerate_and_alter(self, alembic_env: Config) -> None:
        config: Config = alembic_env
        engine = self.engine
        view_name = "integration_test_alter_view"
        with engine.connect() as conn:
            initial_ddl = f"""
            CREATE OR REPLACE VIEW {view_name}
            (c1 COMMENT 'col 1')
            COMMENT 'Initial version'
            SECURITY INVOKER
            AS SELECT 1 AS c1
            """
            conn.execute(text(initial_ddl))
            try:
                target_metadata = MetaData()
                altered_view = View(
                    view_name,
                    "SELECT 2 AS new_c1, 3 AS new_c2",
                    comment="Altered version",
                    security="DEFINER",
                    columns=[
                        {'name': 'new_c1', 'comment': 'new col 1'},
                        {'name': 'new_c2', 'comment': 'new col 2'},
                    ]
                )
                target_metadata.info['views'] = {(altered_view, None): altered_view}
                mc: MigrationContext = MigrationContext.configure(connection=conn)
                migration_script = api.produce_migrations(mc, target_metadata)
                assert len(migration_script.upgrade_ops.ops) == 1
                op_item = migration_script.upgrade_ops.ops[0]
                assert isinstance(op_item, AlterViewOp)
                assert op_item.view_name == view_name
                op = Operations(mc)
                for op_to_run in migration_script.upgrade_ops.ops:
                    op.invoke(op_to_run)
                inspector = inspect(conn)
                view_info = inspector.get_view(view_name)
                assert view_info is not None
                logger.info(f"view_info.definition: {view_info.definition}")
                assert _normalize_sql("SELECT 2 AS new_c1, 3 AS new_c2") == _normalize_sql(view_info.definition)
            finally:
                conn.execute(text(f"DROP VIEW IF EXISTS {view_name}"))
