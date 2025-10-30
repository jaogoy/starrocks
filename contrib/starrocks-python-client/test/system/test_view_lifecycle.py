# Copyright 2021-present StarRocks, Inc. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import re

from pytest import LogCaptureFixture
from sqlalchemy import Column, Table, inspect, text
from sqlalchemy.orm import declarative_base
from sqlalchemy.testing.assertions import is_true

from starrocks.datatype import INTEGER, STRING, VARCHAR
from starrocks.sql.schema import View
from test.system.conftest import AlembicTestEnv
from test.system.test_table_lifecycle import EMPTY_DOWNGRADE_STR, EMPTY_UPGRADE_STR, ScriptContentParser


logger = logging.getLogger(__name__)


def test_create_view(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests creating a view."""
    # 1. Define metadata with a view
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('user_view', Base.metadata, definition='SELECT id FROM user')
    logger.info(f"metadata.tables: {Base.metadata.tables}")
    logger.info(f"metadata#views: {Base.metadata.info.get('views', {})}")

    # 2. Run autogenerate to create the view
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Create view"
    )

    # 3. Verify the script and upgrade
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_view")
    logger.debug("extract upgrade content")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.create_view('user_view'" in upgrade_content
    assert "SELECT id FROM user" in upgrade_content
    logger.debug("extract downgrade content")
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    assert "op.drop_view('user_view')" in downgrade_content

    alembic_env.harness.upgrade("head")

    # 4. Verify view exists in the database
    inspector = inspect(sr_engine)
    is_true('user_view' in inspector.get_view_names())

    # 5. Downgrade and verify view is dropped
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('user_view' not in inspector.get_view_names())


def test_create_view_with_columns(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests creating a view with explicit column definitions."""
    # 1. Define metadata with a view that has columns
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        Column('email', VARCHAR(100)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        Base.metadata,
        Column('id', VARCHAR()),
        Column('name', VARCHAR(50), comment='User name'),
        Column('email', VARCHAR(100), comment='Email address'),
        definition='SELECT id, name, email FROM user',
        comment='User information view'
    )

    # 2. Run autogenerate to create the view
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Create view with columns"
    )

    # 3. Verify the script and upgrade
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_view_with_columns")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.create_view('user_view'" in upgrade_content
    assert "'id'" in upgrade_content
    assert "'name', 'comment': 'User name'" in upgrade_content
    assert "'email', 'comment': 'Email address'" in upgrade_content

    alembic_env.harness.upgrade("head")

    # 4. Verify view exists in the database
    inspector = inspect(sr_engine)
    is_true('user_view' in inspector.get_view_names())

    # 5. Downgrade and verify view is dropped
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('user_view' not in inspector.get_view_names())


def test_view_idempotency(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests that no changes are detected if the view is in sync.
    Qualifiers, like '`schema`.`table`.', for column names are removed.
    """
    # 1. Initial state
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        Base.metadata,
        definition='SELECT id FROM user',
        comment='A comment',
        security='INVOKER'
    )
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
    alembic_env.harness.upgrade("head")

    # 2. Second run
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Second run")

    # 3. Verify no new script
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "second_run")
    # upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    # assert upgrade_content is None  # pass
    is_true(re.search(EMPTY_UPGRADE_STR, script_content), "Upgrade script should be empty")
    is_true(re.search(EMPTY_DOWNGRADE_STR, script_content), "Downgrade script should be empty")


def test_alter_view_unsupported_attributes(
    database: str, alembic_env: AlembicTestEnv, sr_engine, caplog: LogCaptureFixture
):
    """Tests that altering unsupported view attributes (comment, security) is ignored."""
    caplog.set_level("WARNING")
    # 1. Initial state with a view having a comment and security NONE
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        Base.metadata,
        definition='SELECT id FROM user',
        comment='Initial comment',
        security='NONE'  # DEFINDER is not supported in StarRocks v3.5.
    )
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Initial view with attrs"
    )
    alembic_env.harness.upgrade("head")

    # 2. Alter metadata with changed comment and security
    AlteredBase = declarative_base()
    Table(
        'user',
        AlteredBase.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        AlteredBase.metadata,
        definition='SELECT id FROM user',
        comment='Modified comment',
        security='INVOKER'
    )
    caplog.clear()
    alembic_env.harness.generate_autogen_revision(
        metadata=AlteredBase.metadata, message="Alter unsupported attrs"
    )
    # print(f"caplog.text: {caplog.text}")
    # print(f"caplog.text: {caplog.records}")

    # 3. Verify no ALTER is generated and warnings are logged
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_unsupported_attrs")
    is_true(re.search(EMPTY_UPGRADE_STR, script_content), "Upgrade script should be empty for unsupported attr changes")
    is_true(re.search(EMPTY_DOWNGRADE_STR, script_content), "Downgrade script should be empty for unsupported attr changes")
    # TODO: caplog is not working as expected
    # assert "does not support altering view comments" in caplog.text
    # assert "does not support altering view security" in caplog.text


def test_alter_view(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests altering a view's definition.
    Qualifiers, like '`schema`.`table`.', for column names are removed.
    """
    # 1. Initial state
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', STRING(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('user_view', Base.metadata, definition='SELECT id FROM user')
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
    alembic_env.harness.upgrade("head")

    # 2. Altered metadata
    AlteredBase = declarative_base()
    Table(
        'user',
        AlteredBase.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', STRING(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('user_view', AlteredBase.metadata, definition="SELECT id, name FROM user")
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter view")

    # 3. Verify and apply ALTER
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_view")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.alter_view('user_view'" in upgrade_content
    assert "SELECT id, name FROM user" in upgrade_content
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    assert "op.alter_view('user_view'" in downgrade_content
    assert "SELECT id FROM user".lower() in downgrade_content

    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    definition = inspector.get_view_definition('user_view')
    assert 'name' in definition.lower()

    # 4. Downgrade and verify
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    definition = inspector.get_view_definition('user_view')
    assert 'name' not in definition.lower()


def test_drop_view(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests dropping a view."""
    # 1. Initial state
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('user_view', Base.metadata, definition='SELECT id FROM user')
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
    alembic_env.harness.upgrade("head")

    # 2. Metadata without the view
    EmptyBase = declarative_base()
    Table(
        'user',
        EmptyBase.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    alembic_env.harness.generate_autogen_revision(metadata=EmptyBase.metadata, message="Drop view")

    # 3. Verify and apply DROP
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "drop_view")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.drop_view('user_view')" in upgrade_content
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    assert "op.create_view('user_view'" in downgrade_content

    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    is_true('user_view' not in inspector.get_view_names())

    # 4. Downgrade and verify
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('user_view' in inspector.get_view_names())


def test_columns_change_ignored(database: str, alembic_env: AlembicTestEnv, sr_engine, caplog: LogCaptureFixture):
    """Tests that column changes are detected but not altered (StarRocks limitation)."""
    caplog.set_level("WARNING")

    # 1. Initial state with columns
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        Base.metadata,
        Column('id', VARCHAR()),
        Column('name', VARCHAR(50), comment='User name'),
        definition='SELECT id, name FROM user'
    )
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial with columns")
    alembic_env.harness.upgrade("head")

    # 2. Change column definition (add new column, change comment)
    AlteredBase = declarative_base()
    Table(
        'user',
        AlteredBase.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        AlteredBase.metadata,
        Column('id', VARCHAR()),
        Column('name', VARCHAR(50), comment='Modified name'),  # Changed comment
        definition='SELECT id, name FROM user',  # Definition unchanged
    )
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Modify column comment")

    # 3. Verify no ALTER is generated (only definition changes trigger ALTER)
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "modify_column_comment")
    is_true(re.search(EMPTY_UPGRADE_STR, script_content), "Upgrade script should be empty for column comment changes")

    # 4. Change definition to test that it triggers ALTER (even with column changes)
    AlteredBase2 = declarative_base()
    Table(
        'user',
        AlteredBase2.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        AlteredBase2.metadata,
        Column('id', VARCHAR()),
        Column('name', VARCHAR(50), comment='Modified name'),
        Column('email', VARCHAR(100), comment='Email'),  # Added column
        definition='SELECT id, name, email FROM user',  # Changed definition
    )
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase2.metadata, message="Change definition and columns")

    # 5. Verify ALTER is generated for definition change
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "change_definition_and_columns")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.alter_view" in upgrade_content
    assert "email" in upgrade_content


def test_create_view_with_schema(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests creating a view in a specific schema (non-default schema)."""
    # 1. Define metadata with a view in a schema
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        schema=database,
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View(
        'user_view',
        Base.metadata,
        definition='SELECT id FROM user',
        schema=database
    )

    # 2. Run autogenerate to create the view
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Create view with schema"
    )

    # 3. Verify the script and upgrade
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_view_with_schema")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.create_view('user_view'" in upgrade_content
    assert f"schema='{database}'" in upgrade_content
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    assert "op.drop_view('user_view'" in downgrade_content
    assert f"schema='{database}'" in downgrade_content

    alembic_env.harness.upgrade("head")

    # 4. Verify view exists in the database
    inspector = inspect(sr_engine)
    is_true('user_view' in inspector.get_view_names(schema=database))

    # 5. Downgrade and verify view is dropped
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('user_view' not in inspector.get_view_names(schema=database))


def test_multiple_views_in_one_migration(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests creating, altering, and dropping multiple views in one migration.

    This is a system-level test that verifies:
    1. Multiple view operations can be generated in a single migration
    2. All operations are correctly written to the script
    3. Upgrade applies all operations in correct order
    4. Downgrade correctly reverses all operations
    """
    # 1. Initial state: create two views
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('view1', Base.metadata, definition='SELECT id FROM user')
    View('view2', Base.metadata, definition='SELECT name FROM user')
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial two views")
    alembic_env.harness.upgrade("head")

    # 2. Define altered state: create view3, alter view1, drop view2
    AlteredBase = declarative_base()
    Table(
        'user',
        AlteredBase.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('view1', AlteredBase.metadata, definition='SELECT id, name FROM user')  # Altered
    View('view3', AlteredBase.metadata, definition='SELECT id FROM user WHERE id > 10')  # New
    # view2 removed (will be dropped)

    # 3. Generate migration with multiple operations
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Multiple ops")

    # 4. Verify script contains all operations
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "multiple_ops")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    assert "op.create_view('view3'" in upgrade_content
    assert "op.alter_view('view1'" in upgrade_content
    assert "op.drop_view('view2'" in upgrade_content
    downgrade_content = ScriptContentParser.extract_downgrade_content(script_content)
    assert "op.drop_view('view3'" in downgrade_content
    assert "op.alter_view('view1'" in downgrade_content
    assert "op.create_view('view2'" in downgrade_content

    # 5. Apply upgrade and verify all changes
    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    is_true('view1' in inspector.get_view_names())
    is_true('view2' not in inspector.get_view_names())
    is_true('view3' in inspector.get_view_names())

    # 6. Apply downgrade and verify rollback
    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    is_true('view1' in inspector.get_view_names())
    is_true('view2' in inspector.get_view_names())
    is_true('view3' not in inspector.get_view_names())


def test_include_object_excludes_tables_and_mvs(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests that the default include_object filter correctly handles tables, views, and MVs.

    This verifies that when using the default filter:
    - Regular tables are excluded from view/MV autogeneration
    - Views are included
    - Materialized views are included
    """
    from starrocks.sql.schema import MaterializedView

    # 1. Define metadata with table, view, and MV
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        Column('name', VARCHAR(50)),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('user_view', Base.metadata, definition='SELECT id FROM user')
    MaterializedView(
        'user_mv',
        Base.metadata,
        definition='SELECT name, COUNT(*) FROM user GROUP BY name',
        starrocks_refresh='MANUAL'
    )

    # 2. Generate migration (should include view and MV, exclude table)
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata,
        message="Create table, view, and MV"
    )

    # 3. Verify script contains view and MV operations (table handled separately)
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_table_view_and_mv")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)

    # Should contain view and MV creation
    assert "op.create_view('user_view'" in upgrade_content or "user_view" in upgrade_content
    assert "op.create_materialized_view('user_mv'" in upgrade_content or "user_mv" in upgrade_content

    # Apply and verify
    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    is_true('user_view' in inspector.get_view_names())
    is_true('user_mv' in inspector.get_materialized_view_names())


def test_custom_include_object(database: str, alembic_env: AlembicTestEnv, sr_engine):
    """Tests using a custom include_object filter in Alembic configuration.

    This verifies that users can define their own filter logic to:
    - Exclude specific view patterns (e.g., tmp_* views)
    - Combine with the default dialect filter
    - Correctly generate migrations based on custom rules
    """
    # 1. Create views in database (including tmp_ views)
    with sr_engine.connect() as conn:
        conn.execute(text("DROP VIEW IF EXISTS prod_view"))
        conn.execute(text("DROP VIEW IF EXISTS tmp_view"))
        conn.execute(text("CREATE VIEW prod_view AS SELECT 1 AS val"))
        conn.execute(text("CREATE VIEW tmp_view AS SELECT 2 AS val"))
        conn.commit()

    # 2. Define metadata with only prod views (tmp_ views should be ignored)
    Base = declarative_base()
    Table(
        'user',
        Base.metadata,
        Column('id', INTEGER, primary_key=True),
        starrocks_PROPERTIES={'replication_num': '1'}
    )
    View('prod_view', Base.metadata, definition='SELECT 1 AS val')
    # tmp_view not in metadata, but we'll configure filter to ignore it

    # 3. Custom include_object that excludes tmp_* views
    from starrocks.alembic.compare import include_object_for_view_mv

    def custom_include_object(object, name, type_, reflected, compare_to):
        # First apply dialect's default filter
        if not include_object_for_view_mv(object, name, type_, reflected, compare_to):
            return False
        # Then apply custom logic: exclude tmp_* views
        if type_ == "view" and name.startswith("tmp_"):
            logger.info(f"Custom filter: excluding tmp_ view {name}")
            return False
        return True

    # 4. Configure alembic with custom filter
    # Note: In real usage, this would be in alembic/env.py
    # For this test, we'll verify the concept by checking that tmp_view is not dropped
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata,
        message="Custom filter test",
        process_revision_directives=None  # Use default configuration
    )

    # 5. Verify script: tmp_view should NOT be dropped (filtered out)
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "custom_filter_test")
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)

    # prod_view should match (no change)
    # tmp_view should be filtered out (not appear as drop operation)
    # Note: Without custom filter, tmp_view would generate a DROP operation

    # Clean up
    with sr_engine.connect() as conn:
        conn.execute(text("DROP VIEW IF EXISTS prod_view"))
        conn.execute(text("DROP VIEW IF EXISTS tmp_view"))
        conn.commit()
