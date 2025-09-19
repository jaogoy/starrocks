import logging
import re

from sqlalchemy import Column, Engine, Integer, inspect
from starrocks.datatype import LARGEINT, VARCHAR, STRING, INTEGER
from sqlalchemy.orm import declarative_base
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.suite import is_true

from starrocks.params import TableInfoKeyWithPrefix
from test.system.conftest import AlembicTestEnv


logger = logging.getLogger(__name__)


def test_create_table_simple(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Tests the autogeneration of a CREATE TABLE script and its application.
    """
    # 1. Define the initial state (a single table)
    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"
        id = Column(INTEGER, primary_key=True)
        name = Column(STRING, nullable=False)
        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num" : "1"},
        }

    # 2. Run autogenerate
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata,
        message="Create user table",
    )

    # # 3. Check the generated script
    versions_dir = alembic_env.root_path / "alembic/versions"
    scripts = list(versions_dir.glob("*create_user_table.py"))
    eq_(len(scripts), 1)
    script_content: str = scripts[0].read_text()
    # logger.debug("script_content: %s", script_content)

    # Normalize script content for robust comparison
    normalized_content = re.sub(r'[ \t]+', ' ', script_content).lower()
    assert "op.create_table('user'" in normalized_content
    assert "sa.Column('id', sr.integer()".lower() in normalized_content
    assert "sa.Column('name', sr.string()".lower() in normalized_content
    assert "sa.PrimaryKeyConstraint('id')".lower() in normalized_content
    assert "starrocks_PRIMARY_KEY='id'".lower() in normalized_content
    assert "starrocks_DISTRIBUTED_BY='HASH(id)'".lower() in normalized_content
    assert "'replication_num': '1'" in normalized_content

    # 4. Run upgrade
    alembic_env.harness.upgrade("head")

    # 5. Verify the table in the database
    # TODO: need to be more exact
    with sr_engine.connect() as conn:
        inspector = inspect(conn)
        is_true(inspector.has_table("user"))
        table_opts = inspector.get_table_options("user")
        logger.info("table_opts: %s", table_opts)
        eq_(table_opts["starrocks_PRIMARY_KEY"], "id")
        eq_(table_opts["starrocks_DISTRIBUTED_BY"], "HASH(`id`)")


EMPTY_UPGRADE_PATTERN = re.compile(r"def upgrade().*:\s*\n\s*#.*\n\s*pass")
EMPTY_DOWNGRADE_PATTERN = re.compile(r"def downgrade().*:\s*\n\s*#.*\n\s*pass")


def test_create_table_idempotency(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Tests that a second autogenerate run produces no new scripts if the
    database is already in sync with the metadata.
    """
    # 1. Define the model and bring the database to the target state
    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"
        id = Column(INTEGER(10), primary_key=True)
        id2 = Column(LARGEINT, primary_key=True)
        name = Column(VARCHAR(50), nullable=False)
        name2 = Column(STRING, nullable=True)
        __table_args__ = {
            "starrocks_primary_key": "`id`, id2",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Create user table"
    )
    alembic_env.harness.upgrade("head")

    versions_dir = alembic_env.root_path / "alembic/versions"
    scripts = list(versions_dir.glob("*create_user_table.py"))
    eq_(len(scripts), 1)
    script_content: str = scripts[0].read_text()
    # logger.debug("script_content: %s", script_content)

    # 2. Run autogenerate again
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Second run"
    )

    # 3. Verify that NO new script was generated
    scripts = list(versions_dir.glob("*second_run.py"))
    eq_(len(scripts), 1, "A second, empty migration script should not be generated.")
    script_content: str = scripts[0].read_text()
    logger.debug("script_content: %s", script_content)
    is_true(re.search(EMPTY_UPGRADE_PATTERN, script_content), "A second, Upgrade script should be empty")
    is_true(re.search(EMPTY_DOWNGRADE_PATTERN, script_content), "A second, Downgrade script should be empty")

