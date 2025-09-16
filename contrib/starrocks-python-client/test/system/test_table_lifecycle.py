import logging
from sqlalchemy import Column, Engine, Integer, String, inspect
from sqlalchemy.orm import declarative_base
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.suite import is_true

from test.system.conftest import AlembicTestEnv


logger = logging.getLogger(__name__)


def test_create_table(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Tests the autogeneration of a CREATE TABLE script and its application.
    """
    # 1. Define the initial state (a single table)
    Base = declarative_base()

    class User(Base):
        __tablename__ = "user"
        id = Column(Integer, primary_key=True)
        name = Column(String(50), nullable=False)
        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    # 2. Run autogenerate
    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata,
        message="Create user table",
    )

    # 3. Check the generated script
    versions_dir = alembic_env.root_path / "alembic/versions"
    scripts = list(versions_dir.glob("*.py"))
    eq_(len(scripts), 1)
    script_content: str = scripts[0].read_text()
    logger.info("script_content: %s", script_content)

    assert "op.create_table('user'" in script_content
    assert "starrocks_PRIMARY_KEY='id'".lower() in script_content.lower()
    assert "starrocks_DISTRIBUTED_BY='HASH(id)'".lower() in script_content.lower()
    assert "'replication_num': '1'" in script_content.lower()

    # 4. Run upgrade
    alembic_env.harness.upgrade("head")

    # 5. Verify the table in the database
    with sr_engine.connect() as conn:
        inspector = inspect(conn)
        is_true(inspector.has_table("user"))
        table_opts = inspector.get_table_options("user")
        logger.info("table_opts: %s", table_opts)
        eq_(table_opts["starrocks_PRIMARY_KEY"], "id")
        eq_(table_opts["starrocks_DISTRIBUTED_BY"], "HASH(`id`)")
