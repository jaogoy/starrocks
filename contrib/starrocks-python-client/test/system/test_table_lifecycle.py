import logging
import re
import time

from sqlalchemy import Column, Engine, inspect
from starrocks.datatype import LARGEINT, VARCHAR, STRING, INTEGER
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.suite import is_true

from starrocks.params import TableInfoKeyWithPrefix
from test.system.conftest import AlembicTestEnv


logger = logging.getLogger(__name__)

# the leading lines of upgrade and downgrade python script
UPGRADE_STR = r"def upgrade\(\).*:\s*\n\s*#.*\n\s*"
DOWNGRADE_STR = r"def downgrade\(\).*:\s*\n\s*#.*\n\s*"


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
    EMPTY_UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"pass")
    EMPTY_DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"pass")
    is_true(re.search(EMPTY_UPGRADE_PATTERN, script_content), "A second, Upgrade script should be empty")
    is_true(re.search(EMPTY_DOWNGRADE_PATTERN, script_content), "A second, Downgrade script should be empty")


def test_alter_table_distribution(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """Tests altering table distribution."""
    # 1. Initial state
    Base = declarative_base()
    class User(Base):
        __tablename__ = "user"
        id = Column(INTEGER, primary_key=True)
        id2 = Column(INTEGER, primary_key=True)
        name = Column(VARCHAR(50))
        __table_args__ = {
            "starrocks_primary_key": "id, id2",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
    alembic_env.harness.upgrade("head")

    # 2. Alter distribution in metadata
    AlteredBase = declarative_base()
    class AlteredUser(AlteredBase):
        __tablename__ = "user"
        id = Column(INTEGER, primary_key=True)
        id2 = Column(INTEGER, primary_key=True)
        name = Column(VARCHAR(50))
        __table_args__ = {
            "starrocks_primary_key": "id, id2",
            "starrocks_distributed_by": "HASH(id2) BUCKETS 3",
            "starrocks_properties": {"replication_num": "1"},
        }
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter dist")

    # 3. Verify and apply the ALTER script
    versions_dir = alembic_env.root_path / "alembic/versions"
    scripts = list(versions_dir.glob("*alter_dist.py"))
    eq_(len(scripts), 1)
    script_content: str = scripts[0].read_text()
    logger.debug("script_content: %s", script_content)
    UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"op.alter_table_distribution\('user', 'HASH\(id2\)', buckets=3\)\s*\n\s*#")
    # NOTE: there is backtick in the distributed by string for downgrade script
    DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"op.alter_table_distribution\('user', 'HASH\(`id`\)'\)\s*\n\s*#")
    is_true(UPGRADE_PATTERN.search(script_content), "Upgrade script should contain ALTER TABLE DISTRIBUTED BY operation")
    is_true(DOWNGRADE_PATTERN.search(script_content), "Downgrade script should contain ALTER TABLE DISTRIBUTED BY operation")

    alembic_env.harness.upgrade("head")

    # 4. Verify in DB and then downgrade
    inspector = inspect(sr_engine)
    # we need to wait the ALTER TABLE take effect
    for i in range(20):
        inspector.clear_cache()
        options = inspector.get_table_options("user")
        logger.debug(f"get table options (round={i+1}): %s", options)
        dist = options['starrocks_DISTRIBUTED_BY'].strip()
        if 'HASH(`id2`) BUCKETS 3' != dist:
            time.sleep(3)
            continue
    assert 'HASH(`id2`) BUCKETS 3' == dist 

    alembic_env.harness.downgrade("-1")
    # we need to wait the ALTER TABLE take effect
    for i in range(20):
        inspector.clear_cache()
        options = inspector.get_table_options("user")
        logger.debug(f"get table options (round={i+1}): %s", options)
        dist = options['starrocks_DISTRIBUTED_BY'].strip()
        if 'HASH(`id`)' != dist:
            time.sleep(3)
            continue
    assert 'HASH(`id`)' == dist 


def test_alter_table_properties(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Tests altering table properties.

    - Adds a new property 'replicated_storage'.
    - Changes an existing property 'storage_medium'.
    """
    # 1. Initial state
    Base = declarative_base()
    class User(Base):
        __tablename__ = "user"
        id = Column(INTEGER, primary_key=True)
        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {
                "replication_num": "1",
                "storage_medium": "HDD",
            },
        }
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
    logger.debug("Init state.")
    alembic_env.harness.upgrade("head")
    logger.debug("Upgrade to head.")

    # 2. Alter properties in metadata
    AlteredBase = declarative_base()
    class AlteredUser(AlteredBase):
        __tablename__ = "user"
        id = Column(INTEGER, primary_key=True)
        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1",
                "replicated_storage": "false",
                "storage_medium": "SSD",
            },
        }
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter props")
    
    versions_dir = alembic_env.root_path / "alembic/versions"
    # 3. Verify and apply the ALTER script
    scripts = list(versions_dir.glob("*alter_props.py"))
    eq_(len(scripts), 1)
    script_content: str = scripts[0].read_text()
    logger.debug("script_content: %s", script_content)
    assert "op.alter_table" in script_content
    assert "'replicated_storage': 'false'" in script_content
    assert "'default.storage_medium': 'SSD'" in script_content
    
    logger.debug("Start to do upgrade for schema diff.")
    alembic_env.harness.upgrade("head")
    logger.debug("Upgrade to head.")
    
    # 4. Verify in DB
    inspector = inspect(sr_engine)
    logger.debug("inspector.get_table_options('user'): %s", inspector.get_table_options("user"))
    props = inspector.get_table_options("user")[TableInfoKeyWithPrefix.PROPERTIES]
    assert props['replicated_storage'] == 'false'
    assert props['storage_medium'] == 'SSD'
    
    # 5. Downgrade one revision, not to the base
    logger.debug("Start to do downgrade.")
    alembic_env.harness.downgrade("-1")
    logger.debug("Downgraded one revision.")

    inspector.clear_cache()
    props = inspector.get_table_options("user")[TableInfoKeyWithPrefix.PROPERTIES]
    assert props['replicated_storage'] == 'true'
    assert props['storage_medium'] == 'HDD'

    # 6. Downgrade to base
    alembic_env.harness.downgrade("base")
    logger.debug("Downgraded to base.")
    inspector.clear_cache()
    is_true(not inspector.has_table("user"))


def test_drop_table(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """Tests dropping a table."""
    # 1. Initial state: (create a table)
    Base = declarative_base()
    class User(Base):
        __tablename__ = "user"
        id = Column(INTEGER, primary_key=True)
        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_properties": {"replication_num": "1"}
        }

    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial")
    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    is_true(inspector.has_table("user"))

    # 2. Drop the table from metadata
    logger.debug("Start to compare the table 'user' with the empty metadata.")
    EmptyBase = declarative_base()
    alembic_env.harness.generate_autogen_revision(metadata=EmptyBase.metadata, message="Drop user")
    
    # 3. Verify and apply the DROP script
    versions_dir = alembic_env.root_path / "alembic/versions"
    scripts = list(versions_dir.glob("*drop_user.py"))
    eq_(len(scripts), 1)
    script_content: str = scripts[0].read_text()
    logger.debug("script_content: %s", script_content)
    UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"op.drop_table\('user'\)\s*\n\s*#")
    DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"op.create_table\('user'")
    is_true(UPGRADE_PATTERN.search(script_content), "Upgrade script should contain DROP TABLE operation")
    is_true(DOWNGRADE_PATTERN.search(script_content), "Downgrade script should contain CREATE TABLE operation")
    
    logger.debug("Start to do upgrade: drop the table 'user'.")
    alembic_env.harness.upgrade("head")
    logger.debug("Upgraded to head: drop the table 'user'.")
    inspector.clear_cache()
    is_true(not inspector.has_table("user"))
    
    # 4. Downgrade and verify table is restored
    logger.debug("Start to do downgrade: restore the table 'user'.")
    alembic_env.harness.downgrade("-1")
    logger.debug("Downgraded to base: restore the table 'user'.")
    inspector.clear_cache()
    is_true(inspector.has_table("user"))


def test_drop_duplicate_key_table(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """Tests dropping a duplicate key table.
    Alembic should not generate useless 'index' for upgrade script.
    """
    # 1. Initial state: (create a table)
    Base = declarative_base()

    class UserDuplicate(Base):
        __tablename__ = "user_duplicate"
        id = Column(INTEGER, primary_key=True)
        name = Column(STRING, nullable=False)
        __table_args__ = {
            "starrocks_duplicate_key": "id",
            "starrocks_properties": {"replication_num": "1"},
        }

    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Initial duplicate"
    )
    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    is_true(inspector.has_table("user_duplicate"))

    # 2. Drop the table from metadata
    logger.debug("Start to compare the table 'user_duplicate' with the empty metadata.")
    EmptyBase = declarative_base()
    alembic_env.harness.generate_autogen_revision(
        metadata=EmptyBase.metadata, message="Drop user_duplicate"
    )

    # 3. Verify and apply the DROP script
    versions_dir = alembic_env.root_path / "alembic/versions"
    scripts = list(versions_dir.glob("*drop_user_duplicate.py"))
    eq_(len(scripts), 1)
    script_content: str = scripts[0].read_text()
    logger.debug("script_content: %s", script_content)
    UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"op.drop_table\('user_duplicate'\)\s*\n\s*#")
    DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"op.create_table\('user_duplicate'")
    is_true(
        UPGRADE_PATTERN.search(script_content),
        "Upgrade script should contain DROP TABLE operation",
    )
    is_true(
        DOWNGRADE_PATTERN.search(script_content),
        "Downgrade script should contain CREATE TABLE operation",
    )


def test_drop_aggregate_key_table(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """Tests dropping an aggregate key table.
    Alembic should not generate useless 'index' for upgrade script.
    """
    # 1. Initial state: (create a table)
    Base = declarative_base()

    class UserAggregate(Base):
        __tablename__ = "user_aggregate"
        id = Column(INTEGER, primary_key=True)
        # name = Column(STRING, starrocks_agg_type="SUM")
        name: Mapped[str] = mapped_column(STRING, starrocks_agg_type="REPLACE")
        __table_args__ = {
            "starrocks_aggregate_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Initial aggregate"
    )
    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    is_true(inspector.has_table("user_aggregate"))

    # 2. Drop the table from metadata
    logger.debug("Start to compare the table 'user_aggregate' with the empty metadata.")
    EmptyBase = declarative_base()
    alembic_env.harness.generate_autogen_revision(
        metadata=EmptyBase.metadata, message="Drop user_aggregate"
    )

    # 3. Verify and apply the DROP script
    versions_dir = alembic_env.root_path / "alembic/versions"
    scripts = list(versions_dir.glob("*drop_user_aggregate.py"))
    eq_(len(scripts), 1)
    script_content: str = scripts[0].read_text()
    logger.debug("script_content: %s", script_content)
    UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"op.drop_table\('user_aggregate'\)\s*\n\s*#")
    DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"op.create_table\('user_aggregate'")
    is_true(
        UPGRADE_PATTERN.search(script_content),
        "Upgrade script should contain DROP TABLE operation",
    )
    is_true(
        DOWNGRADE_PATTERN.search(script_content),
        "Downgrade script should contain CREATE TABLE operation",
    )

    logger.debug("Start to do upgrade: drop the table 'user_aggregate'.")
    alembic_env.harness.upgrade("head")
    logger.debug("Upgraded to head: drop the table 'user_aggregate'.")
    inspector.clear_cache()
    is_true(not inspector.has_table("user_aggregate"))

    # 4. Downgrade and verify table is restored
    logger.debug("Start to do downgrade: restore the table 'user_aggregate'.")
    alembic_env.harness.downgrade("-1")
    logger.debug("Downgraded to base: restore the table 'user_aggregate'.")
    inspector.clear_cache()
    is_true(inspector.has_table("user_aggregate"))


def test_drop_unique_key_table(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """Tests dropping a unique key table.
    Alembic should not generate useless 'index' for upgrade script.
    """
    # 1. Initial state: (create a table)
    Base = declarative_base()

    class UserUnique(Base):
        __tablename__ = "user_unique"
        id = Column(INTEGER, primary_key=True)
        name = Column(STRING, nullable=False)
        __table_args__ = {
            "starrocks_unique_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }

    alembic_env.harness.generate_autogen_revision(
        metadata=Base.metadata, message="Initial unique"
    )
    alembic_env.harness.upgrade("head")
    inspector = inspect(sr_engine)
    is_true(inspector.has_table("user_unique"))

    # 2. Drop the table from metadata
    logger.debug("Start to compare the table 'user_unique' with the empty metadata.")
    EmptyBase = declarative_base()
    alembic_env.harness.generate_autogen_revision(
        metadata=EmptyBase.metadata, message="Drop user_unique"
    )

    # 3. Verify and apply the DROP script
    versions_dir = alembic_env.root_path / "alembic/versions"
    scripts = list(versions_dir.glob("*drop_user_unique.py"))
    eq_(len(scripts), 1)
    script_content: str = scripts[0].read_text()
    logger.debug("script_content: %s", script_content)
    UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"op.drop_table\('user_unique'\)\s*\n\s*#")
    DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"op.create_table\('user_unique'")
    is_true(
        UPGRADE_PATTERN.search(script_content),
        "Upgrade script should contain DROP TABLE operation",
    )
    is_true(
        DOWNGRADE_PATTERN.search(script_content),
        "Downgrade script should contain CREATE TABLE operation",
    )
