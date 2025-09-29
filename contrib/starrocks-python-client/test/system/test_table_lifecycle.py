import logging
import re
import time
from typing import List, Optional

from sqlalchemy import Column, Engine, Inspector, inspect
from starrocks.datatype import (
    LARGEINT, VARCHAR, STRING, INTEGER, BIGINT, BOOLEAN, TINYINT, SMALLINT,
    DECIMAL, DOUBLE, FLOAT, DATE, DATETIME, CHAR, ARRAY, MAP, STRUCT, JSON,
    HLL, BITMAP
)
from sqlalchemy.orm import Mapped, declarative_base, mapped_column
from sqlalchemy.testing.assertions import eq_
from sqlalchemy.testing.suite import is_true

from starrocks.dialect import StarRocksDialect
from starrocks.params import TableInfoKeyWithPrefix
from test.system.conftest import AlembicTestEnv


logger = logging.getLogger(__name__)

# the leading lines of upgrade and downgrade python script
UPGRADE_STR = r"def upgrade\(\).*?:\s*\n\s*#.*?\n\s*"
DOWNGRADE_STR = r"def downgrade\(\).*?:\s*\n\s*#.*?\n\s*"


def print_sql_before_execute(conn, cursor, statement, parameters, context, executemany):
    logger.debug("SQL ready to be executed: %s", statement)


def bind_print_sql_before_execute(engine: Engine):
    """Bind the print_sql_before_execute event to the connection."""
    from sqlalchemy import event
    event.listen(engine, "before_cursor_execute", print_sql_before_execute)


class ScriptContentParser():
    UPGRADE_EXTRACTION_REGEX = re.compile(UPGRADE_STR + r"(.*?)(?=" + DOWNGRADE_STR + r"|\Z)", re.DOTALL)
    DOWNGRADE_EXTRACTION_REGEX = re.compile(DOWNGRADE_STR + r"(.*?)(?=" + UPGRADE_STR + r"|\Z)", re.DOTALL)

    @classmethod
    def check_script_content(cls, alembic_env: AlembicTestEnv, script_num: int, script_name: str) -> str:
        """Check the content of the script.
        """
        versions_dir = alembic_env.root_path / "alembic/versions"
        scripts = list(versions_dir.glob(f"*{script_name}.py"))
        assert len(scripts) == script_num
        script_content = scripts[0].read_text()
        logger.debug(f"script_content:\n>>>>\n{script_content}\n<<<<")
        return script_content

    @classmethod
    def _extract_upgrade_or_downgrade_content(cls, header: str, script: str) -> Optional[str]:
        """Extract the body of the upgrade() function from an Alembic migration script."""
        match = cls.UPGRADE_EXTRACTION_REGEX.search(script)
        if not match:
            return None
        content = match.group(1)
        logger.debug(f"upgrade/downgrade content:\n>>>>\n{content}\n<<<<")
        return content

    @classmethod
    def extract_upgrade_content(cls, script: str) -> Optional[str]:
        """Extract the body of the upgrade() function from an Alembic migration script."""
        return cls._extract_upgrade_or_downgrade_content(cls.UPGRADE_EXTRACTION_REGEX, script)

    @classmethod
    def extract_downgrade_content(cls, script: str) -> Optional[str]:
        return cls._extract_upgrade_or_downgrade_content(cls.DOWNGRADE_EXTRACTION_REGEX, script)
    
    @classmethod
    def extract_non_comment_lines(cls, content: str) -> List[str]:
        """Extract the non-comment lines from an Alembic migration script."""
        non_comment_lines = [line for line in content.split('\n') 
                            if line.strip() and not line.strip().startswith('#')]
        non_comment_lines_str = '\n'.join(non_comment_lines)
        logger.debug(f"non comment lines:\n>>>>\n{non_comment_lines_str}\n<<<<")

        return non_comment_lines


def wait_for_alter_table_attributes(inspector: Inspector, table_name: str,
                                    attribute_name: str, expected_value: str,
                                    max_round: int = 20, sleep_time: int = 3):
    """Wait for the ALTER TABLE to finish."""
    for i in range(max_round):
        inspector.clear_cache()
        options = inspector.get_table_options(table_name)
        logger.debug(f"get table options (round={i+1}) for table: {table_name}, attribute: {attribute_name}. %s", options)
        value = options[attribute_name].strip()
        if expected_value == value:
            break
        else:
            time.sleep(sleep_time)
    assert expected_value == value
    return options


def check_for_alter_table_optimization(engine: Engine, table_name: str, 
        schema: Optional[str] = None, max_round: int = 20, sleep_time: int = 3):
    """Wait for the ALTER TABLE to finish.
    Because the state may not change after the ALTER TABLE command is executed successfully.
    """
    with engine.connect() as conn:
        for i in range(max_round):
            show_alter_table_optimization = StarRocksDialect.get_show_alter_table_optimization(conn, table_name, schema)
            logger.debug(f"get show alter table optimization (round={i+1}) for table: {table_name}, schema: {schema}. %s", show_alter_table_optimization)
            if not show_alter_table_optimization:  # no running alter table
                break
            time.sleep(sleep_time)
        if show_alter_table_optimization:
            logger.warning("ALTER TABLE is still running for table: %s", table_name)
    return not show_alter_table_optimization


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
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "create_user_table")
    
    # Normalize script content for robust comparison
    upgrade_content = ScriptContentParser.extract_upgrade_content(script_content)
    normalized_content = re.sub(r'[ \t]+', ' ', upgrade_content).lower()
    logger.debug(f"normalized_content: {normalized_content}")
    assert "op.create_table('user'" in normalized_content
    assert "sa.Column('id', integer()".lower() in normalized_content
    assert "sa.Column('name', string()".lower() in normalized_content
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


def test_idempotency_comprehensive(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Tests that a second autogenerate run produces no new scripts for a complex table.
    """
    # 1. Define the comprehensive model
    Base = declarative_base()

    class KitchenSink(Base):
        __tablename__ = "t_kitchen_sink"
        # Column Types
        col_pk = Column(INTEGER(8), primary_key=True)
        col_bool = Column(BOOLEAN, primary_key=True)
        col_tinyint = Column(TINYINT(2), comment="a tiny int")
        col_smallint = Column(SMALLINT)
        col_bigint = Column(BIGINT, nullable=False)
        col_largeint = Column(LARGEINT, default=0)
        col_decimal = Column(DECIMAL(10, 2))
        col_double = Column(DOUBLE)
        col_float = Column(FLOAT)
        col_char = Column(CHAR(10))
        col_varchar = Column(VARCHAR(100))
        col_string = Column(STRING, comment="a string")
        col_date = Column(DATE)
        col_datetime = Column(DATETIME)
        col_array = Column(ARRAY(VARCHAR(20)))
        col_map = Column(MAP(STRING, DECIMAL(5, 2)))
        col_struct = Column(STRUCT(name=VARCHAR(50), age=INTEGER(10)))
        col_nested = Column(STRUCT(
            name=VARCHAR(100),
            details=MAP(
                STRING,
                ARRAY(STRUCT(item_id=INTEGER, price=DECIMAL(10, 2)))
            )
        ))
        col_json = Column(JSON)
        col_hll = Column(HLL)
        col_bitmap = Column(BITMAP)

        __table_args__ = {
            "comment": "Comprehensive table for idempotency test",
            "starrocks_primary_key": "col_pk, col_bool",
            "starrocks_distributed_by": "HASH(col_pk)",
            "starrocks_partition_by": "RANGE(col_pk) (PARTITION p1 VALUES LESS THAN ('100'))",
            "starrocks_order_by": "col_string",
            "starrocks_properties": {"replication_num": "1", "storage_medium": "HDD"},
        }

    # 2. Generate initial revision and upgrade
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Create kitchen sink")
    ScriptContentParser.check_script_content(alembic_env, 1, "create_kitchen_sink")
    logger.debug("Upgrade to head.")
    alembic_env.harness.upgrade("head")

    # 3. Run autogenerate again
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Second run kitchen sink")

    # 4. Verify that NO new script was generated
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "second_run_kitchen_sink")
    EMPTY_UPGRADE_PATTERN = re.compile(UPGRADE_STR + r"pass")
    EMPTY_DOWNGRADE_PATTERN = re.compile(DOWNGRADE_STR + r"pass")
    is_true(re.search(EMPTY_UPGRADE_PATTERN, script_content), "Upgrade script should be empty")
    is_true(re.search(EMPTY_DOWNGRADE_PATTERN, script_content), "Downgrade script should be empty")


def test_alter_table_columns_comprehensive(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """Tests comprehensive column alterations: ADD, DROP, ALTER."""
    # 1. Initial state
    Base = declarative_base()
    class User(Base):
        __tablename__ = "t_alter_columns"
        id = Column(INTEGER, primary_key=True)
        col_to_modify = Column(INTEGER, nullable=False, comment="Original comment")
        col_to_drop = Column(STRING)
        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_properties": {"replication_num": "1"},
        }
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial columns")
    alembic_env.harness.upgrade("head")

    # 2. Alter columns in metadata
    AlteredBase = declarative_base()
    class AlteredUser(AlteredBase):
        __tablename__ = "t_alter_columns"
        id = Column(INTEGER, primary_key=True)
        col_to_modify = Column(BIGINT, nullable=True, comment="Modified comment")
        # col_to_drop is removed
        col_added = Column(VARCHAR(100))
        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",  # auto added by SR
            "starrocks_properties": {"replication_num": "1"},
        }
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter columns")

    # 3. Verify and apply the ALTER script
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_columns")
    # upgrade_contnt = ScriptContentParser.extract_upgrade_content(script_content).lower()
    
    assert "op.add_column('t_alter_columns', sa.Column('col_added', VARCHAR(length=100), nullable=True))" in script_content
    assert "op.drop_column('t_alter_columns', 'col_to_drop')" in script_content
    assert "op.alter_column('t_alter_columns', 'col_to_modify'," in script_content
    assert "type_=BIGINT()" in script_content
    assert "nullable=True" in script_content
    assert "comment='Modified comment'" in script_content
    
    alembic_env.harness.upgrade("head")

    # 4. Verify in DB and then downgrade
    inspector = inspect(sr_engine)
    columns = inspector.get_columns("t_alter_columns")
    col_names = [c['name'] for c in columns]
    assert 'col_to_drop' not in col_names
    assert 'col_added' in col_names
    for col in columns:
        if col['name'] == 'col_to_modify':
            assert isinstance(col['type'], BIGINT)
            assert col['nullable'] is True
            assert col['comment'] == 'Modified comment'

    alembic_env.harness.downgrade("-1")
    inspector.clear_cache()
    columns = inspector.get_columns("t_alter_columns")
    col_names = [c['name'] for c in columns]
    assert 'col_to_drop' in col_names
    assert 'col_added' not in col_names
    for col in columns:
        if col['name'] == 'col_to_modify':
            assert isinstance(col['type'], INTEGER)
            assert col['nullable'] is False
            assert col['comment'] == 'Original comment'


def test_alter_table_attributes_distribution(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """Tests altering table attributes. Includes:
        distribution, comment, and order by. But:
        1. When there is a distribution chagne (time consuming) running, all other ALTER TABLE can't be submitted. So:
            COMMENT, ORDER BY, need to be extracted into another test cases.
        2. engine, key, partition are not tested here, because they are not supported in StarRocks.
        3. And, all operations are tested in test_alter_table_operations.
    """
    # 1. Initial state
    Base = declarative_base()
    class OriginalTableAttr(Base):
        __tablename__ = "t_alter_attr"
        id = Column(INTEGER, primary_key=True)
        id2 = Column(INTEGER, primary_key=True)
        name = Column(STRING)
        __table_args__ = {
            "starrocks_primary_key": "id, id2",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial attr")
    alembic_env.harness.upgrade("head")

    # 2. Alter attributes in metadata
    AlteredBase = declarative_base()
    class AlteredUser(AlteredBase):
        __tablename__ = "t_alter_attr"
        id = Column(INTEGER, primary_key=True)
        id2 = Column(INTEGER, primary_key=True)
        name = Column(STRING)
        __table_args__ = {
            "starrocks_primary_key": "id, id2",
            # "comment": "A new table comment",
            "starrocks_distributed_by": "HASH(id2) BUCKETS 3",
            # "starrocks_order_by": "name",
            "starrocks_properties": {"replication_num": "1"},
        }
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter attr")

    # 3. Verify and apply the ALTER script
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_attr")
    # is_true(re.search(r"op\.create_table_comment\(\s*'t_alter_attr',\s*(:comment=)?\s*'A new table comment'", script_content))
    is_true(re.search(r"op\.alter_table_distribution\(\s*'t_alter_attr',\s*'HASH\(id2\)',\s*buckets=3", script_content))
    # is_true(re.search(r"op\.alter_table_order\(\s*'t_alter_attr',\s*'name'", script_content))

    # bind_print_sql_before_execute(sr_engine)
    alembic_env.harness.upgrade("head")

    # 4. Verify in DB and then downgrade
    logger.debug("Start to verify in DB and then downgrade.")
    inspector = inspect(sr_engine)
    # we need to wait the ALTER TABLE take effect
    options = wait_for_alter_table_attributes(inspector, "t_alter_attr",
        TableInfoKeyWithPrefix.DISTRIBUTED_BY, "HASH(`id2`) BUCKETS 3")
    # assert options.get("starrocks_ORDER_BY") == "name"
    # assert options.get(TableInfoKeyWithPrefix.COMMENT) == "A new table comment"
    
    # 5. Downgrade
    alembic_env.harness.downgrade("-1")
    # we need to wait the ALTER TABLE take effect
    options = wait_for_alter_table_attributes(inspector, "t_alter_attr",
        TableInfoKeyWithPrefix.DISTRIBUTED_BY, "HASH(`id`)")
    assert options.get(TableInfoKeyWithPrefix.COMMENT) is None
    # The default order by key is the primary key
    assert options.get(TableInfoKeyWithPrefix.ORDER_BY) == "`id`, `id2`"


def test_alter_table_order_by(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """Tests altering table order by separately."""
    # 1. Initial state
    Base = declarative_base()
    class OriginalTable(Base):
        __tablename__ = "t_alter_order"
        id = Column(INTEGER, primary_key=True)
        name = Column(STRING)
        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_properties": {"replication_num": "1"},
        }
    alembic_env.harness.generate_autogen_revision(metadata=Base.metadata, message="Initial order")
    alembic_env.harness.upgrade("head")

    # 2. Alter order by in metadata
    AlteredBase = declarative_base()
    class AlteredTable(AlteredBase):
        __tablename__ = "t_alter_order"
        id = Column(INTEGER, primary_key=True)
        name = Column(STRING)
        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "starrocks_order_by": "name",
            "starrocks_properties": {"replication_num": "1"},
        }
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter order")

    # 3. Verify and apply the ALTER script
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_order")
    is_true(re.search(r"op\.alter_table_order\(\s*'t_alter_order',\s*'name'", script_content))

    alembic_env.harness.upgrade("head")

    # 4. Verify in DB and then downgrade
    inspector = inspect(sr_engine)
    options = wait_for_alter_table_attributes(inspector, "t_alter_order", TableInfoKeyWithPrefix.ORDER_BY, "`name`")
    assert options.get(TableInfoKeyWithPrefix.ORDER_BY) == "`name`"

    # 5. Downgrade
    alembic_env.harness.downgrade("-1") 
    options = wait_for_alter_table_attributes(inspector, "t_alter_order", TableInfoKeyWithPrefix.ORDER_BY, "`id`")
    assert options.get(TableInfoKeyWithPrefix.ORDER_BY) == "`id`"


def test_alter_table_properties_and_comment(database: str, alembic_env: AlembicTestEnv, sr_engine: Engine):
    """
    Tests altering table properties and comment.
    All the properties and comment won't cost a lot of time. (replication_num?)

    - Adds a new table comment.
    
    - Adds a new property 'replicated_storage'.
    - Changes an existing property 'storage_medium'.
    """
    # 1. Initial state
    Base = declarative_base()
    class User(Base):
        __tablename__ = "t_alter_props"
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
        __tablename__ = "t_alter_props"
        id = Column(INTEGER, primary_key=True)
        __table_args__ = {
            "starrocks_primary_key": "id",
            "starrocks_distributed_by": "HASH(id)",
            "comment": "A new table comment",
            "starrocks_properties": {"replication_num": "1",
                "replicated_storage": "false",
                "storage_medium": "SSD",
            },
        }
    alembic_env.harness.generate_autogen_revision(metadata=AlteredBase.metadata, message="Alter props")
    
    # 3. Verify and apply the ALTER script
    script_content = ScriptContentParser.check_script_content(alembic_env, 1, "alter_props")
    assert "op.alter_table" in script_content
    assert "'replicated_storage': 'false'" in script_content
    assert "'default.storage_medium': 'SSD'" in script_content
    assert "comment='A new table comment'" in script_content
    
    logger.debug("Start to do upgrade for schema diff.")
    alembic_env.harness.upgrade("head")
    logger.debug("Upgrade to head.")
    
    # 4. Verify in DB
    inspector = inspect(sr_engine)
    options = inspector.get_table_options("t_alter_props")
    props = options[TableInfoKeyWithPrefix.PROPERTIES]
    assert props['replicated_storage'] == 'false'
    assert props['storage_medium'] == 'SSD'
    assert options.get(TableInfoKeyWithPrefix.COMMENT) == "A new table comment"
    
    # 5. Downgrade one revision, not to the base
    logger.debug("Start to do downgrade.")
    alembic_env.harness.downgrade("-1")
    logger.debug("Downgraded one revision.")

    inspector.clear_cache()
    options = inspector.get_table_options("t_alter_props")
    props = options[TableInfoKeyWithPrefix.PROPERTIES]
    assert props['replicated_storage'] == 'true'
    assert props['storage_medium'] == 'HDD'
    assert options.get(TableInfoKeyWithPrefix.COMMENT) is None

    # 6. Downgrade to base
    alembic_env.harness.downgrade("base")
    logger.debug("Downgraded to base.")
    inspector.clear_cache()
    is_true(not inspector.has_table("t_alter_props"))


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
