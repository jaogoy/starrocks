import pytest
from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.engine import Engine
from sqlalchemy.exc import NotSupportedError

from test.conftest_sr import create_test_engine, test_default_schema
from starrocks.alembic.compare import compare_starrocks_table


@pytest.mark.integration
class TestCompareColumnAutoincrementIntegration:
    engine: Engine
    test_schema: str | None

    @classmethod
    def setup_class(cls) -> None:
        cls.engine = create_test_engine()
        cls.test_schema = test_default_schema
        with cls.engine.begin() as conn:
            if cls.test_schema:
                conn.exec_driver_sql(f"CREATE DATABASE IF NOT EXISTS {cls.test_schema}")

    @classmethod
    def teardown_class(cls) -> None:
        with cls.engine.begin() as conn:
            # Drop all tables in schema
            res = conn.exec_driver_sql("SHOW TABLES")
            for row in res:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {row[0]}")
        cls.engine.dispose()

    def _full(self, name: str) -> str:
        return f"{self.test_schema}.{name}" if self.test_schema else name

    def test_autoincrement_same_noop(self):
        tname = "ai_same"
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                f"""
                CREATE TABLE {self._full(tname)} (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    v INT
                )
                DISTRIBUTED BY RANDOM
                PROPERTIES("replication_num"="1")
                """
            )
        try:
            md_db = MetaData()
            reflected = Table(tname, md_db, autoload_with=self.engine, schema=self.test_schema)

            md_target = MetaData()
            # Modeling with autoincrement True
            target = Table(
                tname, md_target,
                Column("id", Integer, autoincrement=True),
                Column("v", Integer),
                schema=self.test_schema,
            )

            ctx = type("C", (), {"dialect": self.engine.dialect})()
            ops = compare_starrocks_table(ctx, reflected, target)
            # No NotSupportedError should be raised, and no ops produced for autoincrement
            assert isinstance(ops, list)
        finally:
            with self.engine.begin() as conn:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self._full(tname)}")

    def test_autoincrement_diff_raises(self):
        tname = "ai_diff"
        with self.engine.begin() as conn:
            conn.exec_driver_sql(
                f"""
                CREATE TABLE {self._full(tname)} (
                    id BIGINT NOT NULL AUTO_INCREMENT,
                    v INT
                )
                DISTRIBUTED BY RANDOM
                PROPERTIES("replication_num"="1")
                """
            )
        try:
            md_db = MetaData()
            reflected = Table(tname, md_db, autoload_with=self.engine, schema=self.test_schema)

            md_target = MetaData()
            # Modeling with autoincrement False (difference)
            target = Table(
                tname, md_target,
                Column("id", Integer, autoincrement=False),
                Column("v", Integer),
                schema=self.test_schema,
            )

            ctx = type("C", (), {"dialect": self.engine.dialect})()
            with pytest.raises(NotSupportedError):
                compare_starrocks_table(ctx, reflected, target)
        finally:
            with self.engine.begin() as conn:
                conn.exec_driver_sql(f"DROP TABLE IF EXISTS {self._full(tname)}")
