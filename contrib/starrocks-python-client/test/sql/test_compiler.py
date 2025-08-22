import pytest
from sqlalchemy.dialects import registry
import re
import logging

from starrocks.sql.ddl import CreateView, DropView, CreateMaterializedView, DropMaterializedView
from starrocks.sql.schema import View, MaterializedView


logger = logging.getLogger(__name__)

def _normalize_sql(sql: str) -> str:
    """Replaces newlines with single spaces, removes tabs, and collapses multiple spaces into one."""
    sql = re.sub(r'--.*\n', '', sql)  # Remove comments first
    sql = sql.replace('\n', ' ')  # Replace newlines with spaces
    sql = sql.replace('\t', '')  # Remove tabs
    sql = re.sub(r'\s*\(\s*', '(', sql)  # Remove spaces around opening parenthesis
    sql = re.sub(r'\s*\)\s*', ')', sql)  # Remove spaces around closing parenthesis
    sql = re.sub(r',\s*', ',', sql) # Remove spaces after commas
    sql = re.sub(r' +', ' ', sql).strip()  # Collapse multiple spaces and strip leading/trailing spaces
    return sql

class TestCompiler:
    @classmethod
    def setup_class(cls):
        registry.register("starrocks", "starrocks.dialect", "StarRocksDialect")
        cls.logger = logging.getLogger(__name__)
        cls.dialect = registry.load("starrocks")()

    def test_create_view(self):
        # Basic CREATE VIEW
        self.logger.info("Testing basic CREATE VIEW")
        view = View("my_view", "SELECT * FROM my_table")
        stmt = CreateView(view)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE VIEW my_view AS SELECT * FROM my_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

    def test_create_view_variations(self):
        """Our compiler should correctly compile various view definitions."""
        logger.info("\n--- Testing: CREATE VIEW ---")
        view = View("simple_view", "SELECT c1, c2, c3 FROM test_table")
        stmt = CreateView(view)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE VIEW simple_view AS SELECT c1, c2, c3 FROM test_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

        logger.info("\n--- Testing: CREATE OR REPLACE VIEW ---")
        view = View("simple_view", "SELECT c1, c2, c3 FROM test_table")
        stmt = CreateView(view, or_replace=True)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE OR REPLACE VIEW simple_view AS SELECT c1, c2, c3 FROM test_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

        logger.info("\n--- Testing: CREATE VIEW IF NOT EXISTS ---")
        view = View("simple_view", "SELECT c1 FROM test_table")
        stmt = CreateView(view, if_not_exists=True)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE VIEW IF NOT EXISTS simple_view AS SELECT c1 FROM test_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

        logger.info("\n--- Testing: CREATE VIEW with schema ---")
        view = View("simple_view", "SELECT c1 FROM test_table", schema="test_db")
        stmt = CreateView(view)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE VIEW test_db.simple_view AS SELECT c1 FROM test_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

        logger.info("\n--- Testing: CREATE VIEW with comment ---")
        view = View("commented_view", "SELECT c1, c2 FROM test_table", comment="This is a view with a comment")
        stmt = CreateView(view)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE VIEW commented_view COMMENT 'This is a view with a comment' AS SELECT c1, c2 FROM test_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

        logger.info("\n--- Testing: CREATE VIEW with explicit columns ---")
        view = View("view_with_columns", "SELECT c1, c2 FROM test_table", columns=["col_a", "col_b"])
        stmt = CreateView(view)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE VIEW view_with_columns(col_a, col_b) AS SELECT c1, c2 FROM test_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

        # With column comments
        self.logger.info("Testing CREATE VIEW with column comments")
        view = View(
            "view_with_column_comments",
            "SELECT c1, c2 FROM test_table",
            columns=[
                {'name': 'col_a', 'comment': 'This is the first column'},
                {'name': 'col_b', 'comment': 'This is the second column'}
            ]
        )
        stmt = CreateView(view)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE VIEW view_with_column_comments(col_a COMMENT 'This is the first column', col_b COMMENT 'This is the second column') AS SELECT c1, c2 FROM test_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

        # Comprehensive example with complex query
        self.logger.info("Testing a comprehensive CREATE VIEW statement")
        complex_query = (
            "SELECT u.id, u.name, p.product_name, SUM(o.amount) AS total_amount "
            "FROM users u JOIN orders o ON u.id = o.user_id "
            "JOIN products p ON o.product_id = p.id "
            "WHERE u.registration_date > '2023-01-01' "
            "GROUP BY u.id, u.name, p.product_name "
            "HAVING SUM(o.amount) > 1000"
        )
        view = View(
            "complex_view",
            complex_query,
            schema="test_db",
            comment="A complex view for testing",
            columns=[
                {'name': 'user_id', 'comment': 'User ID'},
                'user_name',
                'product_name',
                {'name': 'total_amount', 'comment': 'Total order amount'}
            ]
        )
        stmt = CreateView(view, or_replace=True)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = (
            "CREATE OR REPLACE VIEW test_db.complex_view("
            "user_id COMMENT 'User ID', "
            "user_name, "
            "product_name, "
            "total_amount COMMENT 'Total order amount'"
            ") "
            "COMMENT 'A complex view for testing' "
            "AS "
            "SELECT u.id, u.name, p.product_name, SUM(o.amount) AS total_amount "
            "FROM users u JOIN orders o ON u.id = o.user_id "
            "JOIN products p ON o.product_id = p.id "
            "WHERE u.registration_date > '2023-01-01' "
            "GROUP BY u.id, u.name, p.product_name "
            "HAVING SUM(o.amount) > 1000"
        )
        assert _normalize_sql(sql) == _normalize_sql(expected)

    def test_create_view_with_security(self):
        logger.info("\n--- Testing: CREATE VIEW with SECURITY clause ---")
        view = View("secure_view", "SELECT 1", security="INVOKER")
        stmt = CreateView(view)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE VIEW secure_view SECURITY INVOKER AS SELECT 1"
        assert _normalize_sql(sql) == _normalize_sql(expected)

    def test_create_view_with_schema(self):
        # With a qualified name (schema)
        self.logger.info("Testing CREATE VIEW with schema (qualified name)")
        view = View("my_view", "SELECT * FROM my_table", schema="my_schema")
        stmt = CreateView(view)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE VIEW my_schema.my_view AS SELECT * FROM my_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

    def test_drop_view(self):
        self.logger.info("Testing DROP VIEW")
        view = View("my_view", "SELECT * FROM my_table")
        stmt = DropView(view)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "DROP VIEW IF EXISTS my_view"
        assert _normalize_sql(sql) == _normalize_sql(expected)

    def test_create_materialized_view(self):
        mv = MaterializedView("my_mv", "SELECT * FROM my_table")
        stmt = CreateMaterializedView(mv)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "CREATE MATERIALIZED VIEW my_mv AS SELECT * FROM my_table"
        assert _normalize_sql(sql) == _normalize_sql(expected)

    def test_create_materialized_view_with_properties(self):
        properties = {"replication_num": "1"}
        mv = MaterializedView("my_mv", "SELECT * FROM my_table", properties=properties)
        stmt = CreateMaterializedView(mv)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = 'PROPERTIES ("replication_num" = "1")'
        assert expected in sql

    def test_drop_materialized_view(self):
        mv = MaterializedView("my_mv", "SELECT * FROM my_table")
        stmt = DropMaterializedView(mv)
        sql = str(stmt.compile(dialect=self.dialect))
        expected = "DROP MATERIALIZED VIEW IF EXISTS my_mv"
        assert _normalize_sql(sql) == _normalize_sql(expected)
