import logging
from sqlalchemy.dialects import registry

from starrocks.sql.ddl import CreateView, DropView, AlterView
from starrocks.sql.schema import View
from test.test_utils import normalize_sql


class TestViewCompiler:
    @classmethod
    def setup_class(cls):
        cls.logger = logging.getLogger(__name__)
        cls.dialect = registry.load("starrocks")()

    def test_create_view(self):
        view = View("my_view", "SELECT * FROM my_table")
        sql = str(CreateView(view).compile(dialect=self.dialect))
        expected = "CREATE VIEW my_view AS SELECT * FROM my_table"
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_view_variations(self):
        view = View("simple_view", "SELECT c1, c2, c3 FROM test_table")
        sql = str(CreateView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW simple_view AS SELECT c1, c2, c3 FROM test_table"
        )

        view = View("simple_view", "SELECT c1, c2, c3 FROM test_table")
        sql = str(CreateView(view, or_replace=True).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE OR REPLACE VIEW simple_view AS SELECT c1, c2, c3 FROM test_table"
        )

        view = View("simple_view", "SELECT c1 FROM test_table")
        sql = str(CreateView(view, if_not_exists=True).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW IF NOT EXISTS simple_view AS SELECT c1 FROM test_table"
        )

        view = View("simple_view", "SELECT c1 FROM test_table", schema="test_db")
        sql = str(CreateView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW test_db.simple_view AS SELECT c1 FROM test_table"
        )

        view = View(
            "commented_view",
            "SELECT c1, c2 FROM test_table",
            comment="This is a view with a comment",
        )
        sql = str(CreateView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW commented_view COMMENT 'This is a view with a comment' AS SELECT c1, c2 FROM test_table"
        )

        view = View(
            "view_with_columns",
            "SELECT c1, c2 FROM test_table",
            columns=["col_a", "col_b"],
        )
        sql = str(CreateView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW view_with_columns(col_a, col_b) AS SELECT c1, c2 FROM test_table"
        )

        view = View(
            "view_with_column_comments",
            "SELECT c1, c2 FROM test_table",
            columns=[
                {"name": "col_a", "comment": "This is the first column"},
                {"name": "col_b", "comment": "This is the second column"},
            ],
        )
        sql = str(CreateView(view).compile(dialect=self.dialect))
        expected = (
            "CREATE VIEW view_with_column_comments("
            "col_a COMMENT 'This is the first column', "
            "col_b COMMENT 'This is the second column') "
            "AS SELECT c1, c2 FROM test_table"
        )
        assert normalize_sql(sql) == normalize_sql(expected)

    def test_create_view_with_security(self):
        view = View("secure_view", "SELECT 1", security="INVOKER")
        sql = str(CreateView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql(
            "CREATE VIEW secure_view SECURITY INVOKER AS SELECT 1"
        )

    def test_drop_view(self):
        view = View("my_view", "SELECT * FROM my_table")
        sql = str(DropView(view).compile(dialect=self.dialect))
        assert normalize_sql(sql) == normalize_sql("DROP VIEW IF EXISTS my_view")

    def test_compile_alter_view(self):
        sql = str(AlterView(View("my_view", "SELECT 2", comment="New Comment", security="DEFINER")).compile(dialect=self.dialect))
        expected = """
        ALTER VIEW my_view
        AS
        SELECT 2
        """
        assert normalize_sql(sql) == normalize_sql(expected)


