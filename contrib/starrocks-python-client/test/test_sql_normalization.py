"""Tests for SQL normalization functions."""

from starrocks.utils import TableAttributeNormalizer


class TestStripIdentifierBackticks:
    """Test the _strip_identifier_backticks function."""

    def test_simple_backtick_removal(self):
        """Test basic backtick removal."""
        sql = "SELECT `column_name` FROM `table_name`"
        expected = "SELECT column_name FROM table_name"
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_preserve_string_literals_single_quotes(self):
        """Test that backticks inside single-quoted strings are preserved."""
        sql = "SELECT `id`, 'don\\'t remove `backticks` here' FROM `users`"
        expected = "SELECT id, 'don\\'t remove `backticks` here' FROM users"
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_preserve_string_literals_double_quotes(self):
        """Test that backticks inside double-quoted strings are preserved."""
        sql = 'SELECT `id`, "keep `backticks` in here" FROM `users`'
        expected = 'SELECT id, "keep `backticks` in here" FROM users'
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_escaped_quotes_in_strings(self):
        """Test proper handling of escaped quotes within strings."""
        sql = "SELECT `id`, 'it\\'s a `test`' FROM `table`"
        expected = "SELECT id, 'it\\'s a `test`' FROM table"
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_complex_sql_with_mixed_content(self):
        """Test complex SQL with functions, operators, and mixed quotes."""
        sql = """
        SELECT `u`.`id`, CONCAT('User: `', `u`.`name`, '`') as display_name
        FROM `users` `u`
        WHERE `u`.`status` = 'active'
        """
        expected = """
        SELECT u.id, CONCAT('User: `', u.name, '`') as display_name
        FROM users u
        WHERE u.status = 'active'
        """
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_no_backticks(self):
        """Test SQL without backticks remains unchanged."""
        sql = "SELECT id, name FROM users WHERE status = 'active'"
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == sql

    def test_empty_string(self):
        """Test empty string handling."""
        result = TableAttributeNormalizer.strip_identifier_backticks("")
        assert result == ""

    def test_only_backticks(self):
        """Test string with only backticks."""
        sql = "````"
        expected = ""
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_mixed_quote_types(self):
        """Test mixed single and double quotes."""
        sql = '''SELECT `id`, "name with `backticks`", 'status with `backticks`' FROM `users`'''
        expected = '''SELECT id, "name with `backticks`", 'status with `backticks`' FROM users'''
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected

    def test_nested_quotes_complex(self):
        """Test complex nested quote scenarios."""
        sql = "SELECT `col1`, 'value with \\'nested\\' and `backticks`' FROM `table`"
        expected = "SELECT col1, 'value with \\'nested\\' and `backticks`' FROM table"
        result = TableAttributeNormalizer.strip_identifier_backticks(sql)
        assert result == expected


class TestNormalizeSQL:
    """Test the normalize_sql function (which uses _strip_identifier_backticks)."""

    def test_full_normalization(self):
        """Test complete SQL normalization."""
        sql = """
        -- This is a comment
        SELECT `u`.`id`,   `u`.`name`
        FROM  `users`   `u`
        WHERE `u`.`status`  =  'active'
        """
        expected = "select u.id, u.name from users u where u.status = 'active'"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_preserve_backticks_in_strings_during_normalization(self):
        """Test that normalization preserves backticks within string literals."""
        sql = "SELECT `id`, 'keep `these` backticks' FROM `table`"
        expected = "select id, 'keep `these` backticks' from table"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_comment_removal(self):
        """Test that SQL comments are properly removed."""
        sql = """
        SELECT `id` -- user identifier
        FROM `users` -- main table
        """
        expected = "select id from users"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_whitespace_normalization(self):
        """Test whitespace collapse and trimming."""
        sql = "  SELECT   `id`  ,  `name`   FROM   `users`  "
        expected = "select id , name from users"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_none_input(self):
        """Test None input handling."""
        result = TableAttributeNormalizer.normalize_sql(None)
        assert result is None

    def test_empty_string_normalization(self):
        """Test empty string normalization."""
        result = TableAttributeNormalizer.normalize_sql("")
        assert result == ""

    def test_case_conversion(self):
        """Test case conversion to lowercase."""
        sql = "SELECT `ID`, `NAME` FROM `USERS` WHERE `STATUS` = 'ACTIVE'"
        expected = "select id, name from users where status = 'active'"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected


class TestStarRocksSpecificScenarios:
    """Test scenarios specific to StarRocks SQL patterns."""

    def test_distribution_clause(self):
        """Test DISTRIBUTED BY clause normalization."""
        sql = "DISTRIBUTED BY HASH(`user_id`) BUCKETS 10"
        expected = "distributed by hash(user_id) buckets 10"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_partition_clause(self):
        """Test PARTITION BY clause normalization."""
        sql = "PARTITION BY RANGE(`date_col`) (PARTITION p1 VALUES [('2023-01-01'), ('2023-02-01')))"
        expected = "partition by range(date_col) (partition p1 values [('2023-01-01'), ('2023-02-01')))"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_properties_clause(self):
        """Test PROPERTIES clause with quoted values."""
        sql = '''PROPERTIES ("replication_num" = "3", "storage_medium" = "SSD")'''
        expected = '''properties ("replication_num" = "3", "storage_medium" = "ssd")'''
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected

    def test_view_definition_with_backticks(self):
        """Test view definition normalization (typical use case)."""
        sql = """
        SELECT
            `t1`.`id`,
            `t1`.`name`,
            `t2`.`category`
        FROM `table1` `t1`
        JOIN `table2` `t2` ON `t1`.`id` = `t2`.`table1_id`
        WHERE `t1`.`status` = 'active'
        """
        expected = "select t1.id, t1.name, t2.category from table1 t1 join table2 t2 on t1.id = t2.table1_id where t1.status = 'active'"
        result = TableAttributeNormalizer.normalize_sql(sql)
        assert result == expected
