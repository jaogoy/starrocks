"""Tests for performance optimizations and default value updates."""

from starrocks.alembic.compare import (
    _parse_distribution_string,
    _normalize_column_identifiers,
    _normalize_distribution_string,
    _normalize_order_by_string
)
from starrocks.defaults import ReflectionTableDefaults


class TestRegexPattern:
    """Test performance optimizations for regex patterns."""

    def test_parse_distribution_string_with_compiled_regex(self):
        """Test that _parse_distribution_string works with pre-compiled regex."""
        # Test with BUCKETS
        result = _parse_distribution_string("HASH(id) BUCKETS 8")
        assert result == ("HASH(id)", 8)

        # Test without BUCKETS
        result = _parse_distribution_string("HASH(id)")
        assert result == ("HASH(id)", None)

        # Test with different case
        result = _parse_distribution_string("HASH(id) buckets 16")
        assert result == ("HASH(id)", 16)

        # Test with extra spaces
        result = _parse_distribution_string("HASH(id)   BUCKETS   32")
        assert result == ("HASH(id)", 32)

        # Test empty string
        result = _parse_distribution_string("")
        assert result == ("", None)

    def test_normalize_column_identifiers_with_compiled_regex(self):
        """Test that _normalize_column_identifiers works with pre-compiled regex."""
        # Test backticks removal
        result = _normalize_column_identifiers("`id`, `name`, `created_at`")
        assert result == "id, name, created_at"

        # Test whitespace normalization
        result = _normalize_column_identifiers("id   name    created_at")
        assert result == "id name created_at"

        # Test mixed backticks and whitespace
        result = _normalize_column_identifiers("`id`   `name`    `created_at`")
        assert result == "id name created_at"

        # Test empty string
        result = _normalize_column_identifiers("")
        assert result == ""

        # Test None
        result = _normalize_column_identifiers(None)
        assert result is None

    def test_normalize_distribution_string(self):
        """Test distribution string normalization."""
        result = _normalize_distribution_string("HASH(`id`) BUCKETS 8")
        assert result == "HASH(id) BUCKETS 8"

        result = _normalize_distribution_string("RANDOM   BUCKETS   16")
        assert result == "RANDOM BUCKETS 16"

    def test_normalize_order_by_string(self):
        """Test ORDER BY string normalization."""
        # Test string input
        result = _normalize_order_by_string("`id` ASC, `name` DESC")
        assert result == "id ASC, name DESC"

        # Test list input
        result = _normalize_order_by_string(["`id`", "`name`"])
        assert result == "id, name"

        # Test None input
        result = _normalize_order_by_string(None)
        assert result == ""


class TestDefaultValues:
    """Test default value constants."""

    def test_engine_normalization(self):
        """Test engine normalization logic."""
        # Test None -> DEFAULT
        result = ReflectionTableDefaults.normalize_engine(None)
        assert result == ReflectionTableDefaults.engine()

        # Test empty string -> DEFAULT
        result = ReflectionTableDefaults.normalize_engine("")
        assert result == ReflectionTableDefaults.engine()

        # Test DEFAULT -> DEFAULT
        result = ReflectionTableDefaults.normalize_engine(ReflectionTableDefaults.engine())
        assert result == ReflectionTableDefaults.engine()

        # Test other engine -> unchanged
        result = ReflectionTableDefaults.normalize_engine("MYSQL")
        assert result == "MYSQL"
