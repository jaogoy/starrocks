# test/test_alter_table_operations.py
"""
Tests for ALTER TABLE operation edge cases and error handling.
"""
import logging
import pytest
from unittest.mock import Mock, patch

from starrocks.alembic.ops import AlterTableEngineOp, AlterTableKeyOp, AlterTablePartitionOp, alter_table_engine, alter_table_key, alter_table_partition


logger = logging.getLogger(__name__)


class TestUnsupportedOperations:
    """Test error handling for unsupported ALTER TABLE operations."""

    def test_alter_table_engine_not_supported(self):
        """Test that ALTER TABLE ENGINE raises NotImplementedError with proper logging."""
        operations = Mock()
        op = AlterTableEngineOp("test_table", "OLAP", schema="test_db")

        with patch('starrocks.alembic.ops.logger') as mock_logger:
            with pytest.raises(NotImplementedError, match="ALTER TABLE ENGINE is not yet supported"):
                alter_table_engine(operations, op)

            # Verify error was logged
            mock_logger.error.assert_called_once()
            error_call = mock_logger.error.call_args[0]
            assert "ALTER TABLE ENGINE is not currently supported" in error_call[0]
            assert "test_table" in error_call[1]
            assert "OLAP" in error_call[2]

    def test_alter_table_key_not_supported(self):
        """Test that ALTER TABLE KEY raises NotImplementedError with proper logging."""
        operations = Mock()
        op = AlterTableKeyOp("test_table", "PRIMARY KEY", "id", schema="test_db")

        with patch('starrocks.alembic.ops.logger') as mock_logger:
            with pytest.raises(NotImplementedError, match="ALTER TABLE KEY is not yet supported"):
                alter_table_key(operations, op)

            # Verify error was logged
            mock_logger.error.assert_called_once()
            error_call = mock_logger.error.call_args[0]
            assert "ALTER TABLE KEY is not currently supported" in error_call[0]
            assert "test_table" in error_call[1]
            assert "PRIMARY KEY" in error_call[2]
            assert "id" in error_call[3]

    def test_alter_table_partition_not_supported(self):
        """Test that ALTER TABLE PARTITION raises NotImplementedError with proper logging."""
        operations = Mock()
        op = AlterTablePartitionOp("test_table", "RANGE(date_col)", schema="test_db")

        with patch('starrocks.alembic.ops.logger') as mock_logger:
            with pytest.raises(NotImplementedError, match="ALTER TABLE PARTITION is not yet supported"):
                alter_table_partition(operations, op)

            # Verify error was logged
            mock_logger.error.assert_called_once()
            error_call = mock_logger.error.call_args[0]
            assert "ALTER TABLE PARTITION is not currently supported" in error_call[0]
            assert "test_table" in error_call[1]
            assert "RANGE(date_col)" in error_call[2]


class TestBucketsParsingEdgeCases:
    """Test edge cases in BUCKETS parsing logic."""

    def test_buckets_none_vs_zero_difference(self):
        """Test that None buckets and 0 buckets are handled differently."""
        from starrocks.sql.ddl import AlterTableDistribution

        # None buckets - no BUCKETS clause
        ddl_none = AlterTableDistribution("test_table", "HASH(id)", buckets=None)

        # Zero buckets - BUCKETS 0 clause
        ddl_zero = AlterTableDistribution("test_table", "HASH(id)", buckets=0)

        assert ddl_none.buckets is None
        assert ddl_zero.buckets == 0
        assert ddl_none.buckets != ddl_zero.buckets


class TestPropertiesOrderPreservation:
    """Test that properties order is preserved for deterministic output."""

    def test_properties_order_preservation(self):
        """Test that properties order is preserved for deterministic output."""
        from starrocks.sql.ddl import AlterTableProperties

        # Python dicts maintain insertion order (Python 3.7+)
        properties = {
            "replication_num": "3",
            "storage_medium": "SSD",
            "dynamic_partition.enable": "true"
        }

        ddl = AlterTableProperties("test_table", properties)

        # Should maintain the same order
        keys = list(ddl.properties.keys())
        expected_keys = ["replication_num", "storage_medium", "dynamic_partition.enable"]
        assert keys == expected_keys
