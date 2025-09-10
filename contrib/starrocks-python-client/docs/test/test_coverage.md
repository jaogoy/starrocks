# Test Coverage Document

This document provides a high-level overview of the test coverage for the StarRocks Python Client, organized by feature area.

## 1. Table Autogeneration and Comparison (`test_autogenerate_tables.py`)

This file primarily focuses on verifying Alembic's `autogenerate` feature's ability to detect differences in StarRocks-specific table attributes between a database-reflected table and a metadata-defined table.

### General Test Patterns for Table Attributes

For Engine, Key Type, Partition By, Distributed By, Order By, Properties, Comment:

Each major table attribute is generally tested across the following scenarios:

- **Value State Changes:**
  - Transition from `None` (not set) to a default value.
  - Transition from `None` to a non-default value.
  - Transition from an explicit value (default or non-default) back to `None`.
  - Change from one explicit value to another.
- **No Change Scenarios:**
  - Database and metadata values are identical (including case-insensitive matches).
- **Special Considerations:**
  - For attributes not supporting `ALTER` operations (e.g., certain Key Type changes), `NotImplementedError` is expected.
  - For `PROPERTIES`, specific tests cover default value handling (implicit vs. explicit) and scenarios where non-default values in the DB are not specified in metadata.
  - `Comment` changes are handled by SQLAlchemy's built-in comparison.

### Other Specific Test Cases:

- **ORM Model with `__table_args__`:** Verifies correct handling of StarRocks properties defined in ORM models.
- **Non-StarRocks Dialect Handling:** Ensures comparison logic is skipped for other dialects.

## 2. Table Reflection (`test_reflection_tables.py`)

This file contains integration tests for `StarRocksDialect.get_table_options`, verifying its ability to correctly reflect StarRocks-specific table attributes from a live database via `information_schema`.

### Covered Scenarios:

- **Comprehensive Table Options Reflection:** Verifies the reflection of all StarRocks-specific table attributes, including `ENGINE`, `KEY` (e.g., PRIMARY KEY), `PARTITION BY`, `DISTRIBUTED BY`, `ORDER BY`, `PROPERTIES` (e.g., `replication_num`, `storage_medium`), and `COMMENT` for a fully configured table.

## 3. Column Aggregate Reflection (`test_reflection_agg.py`)

This file contains integration tests for `StarRocksDialect.get_columns` focusing on the reflection of StarRocks-specific column aggregation types (`AGG_TYPE`).

### Covered Scenarios:

- **Aggregate Key Table Reflection:** Confirms that `AGGREGATE KEY` table columns with various aggregate functions (`SUM`, `MIN`, `MAX`, `REPLACE`, `REPLACE_IF_NOT_NULL`, `HLL_UNION`, `BITMAP_UNION`) are correctly reflected and their `AGG_TYPE` is identifiable in `column.info`.
