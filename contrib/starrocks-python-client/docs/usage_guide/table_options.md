# StarRocks Table Options in SQLAlchemy

When defining a StarRocks table using `sqlalchemy.Table`, you can specify various StarRocks-specific physical properties. This is achieved by passing special keyword arguments, prefixed with `starrocks_`, either directly to the `Table` constructor or within the `__table_args__` dictionary when using the ORM.

This document outlines the available options and how to use them.

## General Syntax

```python
from sqlalchemy import Table, MetaData, Column, Integer, String

metadata = MetaData()

my_table = Table(
    'my_table',
    metadata,
    Column('id', Integer),
    Column('dt', Datetime),
    Column('data', String(255)),
    comment="my first sqlalchemy table"

    # StarRocks-specific options follow
    starrocks_ENGINE="OLAP",
    starrocks_PRIMARY_KEY="dt, id",
    starrocks_PARTITION_BY="date_trunc('day', dt)",
    starrocks_DISTRIBUTED_BY="HASH(id) BUCKETS 10",
    starrocks_ORDERY_BY="id",
    starrocks_PROPERTIES={"replication_num": "3"}
)
```

## Available Options

Here is a comprehensive list of the supported `starrocks_` prefixed arguments. The order of attributes in the documentation follows the recommended order in the `CREATE TABLE` DDL statement.

### 1. `starrocks_ENGINE`

Specifies the table engine. Currently, only `OLAP` is supported and it is also the default.

- **Type**: `str`
- **Default**: `"OLAP"`
- **Example**: `starrocks_ENGINE="OLAP"`

### 2. Table Type (`starrocks_*_KEY`)

Defines the table's type (key type) and the columns that constitute the key. You can choose **at most one** of the following options.

- **`starrocks_PRIMARY_KEY`**

  - **Description**: Defines a Primary Key type table. Data is sorted by the primary key, and each row is unique.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_PRIMARY_KEY="user_id, event_date"`

  > - You can't specify the Primary Key type in a column, such as `Column('id', Integer, primary_key=True)`, which is not supported for StarRocks.
  > - You can't specify the Primary Key type by using `PrimaryKeyConstraint` either.

- **`starrocks_DUPLICATE_KEY`**

  - **Description**: Defines a Duplicate Key type table. This is the default type if no key is specified. It's suitable for storing raw, unchanged data.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_DUPLICATE_KEY="request_id, timestamp"`

- **`starrocks_AGGREGATE_KEY`**

  - **Description**: Defines an Aggregate Key type table. Rows with the same key are aggregated into a single row.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_AGGREGATE_KEY="site_id, visit_date"`

- **`starrocks_UNIQUE_KEY`**
  - **Description**: Defines a Unique Key type table, where all rows are unique. It functions like a primary key but with a different underlying implementation strategy. You could use it only when Primary Key type can't satisfy you.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_UNIQUE_KEY="device_id"`

### 3. `COMMENT`

It's **NOT** supported to specify the comment by using `starrocks_COMMENT="xxx"`. You can specify the it as normal parameters in `sqlalchemy.Table()`.

### 4. `starrocks_PARTITION_BY`

Defines the partitioning strategy for the table.

- **Type**: `str`
- **Example**:

    ```python
    starrocks_PARTITION_BY="""RANGE(event_date) (
        START ('2022-01-01') END ('2023-01-01') EVERY (INTERVAL 1 DAY)
    )"""
    ```

### 5. `starrocks_DISTRIBUTED_BY`

Specifies the data distribution (including bucketing) strategy.

- **Type**: `str`
- **Default**: `RANDOM`
- **Example**: `starrocks_DISTRIBUTED_BY="HASH(user_id) BUCKETS 32"`

### 6. `starrocks_ORDER_BY`

Specifies the columns for sorting data within Duplicate, Aggregate, and Unique Key Types, which can optimize query performance.

- **Type**: `str` (comma-separated column names)
- **Example**: `starrocks_ORDER_BY="event_timestamp, event_type"`

### 7. `starrocks_PROPERTIES`

A dictionary of additional table properties.

- **Type**: `dict[str, str]`
- **Example**:
  ```python
  starrocks_PROPERTIES={
      "replication_num": "3",
      "storage_medium": "SSD",
      "enable_persistent_index": "true"
  }
  ```

## Integration with Alembic

The `sqlalchemy-starrocks` dialect integrates with Alembic to support autogeneration of schema migrations. When you run `alembic revision --autogenerate`, it will compare the `starrocks_` options on your SQLAlchemy Types against the state of the database and generate the appropriate `ALTER TABLE` operations for supported attributes (`DISTRIBUTED_BY`, `ORDER_BY`, and `PROPERTIES`).

Note that changes to non-alterable attributes like `ENGINE`, `table type`, or `partitioning` will be detected, but will raise an error to prevent generating an unsupported migration.
