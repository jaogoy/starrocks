# Defining StarRocks Tables with SQLAlchemy

This guide provides a comprehensive overview of how to define StarRocks tables using SQLAlchemy, which allows you to manage your database schema in Python code and integrate with tools like Alembic for migrations.

## Defining Table Properties

When defining a StarRocks table using SQLAlchemy, you can specify both table-level and column-level properties.

### Table-Level Properties (`starrocks_*` Prefixes)

StarRocks-specific physical properties for a table are configured by passing special keyword arguments, prefixed with `starrocks_`, either directly to the `Table` constructor or within the `__table_args__` dictionary (in [ORM style](#defining-tables-with-the-orm-declarative-style)).

#### General Syntax

```python
from sqlalchemy import Table, MetaData, Column, Integer, String, Datetime

metadata = MetaData()

my_table = Table(
    'my_table',
    metadata,
    Column('id', Integer),
    Column('dt', Datetime),
    Column('data', String(255)),
    comment="my first sqlalchemy table",

    # StarRocks-specific table-level options follow [optional]
    starrocks_ENGINE="OLAP",
    starrocks_PRIMARY_KEY="id, dt",
    starrocks_PARTITION_BY="date_trunc('day', dt)",
    starrocks_DISTRIBUTED_BY="HASH(id) BUCKETS 10",
    starrocks_ORDER_BY="dt, id",
    starrocks_PROPERTIES={"replication_num": "3"}
)
```

#### Available Table-Level Options

Here is a comprehensive list of the supported `starrocks_` prefixed arguments. The order of attributes in the documentation follows the recommended order in the `CREATE TABLE` DDL statement.

##### 1. `starrocks_ENGINE`

Specifies the table engine. `OLAP` is the default and only supported engine.

- **Type**: `str`
- **Default**: `"OLAP"`

##### 2. Table Type (`starrocks_*_KEY`)

Defines the table's type (key) and the columns that constitute the key. You must choose **at most one** of the following options.

- **`starrocks_PRIMARY_KEY`**
  > - You **can't** specify the Primary Key type in a column, such as `Column('id', Integer, primary_key=True)`, which is not supported for StarRocks.
  > - You **can't** specify the Primary Key type by using `PrimaryKeyConstraint` either.

  - **Description**: Defines a Primary Key type table. Data is sorted by the primary key, and each row is unique.
  - **Type**: `str` (comma-separated column names)
  - **Example**: `starrocks_PRIMARY_KEY="user_id, event_date"`

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

##### 3. `COMMENT`

The table comment should be passed as the standard `comment` keyword argument to the `Table` constructor, not as a `starrocks_` prefix.

##### 4. `starrocks_PARTITION_BY`

Defines the partitioning strategy for the table.

- **Type**: `str`
- **Example**:

  ```Python
  starrocks_PARTITION_BY="""RANGE(event_date) (
      START ('2022-01-01') END ('2023-01-01') EVERY (INTERVAL 1 DAY)
  )"""
  ```

##### 5. `starrocks_DISTRIBUTED_BY`

Specifies the data distribution (including bucketing) strategy.

- **Type**: `str`
- **Default**: `RANDOM`
- **Example**: `starrocks_DISTRIBUTED_BY="HASH(user_id) BUCKETS 32"`

##### 6. `starrocks_ORDER_BY`

Specifies the sorting columns to optimize query performance.

- **Type**: `str` (comma-separated column names)
- **Example**: `starrocks_ORDER_BY="event_timestamp, event_type"`

##### 7. `starrocks_PROPERTIES`

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

### Column-Level Properties (Aggregate Types)

For `AGGREGATE KEY` tables, you can specify an aggregate function for each value column (i.e., non-key columns). This is done by passing an `info` dictionary with the key `starrocks_AGG_TYPE` to the `Column` constructor.

#### General Syntax

```python
Column('page_views', Integer, info={'starrocks_AGG_TYPE': 'SUM'}),
```

#### Available Aggregate Types

The following aggregate types (`AGG_TYPE`) are supported:

- **`SUM`**: Sums the values for rows with the same key.
- **`REPLACE`**: Replaces existing values with the newest value for rows with the same key.
- **`REPLACE_IF_NOT_NULL`**: Replaces existing values only if the new value is not null.
- **`MAX`**: Keeps the maximum value.
- **`MIN`**: Keeps the minimum value.
- **`HLL_UNION`**: Aggregates data using HyperLogLog.
- **`BITMAP_UNION`**: Aggregates data using Bitmap.

### Example: Aggregate Key Table

Here is a complete example of an `AGGREGATE KEY` table that demonstrates both table-level and column-level properties.

```python
from sqlalchemy import Table, MetaData, Column, Integer, String, Date
from starrocks.types import BITMAP, HLL

metadata = MetaData()

aggregate_table = Table(
    'aggregate_table',
    metadata,
    # Key columns do not need special properties
    Column('event_date', Date),
    Column('site_id', Integer),

    # Value columns with aggregate types specified in `info`
    Column('page_views', Integer, info={'starrocks_AGG_TYPE': 'SUM'}),
    Column('last_visit_time', Date, info={'starrocks_AGG_TYPE': 'REPLACE'}),
    Column('user_ids', BITMAP, info={'starrocks_AGG_TYPE': 'BITMAP_UNION'}),
    Column('uv_estimate', HLL, info={'starrocks_AGG_TYPE': 'HLL_UNION'}),

    # Table-level options
    starrocks_AGGREGATE_KEY="event_date, site_id",  # Must specify it for AGGREGATE tabke
    starrocks_PARTITION_BY="date_trunc('day', event_date)",
    starrocks_DISTRIBUTED_BY="RANDOM",
    starrocks_PROPERTIES={"replication_num": "3"}
)
```

## Defining Tables with the ORM (Declarative Style)

When using SQLAlchemy's Declarative style, you define table-level properties within the `__table_args__` dictionary. Column-level properties are defined using the `info` dictionary on each `Column`.

### Example: ORM Aggregate Key Table

Here is a complete example of an `AGGREGATE KEY` table defined using the Declarative style. It demonstrates both table-level and column-level properties.

```python
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import declarative_base
from starrocks.types import BITMAP, HLL

Base = declarative_base()

class PageViewAggregates(Base):
    __tablename__ = 'page_view_aggregates'

    # -- Key Columns --
    # For AGGREGATE KEY tables, key columns can be marked with `starrocks_is_agg_key`.
    # This is optional but improves clarity and allows for validation.
    page_id = Column(Integer, info={'starrocks_is_agg_key': True})
    visit_date = Column(Date)

    # -- Value Columns --
    # Value columns have their aggregate function specified in the `info` dict.
    total_views = Column(Integer, info={'starrocks_agg': 'SUM'})
    last_user = Column(String, info={'starrocks_agg': 'REPLACE'})
    distinct_users = Column(BITMAP, info={'starrocks_agg': 'BITMAP_UNION'})
    uv_estimate = Column(HLL, info={'starrocks_agg': 'HLL_UNION'})

    # -- Table-Level Arguments --
    __table_args__ = {
        'starrocks_aggregate_key': 'page_id, visit_date',
        'starrocks_partition_by': 'date_trunc("day", visit_date)',
        'starrocks_distributed_by': 'HASH(page_id)',
        'starrocks_properties': {"replication_num": "3"}
    }
```

## Integration with Alembic

The `sqlalchemy-starrocks` dialect integrates with Alembic to support autogeneration of schema migrations. When you run `alembic revision --autogenerate`, it will compare both the table-level `starrocks_` options and column-level properties against the database and generate the appropriate DDL.

Note that changes to non-alterable attributes like `ENGINE`, `table type`, or `partitioning` will be detected, but will raise an error to prevent generating an unsupported migration.
