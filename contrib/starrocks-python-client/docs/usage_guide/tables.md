# Defining StarRocks Tables with SQLAlchemy

This guide provides a comprehensive overview of how to define StarRocks tables using SQLAlchemy, which allows you to manage your database schema in Python code and integrate with tools like Alembic for migrations.

## Defining Table Properties

When defining a StarRocks table using SQLAlchemy, you can specify both table-level and column-level properties using keyword arguments prefixed with `starrocks_`.

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

StarRocks-specific physical properties for a table are configured by passing special keyword arguments, prefixed with `starrocks_`, either directly to the `Table` constructor or within the `__table_args__` dictionary for ORM style.

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

Defines the partitioning strategy.

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

Specifies the sorting columns.

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

### Column-Level Properties (`starrocks_*` Prefixes)

For `AGGREGATE KEY` tables, you can specify properties for each column by passing a `starrocks_` prefixed keyword argument directly to the `Column` constructor.

#### Available Column-Level Options

- **`starrocks_agg_type`**: A string specifying the aggregate type for a value column. Supported values are:
  - `'SUM'`, `'REPLACE'`, `'REPLACE_IF_NOT_NULL'`, `'MAX'`, `'MIN'`, `'HLL_UNION'`, `'BITMAP_UNION'`
- **`starrocks_is_agg_key`**: A boolean that can be set to `True` to explicitly mark a column as a key in an `AGGREGATE KEY` table. This is optional but improves clarity.

### Example: Aggregate Key Table

Here is a complete example of an `AGGREGATE KEY` table that demonstrates both table-level and column-level properties.

```python
from sqlalchemy import Table, MetaData, Column, Integer, Date
from starrocks.types import BITMAP, HLL

metadata = MetaData()

aggregate_table = Table(
    'aggregate_table',
    metadata,
    # Key columns (explicitly marked for clarity)
    Column('event_date', Date, starrocks_is_agg_key=True),
    Column('site_id', Integer, starrocks_is_agg_key=True),

    # Value columns with aggregate types
    Column('page_views', Integer, starrocks_agg_type='SUM'),
    Column('last_visit_time', Date, starrocks_agg_type='REPLACE'),
    Column('user_ids', BITMAP, starrocks_agg_type='BITMAP_UNION'),
    Column('uv_estimate', HLL, starrocks_agg_type='HLL_UNION'),

    # Table-level options
    starrocks_AGGREGATE_KEY="event_date, site_id",
    starrocks_PARTITION_BY="date_trunc('day', event_date)",
    starrocks_DISTRIBUTED_BY="RANDOM",
    starrocks_PROPERTIES={"replication_num": "3"}
)
```

## Defining Tables with the ORM (Declarative Style)

When using SQLAlchemy's Declarative style, you define table-level properties within the `__table_args__` dictionary. Column-level properties are passed directly as keyword arguments to each `Column`.

### Example: ORM Aggregate Key Table

```python
from sqlalchemy import Column, Integer, String, Date
from sqlalchemy.orm import declarative_base
from starrocks.types import BITMAP, HLL

Base = declarative_base()

class PageViewAggregates(Base):
    __tablename__ = 'page_view_aggregates'

    # -- Key Columns --
    page_id = Column(Integer, primary_key=True, starrocks_is_agg_key=True)
    visit_date = Column(Date, starrocks_is_agg_key=True)

    # -- Value Columns --
    total_views = Column(Integer, starrocks_agg_type='SUM')
    last_user = Column(String, starrocks_agg_type='REPLACE')
    distinct_users = Column(BITMAP, starrocks_agg_type='BITMAP_UNION')
    uv_estimate = Column(HLL, starrocks_agg_type='HLL_UNION')

    # -- Table-Level Arguments --
    __table_args__ = {
        'starrocks_aggregate_key': 'page_id, visit_date',
        'starrocks_partition_by': 'date_trunc("day", visit_date)',
        'starrocks_distributed_by': 'HASH(page_id)',
        'starrocks_properties': {"replication_num": "3"}
    }
```

## Integration with Alembic

The `sqlalchemy-starrocks` dialect integrates with Alembic to support autogeneration of schema migrations. When you run `alembic revision --autogenerate`, it will compare both the table-level and column-level `starrocks_` options against the database and generate the appropriate DDL.

Note that changes to non-alterable attributes like `ENGINE`, `table type`, or `partitioning` will be detected, but will raise an error to prevent generating an unsupported migration.
