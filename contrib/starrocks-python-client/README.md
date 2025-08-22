# StarRocks Python Client

A StarRocks client for the Python programming language.

StarRocks is the next-generation data platform designed to make data-intensive real-time analytics fast and easy. It delivers query speeds 5 to 10 times faster than other popular solutions. StarRocks can perform real-time analytics well while updating historical records. It can also enhance real-time analytics with historical data from data lakes easily. With StarRocks, you can get rid of the de-normalized tables and get the best performance and flexibility.

A StarRocks client and SQLAlchemy dialect for the Python programming language. This dialect allows developers to interact with StarRocks using the powerful features of SQLAlchemy and manage their database schema declaratively with Alembic.

## Installation

```bash
pip install starrocks
```

## SQLAlchemy Usage

To connect to StarRocks using SQLAlchemy, use a connection string (URL) following this pattern:

- **User**: User Name
- **Password**: DBPassword
- **Host**: StarRocks FE Host
- **Catalog**: Catalog Name
- **Database**: Database Name
- **Port**: StarRocks FE port

Here's what the connection string looks like:

```ini
starrocks://<User>:<Password>@<Host>:<Port>/<Catalog>.<Database>
```

*Note: The `Catalog` can be omitted and is managed by StarRocks' `default_catalog`.*

### Example: Basic Operations

```python
from sqlalchemy import create_engine, text

# Connect to the 'default_catalog'
engine = create_engine('starrocks://root:@localhost:9030/mydatabase')

with engine.connect() as connection:
    rows = connection.execute(text("SELECT * FROM my_table")).fetchall()
    print(rows)
```

## Alembic Integration for Schema Management

This dialect extends Alembic to support automated schema migrations for StarRocks, including Tables, Views, and Materialized Views.

### 1. Configuration

First, configure your Alembic environment.

**In `alembic.ini`:**

Set your `sqlalchemy.url` to point to your StarRocks database.

```ini
[alembic]
# ... other configs
sqlalchemy.url = starrocks://root:@localhost:9030/mydatabase
```

**In `env.py`:**

Ensure the StarRocks dialect is imported so Alembic's autogenerate can find the custom comparison logic.

```python
# Add this import at the top of your env.py
import starrocks.alembic
# ... rest of env.py
```

### 2. Defining Schema Objects

You define your schema in your Python models file (e.g., `models.py`) using SQLAlchemy's declarative style.

#### Defining Tables with StarRocks Properties

You can specify StarRocks-specific properties using the `__table_args__` dictionary.

```python
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String

Base = declarative_base()

class MyTable(Base):
    __tablename__ = 'my_table'
    id = Column(Integer, primary_key=True)
    name = Column(String(50))

    __table_args__ = {
        "starrocks_PRIMARY_KEY": "id",
        "starrocks_engine": "OLAP",
        "starrocks_comment": "table comment",
        "starrocks_distributed_by": "HASH(id) BUCKETS 10",
        "starrocks_partition_by": "RANGE (id) (PARTITION p1 VALUES LESS THAN ('100'))",
        "starrocks_properties": (
            ("storage_medium", "SSD"),
            ("storage_cooldown_time", "2025-06-04 00:00:00"),
            ("replication_num", "1")
        )
    }
```

#### Defining Views and Materialized Views

Define Views and Materialized Views using the provided `View` and `MaterializedView` classes. These objects should be associated with your `MetaData` object.

```python
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column, Integer, String, MetaData
from starrocks.sql.schema import View, MaterializedView

Base = declarative_base()

# --- Comprehensive View and Materialized View Definitions ---

# Get the metadata object from the Base
metadata = Base.metadata

# Define a View with all supported clauses
my_view = View(
    'my_view',
    "SELECT id, name FROM my_table WHERE id > 50",
    schema='my_schema',
    comment='A sample view with all options.',
    columns=['user_id', 'user_name'],
    security='INVOKER'
)

# Define a Materialized View
my_mv = MaterializedView(
    'my_mv',
    "SELECT name, count(1) FROM my_table GROUP BY name",
    properties={'replication_num': '1'}
)

# Associate with metadata so autogenerate can find them
metadata.info.setdefault('views', {})[(my_view, 'my_schema')] = my_view
metadata.info.setdefault('materialized_views', {})[(my_mv, None)] = my_mv
```

### 3. Generating and Applying Migrations

Follow the standard Alembic workflow:

1. **Generate a new revision:**
    Alembic will compare your Python models with the database and generate a migration script.

    ```bash
    alembic revision --autogenerate -m "Create initial tables and views"
    ```

2. **Review the script:**
    Check the generated file in your `versions/` directory. It will contain `op.create_table()`, `op.create_view()`, etc.

3. **Apply the migration:**
    Run the `upgrade` command to apply the changes to your StarRocks database.

    ```bash
    alembic upgrade head
    ```

### 4. Debugging and Logging

To see the raw SQL that the dialect compiles and executes, you can configure logging.

**For Alembic commands:**

Add a logger for `starrocks.dialect` in your `alembic.ini` and set the level to `DEBUG`.

```ini
[loggers]
keys = root,sqlalchemy,alembic,starrocks.dialect

# ... other loggers

[logger_starrocks.dialect]
level = DEBUG
handlers =
qualname = starrocks.dialect
```

**For `pytest`:**

Create a `pytest.ini` file in your project root with the following content:

```ini
[pytest]
log_cli = true
log_cli_level = DEBUG
log_cli_format = %(levelname)-5.5s [%(name)s] %(message)s
```

## Contributing

### Running Unit Tests

To run tests for the StarRocks SQLAlchemy dialect, install the test dependencies and run `pytest`.

```bash
pip install -r requirements-dev.txt
pytest
```

This will run the standard SQLAlchemy dialect test suite as well as StarRocks-specific tests. For more details, please check [SQLAlchemy's guide for dialect development](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst).
