# StarRocks Python Client

A StarRocks client for the Python programming language.

StarRocks is a next-generation data platform designed for fast, real-time analytics. This Python client, along with its SQLAlchemy dialect and Alembic extension, allows developers to interact with StarRocks, leveraging SQLAlchemy's powerful features and managing database schemas declaratively with Alembic.

## Installation

```bash
pip install starrocks
```

### Supported Python Versions

Python >= 3.8, <= 3.12

### Using a Virtual Environment (Recommended)

It is highly recommended to install `starrocks` in a virtual environment to avoid conflicts with system-wide packages.

**Mac/Linux:**

```bash
pip install virtualenv
virtualenv <your-env-name>
source <your-env-name>/bin/activate
<your-env-name>/bin/pip install starrocks
```

**Windows:**

```bash
pip install virtualenv
virtualenv <your-env-name>
<your-env-name>\Scripts\activate
<your-env-name>\Scripts\pip.exe install starrocks
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
starrocks://<User>:<Password>@<Host>:<Port>/[<Catalog>.]<Database>
```

> Note: The `Catalog` can be omitted and is managed by StarRocks' `default_catalog`.

### Example: Basic Operations

```python
from sqlalchemy import create_engine, text

# Connect to the 'default_catalog'
engine = create_engine('starrocks://myname:pswd1234@localhost:9030/mydatabase')

with engine.connect() as connection:
    rows = connection.execute(text("SELECT * FROM mytable LIMIT 2")).fetchall()
    print(rows)
```

### Example: Advanced Table Operations (ORM Declarative Style)

For more complex table definitions, including `PRIMARY KEY`, `AGGREGATE KEY` tables, and various StarRocks-specific attributes and data types, you can use SQLAlchemy's ORM declarative style. This approach allows you to define your schema directly within Python classes.

```python
from sqlalchemy import Column, Integer, String, Date, text, create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from starrocks import BITMAP, INTEGER, DATE, STRING # Import StarRocks specific types
from datetime import date


# --- StarRocks Table declaration ---

Base = declarative_base()

class PageViewAggregates(Base):
    __tablename__ = 'page_view_aggregates'

    page_id = Column(INTEGER, primary_key=True, starrocks_is_agg_key=True)
    visit_date = Column(DATE, primary_key=True, starrocks_is_agg_key=True)
    total_views = Column(INTEGER, starrocks_agg_type='SUM')
    last_user = Column(STRING, starrocks_agg_type='REPLACE')
    distinct_users = Column(BITMAP, starrocks_agg_type='BITMAP_UNION')

    __table_args__ = {
        'starrocks_AGGREGATE_KEY': 'page_id, visit_date',
        'starrocks_PARTITION_BY': 'date_trunc("day", visit_date)',
        'starrocks_DISTRIBUTED_BY': 'HASH(page_id)',
        'starrocks_PROPERTIES': {"replication_num": "1"}
    }

# --- Data Insertion and Query Examples ---

# Assuming `engine` is already created as shown in "Basic Operations"
engine = create_engine('starrocks://myname:pswd1234@localhost:9030/mydatabase')

Base.metadata.create_all(engine) # Create the table in the database

Session = sessionmaker(bind=engine)
session = Session()

# Insert data
new_data = PageViewAggregates(
    page_id=1,
    visit_date=date(2023, 10, 26),
    total_views=100,
    last_user="user_A",
    distinct_users=None # BITMAP/HLL types might require specific functions for value insertion
)
session.add(new_data)
session.commit()

# Insert data using raw SQL (two records with same key for aggregation)
with engine.connect() as connection:
    connection.execute(
        text("""
            INSERT INTO page_view_aggregates
            (page_id, visit_date, total_views, last_user, distinct_users)
            VALUES
                (:page_id_1, :visit_date_1, :total_views_1, :last_user_1, NULL),
                (:page_id_2, :visit_date_2, :total_views_2, :last_user_2, NULL)"""),
            {
                "page_id_1": 2, "visit_date_1": "2023-10-27",
                "total_views_1": 200, "last_user_1": "user_B",
                "page_id_2": 2, "visit_date_2": "2023-10-27",
                "total_views_2": 150, "last_user_2": "user_C"
            })
    connection.commit()

# Query all data to verify aggregation
all_results = session.query(PageViewAggregates).order_by(PageViewAggregates.page_id).all()
print(f"Total unique rows after insertions: {len(all_results)}")
for row in all_results:
    print(f"  Page ID: {row.page_id}, Visit Date: {row.visit_date}, Total Views: {row.total_views}, Last User: {row.last_user}")

session.close()
```

For a more in-depth guide on defining tables, including detailed explanations of all StarRocks-specific attributes and various data types, please refer to the [Tables Usage Guide](docs/usage_guide/tables.md).

## Alembic Integration for Schema Management

Alembic is a database migration tool for SQLAlchemy that allows you to manage changes to your database schema over time. This StarRocks dialect extends Alembic to support automated schema migrations, including Tables, Views, and Materialized Views. For a general introduction to Alembic and its usage, please refer to the [Alembic Official Documentation](https://alembic.sqlalchemy.org/en/latest/).

> Views and Materialized Views will be supported in the near future.

### 1. Create and configure your Alembic project

#### Installation Alembic

First, ensure you have Alembic installed. If not, you can install it via pip:

```bash
pip install "alembic>=1.16"
```

For more detailed installation instructions, refer to the [Alembic Installation Guide](https://alembic.sqlalchemy.org/en/latest/front.html#installation).

#### Initializing an Alembic Project

If you are starting a new project, initialize an Alembic environment in your project directory:

```bash
mkdir my_sr_alembic_project  # your alembic project directory
cd my_sr_alembic_project/
alembic init alembic
```

This command creates a new `alembic` directory with the necessary configuration files and a `versions` directory for your migration scripts. For more information on setting up an Alembic project, see the [Alembic Basic Usage Guide](https://alembic.sqlalchemy.org/en/latest/tutorial.html).

#### Configuration

First, configure your Alembic environment.

**In `alembic.ini`:**

Set your `sqlalchemy.url` to point to your StarRocks database.

```ini
[alembic]
# ... other configs
sqlalchemy.url = starrocks://myname:pswd1234@localhost:9030/mydatabase
```

**In `alembic/env.py`:**

Set the metadata of you models as the `target_metadata`.

Ensure the StarRocks dialect and Alembic integration is imported and configure `render_item` (in both `run_migrations_offline()` and `run_migrations_online()`), so autogenerate recognizes StarRocks column types.

```python
# Add these imports at the top of your env.py
from starrocks.alembic import render  # For type rendering
from starrocks.alembic.starrocks import StarRocksImpl  # Ensure impl registered
from myapps import models  # Adjust 'myapps.models' to your actual models file path as defined later
# from myapps import models_view  # Import mv as well

target_metadata = models.Base.metadata

# Set the replication (by using kwargs) for test env if there is only on BE
version_table_kwargs = {
    "starrocks_properties": {"replication_num":"1"},
}

# In both run_migrations_offline() and run_migrations_online()
def run_migrations_offline() -> None:
    ...
    context.configure(
        # ... other parameters ...
        render_item=render.render_column_type,
        version_table_kwargs=version_table_kwargs,
    )
    ...

def run_migrations_online() -> None:
    ...
    with connectable.connect() as connection:
        context.configure(
            # ... other parameters ...
            render_item=render.render_column_type,
            version_table_kwargs=version_table_kwargs,
        )
        ...
```

### 2. Defining Schema Objects

You define your schema in your Python models file (e.g., `models.py`) using SQLAlchemy's declarative style.

> You can put `models.py` into a directory `myapps` under your alembic project. such as `my_sr_alembic_project/myapps/models.py`.

#### Defining Tables with StarRocks Attributes and Types

Use uppercase types (such as `INTEGER` instead of `Integer`) from `starrocks` and specify StarRocks attributes via `__table_args__` (in ORM style).

```python
# models.py
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from starrocks import INTEGER, VARCHAR

Base = declarative_base()

class MyTable(Base):
    __tablename__ = 'my_table'
    id = Column(INTEGER, primary_key=True)
    name = Column(VARCHAR(50))

    __table_args__ = {
        'starrocks_PRIMARY_KEY': 'id',
        'starrocks_ENGINE': 'OLAP',
        'starrocks_COMMENT': 'table comment',
        'starrocks_DISTRIBUTED_BY': 'HASH(id) BUCKETS 10',
        'starrocks_PARTITION_BY': """RANGE (id) (
                PARTITION p1 VALUES LESS THAN ('100')
            )""",
        'starrocks_PROPERTIES': {
            'storage_medium': 'SSD',
            'storage_cooldown_time': '2025-06-04 00:00:00',
            'replication_num': '1'
        }
    }
```

**Note**: All columns that appear in a StarRocks key (`starrocks_PRIMARY_KEY`, `starrocks_UNIQUE_KEY`, `starrocks_DUPLICATE_KEY`, or `starrocks_AGGREGATE_KEY`) must also be marked with `primary_key=True` in their `Column(...)` declarations.

> In the above example, it the `id` column.

**Note**: Usage mirrors SQLAlchemy’s patterns (e.g., MySQL), but always import and use uppercase types from `starrocks`.

#### Defining Views and Materialized Views (for future)

Define Views and Materialized Views using the provided `View` and `MaterializedView` classes. These objects should be associated with your `MetaData` object.

```python
# models_view.py
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
    metadata,
    schema='my_schema',
    comment='A sample view with all options.',
    columns=['user_id', 'user_name'],
    security='INVOKER',
    metadata=metadata  # Associate with metadata automatically
)

# Define a Materialized View
my_mv = MaterializedView(
    'my_mv',
    "SELECT name, count(1) FROM my_table GROUP BY name",
    metadata,
    properties={'replication_num': '1'},
    metadata=metadata  # Associate with metadata automatically
)
```

### 3. Generating and Applying Migrations

Follow the standard Alembic workflow:

1. **Generate a new revision:**
   Alembic will compare your Python models with the database and generate a migration script.

   ```bash
   alembic revision --autogenerate -m "Create initial tables and views"
   ```

   The generated script (e.g., `versions/<revision_id>_...py`) contains an `upgrade()` function with operations like `op.create_table()` to apply your schema, and a `downgrade()` function to revert it.

2. **Review the script:**
   Check the generated file in your `versions/` directory. It will contain `op.create_table()`, `op.create_view()`, etc.

3. **Apply the migration:**
   Run the `upgrade` command to apply the changes to your StarRocks database.

   ```bash
   alembic upgrade head
   ```

If there is some problems of the generated script (e.g., `versions/<revision_id>_...py`), or some problems of the `models.py`, you should delete the generated script file, and re-run the `--autogenerate` commond above, to re-generate a migration script.

#### View Autogenerate Details and Limitations (for future)

When you define `View` or `MaterializedView` objects in your model files (e.g., `models_view.py`), Alembic's autogenerate process will detect them and create the appropriate migration operations, which are similar with Tables.

The generated migration script will contain `op.create_view`, `op.drop_view`, `op.create_materialized_view`, and `op.drop_materialized_view`.

A typical generated snippet for creating a view might look like this:

```python
# inside versions/<revision_id>_...py
def upgrade():
    op.create_view('my_view', 'SELECT id, name FROM my_table WHERE id > 50')

def downgrade():
    op.drop_view('my_view')
```

- **Autogenerate will detect:**
  - New views/MVs in metadata: emits `op.create_view(...)` or `op.create_materialized_view(...)`.
  - Dropped views/MVs from metadata: emits `op.drop_view(...)` or `op.drop_materialized_view(...)`.
  - Definition changes: emits `op.alter_view(...)` or `op.alter_materialized_view(...)`.
- **StarRocks Limitation:** `ALTER VIEW` only supports redefining the `AS SELECT` clause. It does not support changing `COMMENT` or `SECURITY` directly. If only `COMMENT`/`SECURITY` change, no operation is emitted; if the definition also changes, those attributes are ignored and only `ALTER VIEW` is generated.
- **Definition Comparison:** View/MV definition comparison uses normalization: remove identifier backticks, strip comments, collapse whitespace, and compare case-insensitively. But, it's still recommended to give the definition with a good and unified SQL style.

### 4. Modifying Existing Tables and Applying a New Migration

After your initial migration, you may need to modify your tables. Let's say you want to add a new column to `MyTable`.

First, you would update your `myapps/models.py` to reflect the new schema:

```python
# myapps/models.py (after modification)
from sqlalchemy.orm import declarative_base
from sqlalchemy import Column
from starrocks import INTEGER, VARCHAR

Base = declarative_base()

class MyTable(Base):
    __tablename__ = 'my_table'
    id = Column(INTEGER, primary_key=True)
    name = Column(VARCHAR(50))
    new_column = Column(DATETIME, nullable=True) # Newly added column

    __table_args__ = {
        'starrocks_PRIMARY_KEY': 'id',
        'starrocks_ENGINE': 'OLAP',
        'starrocks_COMMENT': 'A modified table comment', # Modified comment
        'starrocks_PARTITION_BY': """RANGE (id) (
                PARTITION p1 VALUES LESS THAN ('100')
            )""",
        'starrocks_DISTRIBUTED_BY': 'HASH(id) BUCKETS 10',
        'starrocks_PROPERTIES': {
            'storage_medium': 'SSD',
            'replication_num': '1'
        }
    }
```

#### 4.1 Generating a New Migration Script

```bash
alembic revision --autogenerate -m "Add column and modify comment"
```

Alembic will detect both changes and generate a script with `op.add_column` and `op.alter_table`:

```python
# inside versions/<new_revision_id>_...py
def upgrade():
    op.add_column('my_table', sa.Column('new_column', DATETIME(), nullable=True))
    op.alter_table('my_table',
        comment='A modified table comment'
    )

def downgrade():
    op.drop_column('my_table', 'new_column')
    # Downgrade for alter_table (comment) may require manual implementation
    # to revert the comment to its previous state.
```

#### 4.2 Applying This Migration to Your Database

```bash
alembic upgrade head
```

> **Important Note on Schema Changes:**
> StarRocks schema change operations (like `ALTER TABLE ... MODIFY COLUMN`) can be time-consuming. Because one table can have only one ongoing schema change operation at a time, StarRocks does not allow other schema change jobs to be submitted for the same table.
>
> **Recommendation:** For potentially slow `ALTER TABLE` operations, it is recommended to modify only **one column or one table property at a time**. After `autogenerate` creates a migration script, review it. If you see multiple `ALTER` operations for the same table that you suspect might be slow, you should split them into separate migration scripts. (We will try to optimize it in the future.)
>
> For more detailed information on StarRocks table attributes and modification limitations, please refer to the [Tables Usage Guide](docs/usage_guide/tables.md).

#### 4.3 Viewing the Generated SQL (optional)

For users who are more familiar with SQL, it can be helpful to see the exact SQL statements that Alembic will execute before applying a migration. You can do this using the `--sql` flag. This will output the SQL to your console without actually running it against the database.

To see the SQL for all migrations up to the `head`:

```bash
alembic upgrade head --sql
```

#### 4.4 Verifying No Further Changes (optional)

After you have applied all migrations and your database schema is in sync with your models, running the `autogenerate` command again should produce an empty migration script. This is a good way to verify that your schema is up-to-date.

```bash
alembic revision --autogenerate -m "Verify no changes"
```

If there are no differences between your models and the database, Alembic will report that no changes were detected, and an empty revision file will be created.

> Delete it after you have checked.

### 5. Debugging and Logging

To see the raw SQL that the dialect compiles and executes, you can configure logging.

**For Alembic commands:**

Add a logger for `starrocks` in your `alembic.ini` and set the level to `DEBUG`.

```ini
[loggers]
keys = root,sqlalchemy,alembic,starrocks

# ... other loggers

[logger_starrocks]
level = DEBUG
handlers =
qualname = starrocks
```

**For `pytest`:**

Create a `pytest.ini` file in your project root with the following content, if you want to do some simple tests:

```ini
[pytest]
log_cli = true
log_cli_level = DEBUG
log_cli_format = %(levelname)-5.5s [%(name)s] %(message)s
```

## Examples

- Quickstart autogenerate for views: `examples/quickstart_autogen_views.py`
- Additional view/mv usage examples: `examples/view_examples.py`
- View naming and schema isolation examples: `examples/view_naming_examples.py`

## Contributing

### Running Unit Tests

To run tests for the StarRocks SQLAlchemy dialect, first install the package in editable mode along with its testing dependencies:

```bash
pip install -e .
pip install pytest mock
```

Then, you can run the test suite using `pytest`:

```bash
pytest
```

This will run the standard SQLAlchemy dialect test suite as well as StarRocks-specific tests. For more details, please check [SQLAlchemy's guide for dialect development](https://github.com/sqlalchemy/sqlalchemy/blob/main/README.dialects.rst).
