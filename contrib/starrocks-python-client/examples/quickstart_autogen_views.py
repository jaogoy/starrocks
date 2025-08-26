"""Quickstart: autogenerate views with Alembic and StarRocks.

Prerequisites:
- A running StarRocks FE reachable via STARROCKS_URI env (e.g. starrocks://root:@127.0.0.1:9030/test)
- Alembic installed and a temp env.py can be created ad-hoc in tests/examples
"""

from sqlalchemy import create_engine, MetaData
from alembic.runtime.migration import MigrationContext
from alembic.autogenerate import api
from alembic.operations import Operations
from starrocks.sql.schema import View
from starrocks.alembic.starrocks import StarrocksImpl  # ensure impl is imported
import os


def main() -> None:
    uri = os.getenv("STARROCKS_URI", "starrocks://root:@127.0.0.1:9030/test")
    engine = create_engine(uri)

    with engine.connect() as conn:
        # Desired metadata state
        target_metadata = MetaData()
        v = View("quickstart_view", "SELECT 1 AS c")
        target_metadata.info.setdefault("views", {})[(v, None)] = v

        # Autogenerate against current DB
        mc = MigrationContext.configure(connection=conn)
        migration_script = api.produce_migrations(mc, target_metadata)

        # Apply upgrade ops
        ops = Operations(mc)
        for op in migration_script.upgrade_ops.ops:
            ops.invoke(op)

        # Print generated operations (for demo)
        for op in migration_script.upgrade_ops.ops:
            print(f"Generated op: {op.__class__.__name__}")

        # Cleanup
        conn.exec_driver_sql("DROP VIEW IF EXISTS quickstart_view")


if __name__ == "__main__":
    main()


