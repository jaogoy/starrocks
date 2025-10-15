import os
from typing import Optional
import pytest
from sqlalchemy import create_engine
from sqlalchemy.engine import Engine

from starrocks.alembic.compare import logger


def get_starrocks_url() -> Optional[str]:
    dsn = os.getenv("STARROCKS_URL")
    if not dsn:
        logger.warning("environment variable STARROCKS_URL is not set")
        return None
    return dsn


def create_test_engine() -> Engine:
    url = get_starrocks_url()
    if not url:
        pytest.skip("STARROCKS URL is not set; skipping integration tests")
    engine = create_engine(url, pool_pre_ping=True)
    # Lightweight connectivity check to ensure credentials/database are valid
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("select 1")
    except Exception as exc:
        pytest.skip(f"Unable to connect to STARROCKS_URL; skipping tests: {exc}")
    return engine


@pytest.fixture(scope="session")
def sr_root_engine() -> Engine:
    """A session-scoped engine that connects to the default 'test' database."""
    eng = create_test_engine()
    try:
        yield eng
    finally:
        eng.dispose()


# Default for local runs; override via environment
os.environ.setdefault("STARROCKS_URL", "starrocks://myname:pswd1234@127.0.0.1:9030/test")
test_default_schema = "test"
