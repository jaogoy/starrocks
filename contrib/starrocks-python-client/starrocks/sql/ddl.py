# starrocks-python-client/starrocks/sql/ddl.py
from sqlalchemy.sql.ddl import DDLElement

# Currently we choose to use __visit_name__ to identify the DDL statement.
# If it's not a good idea, maybe it's not easy to understand, then
# we use the `compiles method to identify the DDL statement.

class CreateView(DDLElement):
    __visit_name__ = "create_view"
    """Represents a CREATE VIEW DDL statement."""
    def __init__(self, element):
        self.element = element

class DropView(DDLElement):
    __visit_name__ = "drop_view"
    """Represents a DROP VIEW DDL statement."""
    def __init__(self, element):
        self.element = element

class CreateMaterializedView(DDLElement):
    __visit_name__ = "create_materialized_view"
    """Represents a CREATE MATERIALIZED VIEW DDL statement."""
    def __init__(self, element):
        self.element = element

class DropMaterializedView(DDLElement):
    __visit_name__ = "drop_materialized_view"
    """Represents a DROP MATERIALIZED VIEW DDL statement."""
    def __init__(self, element):
        self.element = element

