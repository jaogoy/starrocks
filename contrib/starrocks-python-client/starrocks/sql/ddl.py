# starrocks-python-client/starrocks/sql/ddl.py
from sqlalchemy.sql.ddl import DDLElement
from .schema import View, MaterializedView

# Currently we choose to use __visit_name__ to identify the DDL statement.
# If it's not a good idea, maybe it's not easy to understand, then
# we use the `compiles method to identify the DDL statement.

class CreateView(DDLElement):
    """Represents a CREATE VIEW DDL statement."""
    __visit_name__ = "create_view"
    def __init__(self, element: View, or_replace: bool = False, if_not_exists: bool = False) -> None:
        self.element = element
        self.or_replace = or_replace
        self.if_not_exists = if_not_exists
        self.security = element.security

class AlterView(DDLElement):
    """Represents an ALTER VIEW DDL statement."""
    __visit_name__ = "alter_view"
    def __init__(self, element: View) -> None:
        self.element = element

class DropView(DDLElement):
    """Represents a DROP VIEW DDL statement."""
    __visit_name__ = "drop_view"
    def __init__(self, element: View) -> None:
        self.element = element

class CreateMaterializedView(DDLElement):
    """Represents a CREATE MATERIALIZED VIEW DDL statement."""
    __visit_name__ = "create_materialized_view"
    def __init__(self, element: MaterializedView) -> None:
        self.element = element

class DropMaterializedView(DDLElement):
    """Represents a DROP MATERIALIZED VIEW DDL statement."""
    __visit_name__ = "drop_materialized_view"
    def __init__(self, element: MaterializedView) -> None:
        self.element = element

