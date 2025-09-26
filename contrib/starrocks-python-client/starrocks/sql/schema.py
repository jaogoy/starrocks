from sqlalchemy.schema import SchemaItem
from sqlalchemy.sql.elements import quoted_name
from typing import Any, Optional, List


class View(SchemaItem):
    """Represents a View object in Python."""
    def __init__(self, name: str, definition: str, schema: Optional[str] = None,
                 comment: Optional[str] = None, columns: Optional[List[str]] = None,
                 security: Optional[str] = None, **kwargs: Any) -> None:
        self.name = quoted_name(name, kwargs.get('quote'))
        self.definition = definition
        self.schema = schema
        self.comment = comment
        self.columns = columns
        self.security = security


class MaterializedView(SchemaItem):
    """Represents a Materialized View object in Python."""
    def __init__(self, name: str, definition: str, properties: Optional[dict] = None, schema: Optional[str] = None, **kwargs: Any) -> None:
        self.name = quoted_name(name, kwargs.get('quote'))
        self.definition = definition  # The SELECT statement
        self.properties = properties or {}  # e.g., {"refresh_type": "ASYNC"}
        self.schema = schema
        self.__visit_name__ = "materialized_view"

