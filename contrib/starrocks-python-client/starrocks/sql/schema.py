from sqlalchemy.schema import MetaData, SchemaItem
from sqlalchemy.sql.elements import quoted_name
from typing import Any, Optional, List


class View(SchemaItem):
    """Represents a View object in Python."""
    def __init__(self, name: str, definition: str, metadata: Optional[MetaData] = None, schema: Optional[str] = None,
                 comment: Optional[str] = None, columns: Optional[List[str]] = None,
                 security: Optional[str] = None, **kwargs: Any) -> None:
        self.name = quoted_name(name, kwargs.get('quote'))
        self.definition = definition
        self.schema = schema
        self.comment = comment
        self.columns = columns
        self.security = security

        if metadata is not None:
            self.dispatch.before_parent_attach(self, metadata)
            if self.schema is None:
                self.schema = metadata.schema
            key = (self.schema, self.name)
            metadata.info.setdefault("views", {})[key] = self
            self.dispatch.after_parent_attach(self, metadata)


class MaterializedView(SchemaItem):
    """Represents a Materialized View object in Python."""
    def __init__(self, name: str, definition: str, properties: Optional[dict] = None,
                 metadata: Optional[MetaData] = None, schema: Optional[str] = None, **kwargs: Any) -> None:
        self.name = quoted_name(name, kwargs.get('quote'))
        self.definition = definition  # The SELECT statement
        self.properties = properties or {}  # e.g., {"refresh_type": "ASYNC"}
        self.schema = schema
        self.__visit_name__ = "materialized_view"

        if metadata is not None:
            self.dispatch.before_parent_attach(self, metadata)
            if self.schema is None:
                self.schema = metadata.schema
            key = (self.schema, self.name)
            metadata.info.setdefault("materialized_views", {})[key] = self
            self.dispatch.after_parent_attach(self, metadata)

