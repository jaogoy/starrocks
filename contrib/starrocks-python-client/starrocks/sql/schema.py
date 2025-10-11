from sqlalchemy.schema import MetaData, SchemaItem
from sqlalchemy.sql.elements import quoted_name
from typing import Any, Optional, List

from starrocks.types import MVRefreshMoment, MVRefreshType


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
    """Represents a Materialized View object in Python.
    Complex syntex check will be put in compiler instead of here.
    """
    def __init__(self, name: str,
                 definition: str,
                 metadata: Optional[MetaData] = None,
                 schema: Optional[str] = None,
                 comment: Optional[str] = None,
                 partition_by: Optional[str] = None,
                 distributed_by: Optional[str] = None,
                 order_by: Optional[str] = None,
                 refresh_moment: Optional[str] = None,
                 refresh_type: Optional[str] = None,
                 properties: Optional[dict] = None,
                 **kwargs: Any) -> None:

        self.name = quoted_name(name, kwargs.get('quote'))
        self.definition = definition  # The SELECT statement
        self.schema = schema
        self.comment = comment
        self.partition_by = partition_by
        self.distributed_by = distributed_by
        self.order_by = order_by

        self.refresh_moment = refresh_moment
        if refresh_moment is not None and refresh_moment.upper() not in MVRefreshMoment.ALLOWED_ITEMS:
            raise ValueError(f"refresh_moment must be one of {MVRefreshMoment.ALLOWED_ITEMS}")

        self.refresh_type = refresh_type

        self.properties = properties or {}
        self.__visit_name__ = "materialized_view"

        if metadata is not None:
            self.dispatch.before_parent_attach(self, metadata)
            if self.schema is None:
                self.schema = metadata.schema
            key = (self.schema, self.name)
            metadata.info.setdefault("materialized_views", {})[key] = self
            self.dispatch.after_parent_attach(self, metadata)

