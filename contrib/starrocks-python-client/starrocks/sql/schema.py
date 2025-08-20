# starrocks-python-client/starrocks/sql/schema.py
from sqlalchemy.schema import SchemaItem
from sqlalchemy.sql.elements import quoted_name

class View(SchemaItem):
    """Represents a View object in Python."""
    def __init__(self, name, definition, schema=None, **kwargs):
        self.name = quoted_name(name, kwargs.get('quote'))
        self.definition = definition # The SELECT statement as a string
        self.schema = schema
        # This allows the compiler to find the right compilation function
        self.__visit_name__ = "view"

class MaterializedView(SchemaItem):
    """Represents a Materialized View object in Python."""
    def __init__(self, name, definition, properties=None, schema=None, **kwargs):
        self.name = quoted_name(name, kwargs.get('quote'))
        self.definition = definition # The SELECT statement
        self.properties = properties or {} # e.g., {"refresh_type": "ASYNC"}
        self.schema = schema
        self.__visit_name__ = "materialized_view"

