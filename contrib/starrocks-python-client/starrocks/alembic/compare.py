from alembic.autogenerate import comparators
from .ops import CreateViewOp, DropViewOp, CreateMaterializedViewOp, DropMaterializedViewOp
import re

def normalize_sql(sql_text):
    """A simple normalizer for SQL text."""
    if sql_text is None:
        return None
    # Remove comments, collapse whitespace, and convert to a consistent case.
    sql_text = re.sub(r'--.*?\n', '', sql_text)
    sql_text = re.sub(r'\s+', ' ', sql_text).strip().lower()
    return sql_text

@comparators.dispatch_for("schema")
def autogen_for_views(autogen_context, upgrade_ops, schemas):
    """Compare views between the database and the metadata."""
    for schema in schemas:
        # Get views from database
        db_views = set(autogen_context.dialect.get_view_names(autogen_context.connection, schema))
        db_view_defs = {v: autogen_context.dialect.get_view_definition(v, schema) for v in db_views}

        # Get views from metadata
        metadata_views = autogen_context.metadata.info.get("views", {})
        md_views = {v.name for v, s in metadata_views.items() if s == schema}
        md_view_defs = {v.name: v.definition for v, s in metadata_views.items() if s == schema}

        # Views to create
        for view_name in md_views - db_views:
            view = next(v for v, s in metadata_views.items() if v.name == view_name and s == schema)
            upgrade_ops.ops.append(CreateViewOp(view.name, view.definition, schema=schema))

        # Views to drop
        for view_name in db_views - md_views:
            upgrade_ops.ops.append(DropViewOp(view_name, schema=schema))
            
        # Views to alter (as drop/create)
        for view_name in md_views.intersection(db_views):
            if normalize_sql(md_view_defs[view_name]) != normalize_sql(db_view_defs[view_name]):
                view = next(v for v, s in metadata_views.items() if v.name == view_name and s == schema)
                upgrade_ops.ops.append(DropViewOp(view.name, schema=schema))
                upgrade_ops.ops.append(CreateViewOp(view.name, view.definition, schema=schema))

@comparators.dispatch_for("schema")
def autogen_for_materialized_views(autogen_context, upgrade_ops, schemas):
    """Compare materialized views between the database and the metadata."""
    for schema in schemas:
        db_mvs = set(autogen_context.dialect.get_materialized_view_names(autogen_context.connection, schema))
        db_mv_defs = {v: autogen_context.dialect.get_materialized_view_definition(v, schema) for v in db_mvs}

        metadata_mvs_info = autogen_context.metadata.info.get("materialized_views", {})
        md_mvs = {v.name for v, s in metadata_mvs_info.items() if s == schema}
        md_mv_defs = {v.name: (v.definition, v.properties) for v, s in metadata_mvs_info.items() if s == schema}

        for mv_name in md_mvs - db_mvs:
            mv = next(v for v, s in metadata_mvs_info.items() if v.name == mv_name and s == schema)
            upgrade_ops.ops.append(CreateMaterializedViewOp(mv.name, mv.definition, mv.properties, schema=schema))

        for mv_name in db_mvs - md_mvs:
            upgrade_ops.ops.append(DropMaterializedViewOp(mv_name, schema=schema))

        for mv_name in md_mvs.intersection(db_mvs):
            md_def, md_props = md_mv_defs[mv_name]
            # NOTE: This simple comparison might not be robust enough for all cases,
            # especially for the properties part which might come from SHOW CREATE VIEW.
            # A more sophisticated parser for the DB definition might be needed.
            if normalize_sql(md_def) != normalize_sql(db_mv_defs[mv_name]):
                mv = next(v for v, s in metadata_mvs_info.items() if v.name == mv_name and s == schema)
                upgrade_ops.ops.append(DropMaterializedViewOp(mv.name, schema=schema))
                upgrade_ops.ops.append(CreateMaterializedViewOp(mv.name, mv.definition, mv.properties, schema=schema))
