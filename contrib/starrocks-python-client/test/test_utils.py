import re

def _normalize_sql(sql: str) -> str:
    """Normalizes a SQL string for comparison."""
    # Remove comments
    sql = re.sub(r'--.*\n', '', sql)
    # Replace newlines and tabs with spaces
    sql = sql.replace('\n', ' ').replace('\t', '')
    # Collapse multiple spaces into one
    sql = re.sub(r'\s+', ' ', sql)
    # Remove spaces around parentheses, commas, and equals for consistency
    sql = re.sub(r'\s*\(\s*', '(', sql)
    sql = re.sub(r'\s*\)\s*', ')', sql)
    sql = re.sub(r'\s*,\s*', ',', sql)
    sql = re.sub(r'\s*=\s*', '=', sql)
    return sql.strip()
