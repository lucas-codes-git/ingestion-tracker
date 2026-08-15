from psycopg import sql
from psycopg.sql import Composed

def append(target_table: str, staging_table: str, cols: list[str], primary_key: str) -> Composed:
    query = sql.SQL("""
        INSERT INTO {target} ({columns})
        SELECT {columns}
        FROM {staging}
        ON CONFLICT ({pk}) DO NOTHING;
    """).format(
        target = sql.Identifier(target_table),
        columns = sql.SQL(", ").join(map(sql.Identifier, cols)),
        staging = sql.Identifier(staging_table),
        pk = sql.Identifier(primary_key)
    )
    
    return query