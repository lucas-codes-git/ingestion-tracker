from psycopg import sql
from psycopg.sql import Composed

def create_table(table_name: str, cols_dtype: dict[str, str]) -> Composed:
    columns = sql.SQL(",\n").join(
        sql.SQL("{} {}").format(
            sql.Identifier(col),
            sql.SQL(dtype)
        )
        for col, dtype in cols_dtype.items()
    )

    query = sql.SQL("""
        CREATE TABLE IF NOT EXISTS {table} (
            {columns}
        );
    """).format(
        table=sql.Identifier(table_name),
        columns=columns,
    )

    return query