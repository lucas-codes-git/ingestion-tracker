from psycopg import sql
from psycopg.sql import Composed

def upsert(
    target_table: str,
    staging_table: str,
    cols: list[str],
    primary_key: str,
) -> Composed:

    columns = sql.SQL(", ").join(map(sql.Identifier, cols))

    update_cols = [
        col for col in cols if col != primary_key
    ]

    updates = sql.SQL(", ").join(
        sql.SQL("{col} = EXCLUDED.{col}").format(
            col=sql.Identifier(col)
        )
        for col in update_cols
    )

    query = sql.SQL("""
        INSERT INTO {target} ({columns})
        SELECT {columns}
        FROM {staging}
        ON CONFLICT ({pk})
        DO UPDATE SET {updates};
    """).format(
        target=sql.Identifier(target_table),
        columns=columns,
        staging=sql.Identifier(staging_table),
        pk=sql.Identifier(primary_key),
        updates=updates,
    )

    return query