from psycopg import sql
from psycopg.sql import Composed


def replace(target_table: str, staging_table: str, cols: list[str]) -> Composed:
    columns = sql.SQL(", ").join(map(sql.Identifier, cols))

    query = sql.SQL("""
        TRUNCATE TABLE {target};

        INSERT INTO {target} ({columns})
        SELECT {columns}
        FROM {staging};
    """).format(
        target=sql.Identifier(target_table),
        columns=columns,
        staging=sql.Identifier(staging_table),
    )

    return query