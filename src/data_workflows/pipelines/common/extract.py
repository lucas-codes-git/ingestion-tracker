import polars as pl


def extract_events(df: pl.DataFrame) -> pl.DataFrame:
    return (
        df
        .select(
            pl.col("_embedded")
            .struct.field("events")
            .explode()
            .alias("event")
        )
        .unnest("event")
    )