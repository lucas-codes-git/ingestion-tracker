import polars as pl
from src.data_workflows.pipelines.common.transformations import apply_silver_schema, snake_case_columns, extract_events

def silver_venues_transformations(data: pl.DataFrame) -> pl.DataFrame:
    
    df = snake_case_columns(data)
    venues = (
        extract_events(df)
        .select(
            pl.col("_embedded")
            .struct.field("venues")
            .explode()
            .alias("venue")
        )
        .unnest("venue")
    )
    
    venues = (
    venues
    .unnest("city")
    .unnest("country")
    .unnest("state")
    .unnest("address")
    .unnest("location")
)