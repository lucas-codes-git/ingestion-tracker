import polars as pl

from src.data_workflows.pipelines.common.transformations import (
    apply_silver_schema,
    snake_case_columns,
    extract_events,
)

from .schema import SILVER_EVENTS_SCHEMA


def silver_events_transformations(data: pl.DataFrame) -> pl.DataFrame:
    
    events = extract_events(data)

    events = snake_case_columns(events)

    events = events.with_columns(
        venue_id=(
            pl.col("embedded")
            .struct.field("venues")
            .list.first()
            .struct.field("id")
        ),

        attraction_id=(
            pl.col("embedded")
            .struct.field("attractions")
            .list.first()
            .struct.field("id")
        ),

        promoter_id=(
            pl.col("promoter")
            .struct.field("id")
        ),

        segment_id=(
            pl.col("classifications")
            .list.first()
            .struct.field("segment")
            .struct.field("id")
        ),

        genre_id=(
            pl.col("classifications")
            .list.first()
            .struct.field("genre")
            .struct.field("id")
        ),

        subgenre_id=(
            pl.col("classifications")
            .list.first()
            .struct.field("subGenre")
            .struct.field("id")
        ),

        type_id=(
            pl.col("classifications")
            .list.first()
            .struct.field("type")
            .struct.field("id")
        ),

        subtype_id=(
            pl.col("classifications")
            .list.first()
            .struct.field("subType")
            .struct.field("id")
        ),
    )


    events = (
        events
        .unnest("dates")
        .unnest("start")
        .unnest("status")
    )
    events = snake_case_columns(events)
    events = events.select(
        [
            pl.col("id").alias("event_id"),
            pl.col("name").alias("event_name"),
            pl.col("type").alias("event_type"),
            pl.col("url").alias("event_url"),
            pl.col("locale"),

            pl.col("venue_id"),
            pl.col("attraction_id"),
            pl.col("promoter_id"),

            pl.col("segment_id"),
            pl.col("genre_id"),
            pl.col("subgenre_id"),
            pl.col("type_id"),
            pl.col("subtype_id"),

            pl.col("local_date"),
            pl.col("local_time"),
            pl.col("date_time").alias("event_datetime"),
            pl.col("timezone"),

            pl.col("date_tbd"),
            pl.col("date_tba"),
            pl.col("time_tba"),
            pl.col("no_specific_time"),

            pl.col("code").alias("status_code"),
            pl.col("span_multiple_days"),
        ]
    )

    events = apply_silver_schema(
        events,
        SILVER_EVENTS_SCHEMA,
    )

    return events