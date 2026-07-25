import polars as pl

SILVER_EVENTS_SCHEMA = {
    "event_id": pl.String,
    "event_name": pl.String,
    "event_type": pl.String,
    "event_url": pl.String,
    "locale": pl.String,

    "venue_id": pl.String,
    "attraction_id": pl.String,
    "promoter_id": pl.String,

    "segment_id": pl.String,
    "genre_id": pl.String,
    "subgenre_id": pl.String,
    "type_id": pl.String,
    "subtype_id": pl.String,

    "local_date": pl.Date,
    "local_time": pl.Time,
    "event_datetime": pl.Datetime(time_zone="UTC"),
    "timezone": pl.String,

    "date_tbd": pl.Boolean,
    "date_tba": pl.Boolean,
    "time_tba": pl.Boolean,
    "no_specific_time": pl.Boolean,

    "status_code": pl.String,
    "span_multiple_days": pl.Boolean,
}