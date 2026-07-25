import polars as pl
import re

def to_snake_case(value: str) -> str:
    value = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", value)
    value = re.sub(r"[^a-zA-Z0-9]+", "_", value)
    return value.lower().strip("_")

def snake_case_columns(df: pl.DataFrame) -> pl.DataFrame:
    return df.rename(
        {
            col: to_snake_case(col)
            for col in df.columns
        }
    )