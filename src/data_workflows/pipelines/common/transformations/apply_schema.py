import polars as pl
import logging

logger = logging.getLogger(__name__)

def apply_silver_schema(data: pl.DataFrame, config: dict) -> pl.DataFrame:
    for col_name, data_type in config.items():
        if col_name not in data.columns:
            logger.warning(f"Column '%s' not found in dataframe.", col_name)
            continue
            
        data = data.with_columns(
            pl.col(col_name).cast(data_type, strict=False)
        )
        
        
    return data