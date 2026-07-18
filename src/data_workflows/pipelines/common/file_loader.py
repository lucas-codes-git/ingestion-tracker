import polars as pl
from io import BytesIO
from src.services.utils.extensions import FileExtensions

def load_file(file_type: FileExtensions.value, file_bytes: bytes) -> pl.DataFrame:
    
    file = BytesIO(file_bytes)
    
    if file_type == FileExtensions.JSON:
        df =  pl.read_json(file)
    elif file_type == FileExtensions.CSV:
        df =  pl.read_csv(file, infer_schema_length=0)
    elif file_type == FileExtensions.PARQUET:
        df =  pl.read_parquet(file)
    else:
        raise ValueError(f"Unsupported file type: {file_type}")
    
    for col in df.columns:
        df = df.with_columns(
            pl.col(col).cast(pl.String)
        )
        
    return df
    
    