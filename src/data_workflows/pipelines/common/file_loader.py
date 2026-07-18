import polars as pl
from io import BytesIO
def load_file(extension: str, file_bytes: bytes) -> pl.DataFrame:
    
    extension = extension.lower().replace(".", "")
    file = BytesIO(file_bytes)
    
    if extension == "json":
        return pl.read_json(file)
    elif extension == "csv":
        return pl.read_csv(file)
    elif extension == "parquet":
        return pl.read_parquet(file)
    else:
        raise ValueError(f"Unsupported file type: {extension}")
    
    