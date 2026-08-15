import polars as pl
from io import BytesIO
from src.services.utils.extensions import FileExtensions


def load_file(file_bytes: bytes, file_type: FileExtensions) -> pl.DataFrame:

    file = BytesIO(file_bytes)

    if file_type == FileExtensions.JSON:
        df = pl.read_json(file)

    elif file_type == FileExtensions.CSV:
        df = pl.read_csv(
            file,
            infer_schema_length=0
        )

    elif file_type == FileExtensions.PARQUET:
        df = pl.read_parquet(file)

    else:
        raise ValueError(f"Unsupported file type: {file_type}")

    return df