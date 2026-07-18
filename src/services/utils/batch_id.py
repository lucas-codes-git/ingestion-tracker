from datetime import date

def build_batch_id(source: str, endpoint: str, file_hash: str, extension: str) -> str:
    _date = date.today()
    return f"{source}_{endpoint}_{_date}_{file_hash}.{extension}"
    