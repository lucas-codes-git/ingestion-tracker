from src.services.clients.supabase import is_clean

def build_supabase_file_path(bucket_name: str, source_name: str, raw_clean: str = "raw" | None) -> str:
    return f"{bucket_name}"