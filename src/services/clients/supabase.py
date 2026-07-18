from supabase import create_client
from src.services.utils import fetch_secrets

class SupaBaseClient:
    def __init__(self):
        self.secrets = fetch_secrets()
        self.client = create_client(self.secrets["supaurl"], self.secrets["supakey"])
    
    def is_clean(self, clean_raw: bool) -> str:
        return "clean" if clean_raw else "raw"

    def build_folder_path(self, source_name: str, data_name: str, file_name: str, clean_raw: bool) -> str:
        folder_path = f"{source_name}/{data_name}/{self.is_clean(clean_raw)}/{file_name}"
        return folder_path
    
    def list_files(self, bucket_name: str, source_name: str, data_name: str, clean_raw: bool) -> list[str]:
        folder_path = f"{source_name}/{data_name}/{self.is_clean(clean_raw)}"
        files = self.client.storage.from_(bucket_name).list(path=folder_path)
        return files
    
    def upload_file(self, bucket_name: str, source_name: str, data_name: str, clean_raw: bool, file_name: str, file_bytes: bytes, file_type: str = "application/json"):
        path = self.build_folder_path(
            source_name,
            data_name,
            file_name,
            clean_raw
        )
        self.client.storage.from_(bucket_name).upload(
            path=path,
            file=file_bytes,
            file_options={"content-type": file_type}
        )
        
    def download_file(self, bucket_name: str, source_name: str, data_name: str, clean_raw: bool, file_name: str) -> bytes:
        path = self.build_folder_path(
            source_name,
            data_name,
            file_name,
            clean_raw
        )
        
        file_bytes = self.client.storage.from_(bucket_name).download(path)
        return file_bytes
        
        