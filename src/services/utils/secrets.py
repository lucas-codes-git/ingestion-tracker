import os
from dotenv import load_dotenv

load_dotenv()

def fetch_secrets() -> dict:
    return {
        "ticketMasterKey": os.getenv("apikey").strip(),
        "supadburl": os.getenv("supadburl").strip(),
        "supakey": os.getenv("supakey").strip(),
        "supaurl": os.getenv("supaurl").strip(),
        "dbname": os.getenv("dbname").strip(),
        "dbpass": os.getenv("dbpass").strip(),
        "dbuser": os.getenv("dbuser").strip(),
        "bucket_name": os.getenv("bucket_name").strip()
    }