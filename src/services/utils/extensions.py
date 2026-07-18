from enum import Enum
class FileExtensions(Enum):
    JSON = ("json", "application/json")
    CSV = ("csv", "text/csv")
    PARQUET = ("parquet", "application/octet-stream")
    
    @property
    def extension(self):
        return self.value[0]
    
    def content_type(self):
        return self.value[1]
    
class JobStatus(Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"

class PipelineStage(Enum):
    BRONZE = "bronze_status"
    SILVER = "silver_status"
    GOLD = "gold_status"
    