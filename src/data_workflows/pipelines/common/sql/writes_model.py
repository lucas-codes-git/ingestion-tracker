from enum import Enum
from .writes.append import append
from .writes.upsert import upsert
from .writes.replace import replace

class WriteMode(Enum):
    APPEND="append"
    UPSERT="upsert"
    REPLACE="replace"
    
class WriteHandlers:
    def __init__(self):
        self.handlers = {
            WriteMode.APPEND: append,
            WriteMode.UPSERT: upsert,
            WriteMode.REPLACE: replace
        }
    
    def get_handler(self, mode: WriteMode):
        return self.handlers[mode]