from pydantic import BaseModel
from typing import Any

class DatabaseConfig(BaseModel):
    service: str
    config: Any