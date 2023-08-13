from typing import Optional

from pydantic import BaseModel


class ConnectorConfig(BaseModel):
    user: str
    password: str
    host: str
    database: str
    port: Optional[int] = 5432
    db_schema: Optional[str] = 'public'
    pool_size: Optional[int] = 1
