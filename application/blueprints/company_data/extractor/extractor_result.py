from typing import List, Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict


class ExtractorResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    departments: Optional[List[pd.DataFrame]] = []
    jobs: Optional[List[pd.DataFrame]] = []
    hired_employees: Optional[List[pd.DataFrame]] = []
