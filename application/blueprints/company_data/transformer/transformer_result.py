from typing import Optional

import pandas as pd
from pydantic import BaseModel, ConfigDict


class TransformerResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    departments: Optional[pd.DataFrame] = None
    jobs: Optional[pd.DataFrame] = None
    hired_employees: Optional[pd.DataFrame] = None


class FilteredTransformerResult(BaseModel):
    model_config = ConfigDict(arbitrary_types_allowed=True)
    departments: Optional[pd.DataFrame] = None
    jobs: Optional[pd.DataFrame] = None
    hired_employees: Optional[pd.DataFrame] = None
