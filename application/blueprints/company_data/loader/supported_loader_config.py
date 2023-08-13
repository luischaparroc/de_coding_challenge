from enum import Enum
from typing import List


class SupportedLoaderConfig(Enum):

    DEPARTMENTS = ('departments', 'index_uuid', ['id'])
    HIRED_EMPLOYEES = ('hired_employees', 'index_uuid', ['id'])
    JOBS = ('jobs', 'index_uuid', ['id'])

    def __init__(self, table: str, index_col: str, compare_columns: List[str]):
        self.table = table
        self.index_col = index_col
        self.compare_columns = compare_columns
