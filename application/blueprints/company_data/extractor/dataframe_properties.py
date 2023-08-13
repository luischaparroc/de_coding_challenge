from enum import Enum
from typing import Any, Dict, List


class DataFrameProperties(Enum):

    DEPARTMENTS = (['id', 'department'], {'id': int, 'department': str})

    JOBS = (['id', 'job'], {'id': int, 'job': str})

    HIRED_EMPLOYEES = (
        ['id', 'name', 'hire_datetime', 'department_id', 'job_id'],
        {'id': int, 'name': str, 'hire_datetime': str, 'department_id': int, 'job_id': int},
    )

    def __init__(self, columns_names: List[str], columns_data_types: Dict[str, Any]):
        self.columns_names = columns_names
        self.columns_data_types = columns_data_types


PROPERTIES_CHOICES = {
    'departments': DataFrameProperties.DEPARTMENTS,
    'jobs': DataFrameProperties.JOBS,
    'hired_employees': DataFrameProperties.HIRED_EMPLOYEES,
}


def get_dataframe_properties(resource_type: str) -> DataFrameProperties:
    properties = PROPERTIES_CHOICES.get(resource_type)
    if not properties:
        raise ValueError(f'Properties for the resource type: {resource_type} was not found')
    return properties
