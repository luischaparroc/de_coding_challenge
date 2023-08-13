from datetime import datetime
from uuid import uuid4

import pandas as pd

from application.blueprints.company_data.extractor.extractor_result import ExtractorResult
from application.blueprints.company_data.transformer.abstract_transformer import BaseTransformer
from application.blueprints.company_data.transformer.transformer_result import (
    FilteredTransformerResult,
    TransformerResult,
)
from application.exceptions import InvalidDataException


class HiredEmployeesTransformer(BaseTransformer):

    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

    def __init__(self, process_dt: pd.Timestamp):
        super().__init__(process_dt)

    def transform(
        self,
        extractor_result: ExtractorResult,
        transform_result: TransformerResult,
        filtered_result: FilteredTransformerResult,
    ):
        self.process_data(extractor_result, transform_result, filtered_result)
        super().transform(extractor_result, transform_result, filtered_result)

    def process_data(
        self,
        extractor_result: ExtractorResult,
        transform_result: TransformerResult,
        filtered_result: FilteredTransformerResult,
    ):
        if extractor_result.hired_employees:
            df_hired_employees = pd.concat(extractor_result.hired_employees)

            df_valid_hired_employees = df_hired_employees[
                ~df_hired_employees['id'].isnull()
                & ~df_hired_employees['name'].isnull()
                & ~df_hired_employees['hire_datetime'].isnull()
                & ~df_hired_employees['department_id'].isnull()
                & ~df_hired_employees['job_id'].isnull()
            ]
            df_valid_hired_employees['index_uuid'] = [uuid4() for _ in range(len(df_valid_hired_employees))]
            df_valid_hired_employees['created_at'] = self._process_dt
            df_valid_hired_employees['updated_at'] = self._process_dt

            try:
                df_valid_hired_employees['hire_datetime'] = df_valid_hired_employees['hire_datetime'].apply(
                    lambda x: datetime.strptime(x, self.__DATETIME_FORMAT)
                )
            except Exception:
                raise InvalidDataException(
                    f'Invalid datetime format for hire_employees information. Valid format {self.__DATETIME_FORMAT}'
                )

            df_filtered_hired_employees = df_hired_employees[
                df_hired_employees['id'].isnull()
                | df_hired_employees['name'].isnull()
                | df_hired_employees['hire_datetime'].isnull()
                | df_hired_employees['department_id'].isnull()
                | df_hired_employees['job_id'].isnull()
            ]
            transform_result.hired_employees = df_valid_hired_employees
            filtered_result.hired_employees = df_filtered_hired_employees
