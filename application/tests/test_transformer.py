from datetime import datetime

import pandas as pd

from application.blueprints.company_data.extractor.dataframe_properties import get_dataframe_properties
from application.blueprints.company_data.extractor.extractor_result import ExtractorResult
from application.blueprints.company_data.transformer.transformer import transform_information
from application.tests.base import BaseTestClass


class TestCompanyDataTransformer(BaseTestClass):

    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

    def setUp(self) -> None:
        super().setUp()

        dataframe_properties = get_dataframe_properties('departments')
        self.departments = pd.read_csv(
            'resources/departments.csv',
            names=dataframe_properties.columns_names,
        )

        dataframe_properties = get_dataframe_properties('jobs')
        self.jobs = pd.read_csv(
            'resources/jobs.csv',
            names=dataframe_properties.columns_names,
        )

        dataframe_properties = get_dataframe_properties('hired_employees')
        self.hired_employees = pd.read_csv(
            'resources/hired_employees.csv',
            names=dataframe_properties.columns_names,
        )

    def test_transformer(self):
        extractor_result = ExtractorResult(
            departments=[self.departments],
            jobs=[self.jobs],
            hired_employees=[self.hired_employees],
        )

        transformer_result, filtered_result = transform_information(extractor_result)

        df_valid_hired_employees = self.hired_employees[~(self.hired_employees['id'] == 2)]
        df_filtered_hired_employees = self.hired_employees[self.hired_employees['id'] == 2]

        df_valid_hired_employees['hire_datetime'] = df_valid_hired_employees['hire_datetime'].apply(
            lambda x: datetime.strptime(x, self.__DATETIME_FORMAT)
        )

        dataframe_properties = get_dataframe_properties('departments')
        df_output_departments = transformer_result.departments[dataframe_properties.columns_names]

        dataframe_properties = get_dataframe_properties('jobs')
        df_output_jobs = transformer_result.jobs[dataframe_properties.columns_names]

        dataframe_properties = get_dataframe_properties('hired_employees')
        df_output_hired_employees = transformer_result.hired_employees[dataframe_properties.columns_names]
        df_output_filtered_hired_employees = filtered_result.hired_employees[dataframe_properties.columns_names]

        assert self.departments.compare(df_output_departments).empty
        assert self.jobs.compare(df_output_jobs).empty
        assert df_output_hired_employees.compare(df_valid_hired_employees).empty
        assert df_output_filtered_hired_employees.compare(df_filtered_hired_employees).empty
