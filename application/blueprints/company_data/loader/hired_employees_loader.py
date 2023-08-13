import numpy as np

from application.blueprints.company_data.loader.abstract_loader import BaseLoader
from application.blueprints.company_data.loader.loader_result import FilteredLoaderResult
from application.blueprints.company_data.loader.supported_loader_config import SupportedLoaderConfig
from application.blueprints.company_data.transformer.transformer_result import TransformerResult
from application.utils.connector.abstract_connector import AbstractConnector


class HiredEmployeesLoader(BaseLoader):

    __GET_CURRENT_DEPARTMENTS_QUERY = """SELECT id AS department_id_ref FROM departments"""
    __GET_CURRENT_JOBS_QUERY = """SELECT id AS job_id_ref FROM JOBS"""

    def __init__(self, connector: AbstractConnector):
        super().__init__(connector)

    def load(
        self,
        transform_result: TransformerResult,
        filtered_result: FilteredLoaderResult,
    ):
        self.store(transform_result, filtered_result)
        super().load(transform_result, filtered_result)

    def store(
        self,
        transform_result: TransformerResult,
        filtered_result: FilteredLoaderResult,
    ):
        if transform_result.hired_employees is not None and not transform_result.hired_employees.empty:
            current_departments = self._connector.get_data_from_db(self.__GET_CURRENT_DEPARTMENTS_QUERY)
            current_jobs = self._connector.get_data_from_db(self.__GET_CURRENT_JOBS_QUERY)

            df_hired_employees = transform_result.hired_employees
            df_hired_employees = df_hired_employees.merge(
                current_departments, left_on='department_id', right_on='department_id_ref'
            )
            df_hired_employees = df_hired_employees.merge(current_jobs, left_on='job_id', right_on='job_id_ref')
            df_hired_employees.drop(['department_id_ref', 'job_id_ref'], axis=1, inplace=True)

            columns_to_insert = list(df_hired_employees.columns.copy())
            columns_to_insert.remove(SupportedLoaderConfig.HIRED_EMPLOYEES.index_col)
            columns_to_update = columns_to_insert.copy()
            columns_to_update.remove('created_at')

            number_of_chunks = int(len(df_hired_employees) / self.MAX_INSERT_BATCH) + 1
            sub_df = np.array_split(df_hired_employees, number_of_chunks)

            for chunk in sub_df:
                chunk = chunk.reset_index()
                chunk = chunk.drop(['index'], axis=1)
                self._connector.bulk_upsert(
                    data_df=chunk,
                    table=SupportedLoaderConfig.HIRED_EMPLOYEES.table,
                    index_col=SupportedLoaderConfig.HIRED_EMPLOYEES.index_col,
                    compare_columns=SupportedLoaderConfig.HIRED_EMPLOYEES.compare_columns,
                    cols_to_insert=columns_to_insert,
                    cols_to_update=columns_to_update,
                )
