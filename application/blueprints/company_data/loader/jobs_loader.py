import numpy as np

from application.blueprints.company_data.loader.abstract_loader import BaseLoader
from application.blueprints.company_data.loader.loader_result import FilteredLoaderResult
from application.blueprints.company_data.loader.supported_loader_config import SupportedLoaderConfig
from application.blueprints.company_data.transformer.transformer_result import TransformerResult
from application.utils.connector.abstract_connector import AbstractConnector


class JobsLoader(BaseLoader):
    def __init__(self, connector: AbstractConnector):
        super().__init__(connector)

    def load(
        self,
        transform_result: TransformerResult,
        filtered_result: FilteredLoaderResult,
    ):
        self.store(transform_result)
        super().load(transform_result, filtered_result)

    def store(self, transform_result: TransformerResult):
        if transform_result.jobs is not None and not transform_result.jobs.empty:
            columns_to_insert = list(transform_result.jobs.columns.copy())
            columns_to_insert.remove(SupportedLoaderConfig.JOBS.index_col)
            columns_to_update = columns_to_insert.copy()
            columns_to_update.remove('created_at')

            number_of_chunks = int(len(transform_result.jobs) / self.MAX_INSERT_BATCH) + 1
            sub_df = np.array_split(transform_result.jobs, number_of_chunks)

            for chunk in sub_df:
                chunk = chunk.reset_index()
                chunk = chunk.drop(['index'], axis=1)
                self._connector.bulk_upsert(
                    data_df=chunk,
                    table=SupportedLoaderConfig.JOBS.table,
                    index_col=SupportedLoaderConfig.JOBS.index_col,
                    compare_columns=SupportedLoaderConfig.JOBS.compare_columns,
                    cols_to_insert=columns_to_insert,
                    cols_to_update=columns_to_update,
                )
