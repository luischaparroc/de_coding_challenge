from uuid import uuid4

import pandas as pd

from application.blueprints.company_data.extractor.extractor_result import ExtractorResult
from application.blueprints.company_data.transformer.abstract_transformer import BaseTransformer
from application.blueprints.company_data.transformer.transformer_result import (
    FilteredTransformerResult,
    TransformerResult,
)


class JobsTransformer(BaseTransformer):
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
        if extractor_result.jobs:
            df_jobs = pd.concat(extractor_result.jobs)

            df_valid_jobs = df_jobs[~df_jobs['id'].isnull() & ~df_jobs['job'].isnull()]
            df_valid_jobs['index_uuid'] = [uuid4() for _ in range(len(df_valid_jobs))]
            df_valid_jobs['created_at'] = self._process_dt
            df_valid_jobs['updated_at'] = self._process_dt

            df_filtered_jobs = df_jobs[df_jobs['id'].isnull() | df_jobs['job'].isnull()]
            transform_result.jobs = df_valid_jobs
            filtered_result.jobs = df_filtered_jobs
