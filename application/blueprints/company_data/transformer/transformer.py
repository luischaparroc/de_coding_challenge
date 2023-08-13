from typing import Tuple

import pandas as pd

from application.blueprints.company_data.extractor.extractor_result import ExtractorResult
from application.blueprints.company_data.transformer.departments_transformer import DepartmentsTransformer
from application.blueprints.company_data.transformer.hired_employees_transformer import HiredEmployeesTransformer
from application.blueprints.company_data.transformer.jobs_transformer import JobsTransformer
from application.blueprints.company_data.transformer.transformer_result import (
    FilteredTransformerResult,
    TransformerResult,
)


def transform_information(extractor_result: ExtractorResult) -> Tuple[TransformerResult, FilteredTransformerResult]:
    process_dt = pd.Timestamp.now()

    departments_transformer = DepartmentsTransformer(process_dt)
    jobs_transformer = JobsTransformer(process_dt)
    hired_employees_transformer = HiredEmployeesTransformer(process_dt)

    departments_transformer.set_next(jobs_transformer)
    jobs_transformer.set_next(hired_employees_transformer)

    transformer_result = TransformerResult()
    filtered_result = FilteredTransformerResult()

    departments_transformer.transform(extractor_result, transformer_result, filtered_result)

    return transformer_result, filtered_result
