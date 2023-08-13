from abc import ABC, abstractmethod

import pandas as pd

from application.blueprints.company_data.extractor.extractor_result import ExtractorResult
from application.blueprints.company_data.transformer.transformer_result import (
    FilteredTransformerResult,
    TransformerResult,
)


class AbstractTransformer(ABC):
    @abstractmethod
    def transform(
        self,
        extractor_result: ExtractorResult,
        transform_result: TransformerResult,
        filtered_result: FilteredTransformerResult,
    ):
        pass

    @abstractmethod
    def set_next(self, handler):
        pass


class BaseTransformer(AbstractTransformer):

    _next_transformer: AbstractTransformer = None

    def __init__(self, process_dt: pd.Timestamp):
        self._process_dt = process_dt

    def set_next(self, transformer: AbstractTransformer) -> AbstractTransformer:
        self._next_transformer = transformer
        return transformer

    def transform(
        self,
        extractor_result: ExtractorResult,
        transform_result: TransformerResult,
        filtered_result: FilteredTransformerResult,
    ):
        if self._next_transformer:
            self._next_transformer.transform(extractor_result, transform_result, filtered_result)
