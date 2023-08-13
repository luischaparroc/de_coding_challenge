from abc import ABC, abstractmethod

from application.blueprints.company_data.loader.loader_result import FilteredLoaderResult
from application.blueprints.company_data.transformer.transformer_result import TransformerResult
from application.utils.connector.abstract_connector import AbstractConnector


class AbstractLoader(ABC):
    @abstractmethod
    def load(
        self,
        transform_result: TransformerResult,
        filtered_result: FilteredLoaderResult,
    ):
        pass

    @abstractmethod
    def set_next(self, handler):
        pass


class BaseLoader(AbstractLoader):

    MAX_INSERT_BATCH = 1000

    _next_loader: AbstractLoader = None

    def __init__(self, connector: AbstractConnector):
        self._connector = connector

    def set_next(self, loader: AbstractLoader) -> AbstractLoader:
        self._next_loader = loader
        return loader

    def load(
        self,
        transform_result: TransformerResult,
        filtered_result: FilteredLoaderResult,
    ):
        if self._next_loader:
            self._next_loader.load(transform_result, filtered_result)
