from abc import ABC, abstractmethod
from typing import Any, List

import pandas as pd


class AbstractConnector(ABC):
    @abstractmethod
    def bulk_insert(self, data_df: pd.DataFrame, table: str, index_col: str = None, cols_to_insert: List = None):
        pass

    @abstractmethod
    def bulk_update(
        self, data_df: pd.DataFrame, table: str, index_col: bool, cols_to_update: List = None, coalesce: bool = True
    ):
        pass

    @abstractmethod
    def bulk_upsert(
        self,
        data_df: pd.DataFrame,
        table: str,
        index_col: str,
        compare_columns: List,
        cols_to_update: List = None,
        cols_to_insert: List = None,
        coalesce: bool = True,
    ):
        pass

    @abstractmethod
    def get_data_from_db(self, query: str, params: Any = None):
        pass
