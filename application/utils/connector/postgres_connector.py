from itertools import chain
from typing import Any, Dict, List
from uuid import uuid4

import numpy as np
import pandas as pd
import sqlalchemy as sql
from psycopg2.sql import SQL, Composed, Identifier, Placeholder

from application.utils.connector.abstract_connector import AbstractConnector
from application.utils.connector.connector_config import ConnectorConfig


class PostgresConnector(AbstractConnector):
    def __init__(self, config: ConnectorConfig):
        conn_str = (
            f'postgresql+psycopg2://{config.user}:{config.password}@{config.host}:{config.port}/{config.database}'
        )
        self.engine = sql.create_engine(
            conn_str,
            connect_args={'options': f'-c search_path={config.db_schema}'},
            pool_size=config.pool_size,
        )

    @staticmethod
    def __set_index_col_value(data_df: pd.DataFrame, index_col: str):
        if index_col not in data_df:
            rows = len(data_df)
            data_df[index_col] = [uuid4() for _ in range(rows)]

        data_df[index_col] = data_df.apply(lambda x: x[index_col] if x[index_col] is not np.nan else uuid4(), axis=1)
        return data_df

    @staticmethod
    def __prepare_df(data_df: pd.DataFrame, index_col: str, cols: list) -> pd.DataFrame:
        if index_col:
            data_df.set_index(index_col, inplace=True)

        if cols and len(cols) > 0:
            data_df = data_df[cols]

        return data_df

    @staticmethod
    def _prepare_df_for_update(data_df: pd.DataFrame, index_col: str, cols_to_update: List) -> pd.DataFrame:
        if cols_to_update is None or len(cols_to_update) == 0:
            return data_df
        elif cols_to_update is not None and len(cols_to_update) > 0:
            cols_to_update.append(index_col)
        data_df = data_df[list(set(cols_to_update))]
        return data_df

    @staticmethod
    def __validate_upsert(data_df: pd.DataFrame, index_col: str, compare_columns: List) -> None:
        if compare_columns is None or len(compare_columns) == 0:
            raise ValueError("Error. compare_columns can not be empty")

        df_cols = set(data_df.columns)
        compare_cols = set(compare_columns)
        intersection = df_cols.intersection(compare_cols)

        if len(intersection) == 0:
            raise ValueError("Error. compare_columns must have at least one value present in dataframe columns")

        if index_col in compare_columns:
            compare_columns.remove(index_col)

    @staticmethod
    def __build_upsert_query(data, table, index_col, compare_columns):
        """
        Builds cte statement to detect objects already present in database.
        """
        check_template = SQL(
            """with {compare} as (
                                {values})
                                select O.{index}, S.*
                                from {table} O inner join S {using}"""
        )
        values_template = SQL("(") + SQL(', ').join([SQL('%s') for _ in range(len(compare_columns))]) + SQL(")")
        columns_sql = SQL("S (") + SQL(', ').join(Identifier(col) for col in compare_columns) + SQL(")")
        values = SQL('values ') + SQL(', \n').join([values_template for _ in range(len(data))])
        using_sql = SQL('using({})').format(SQL(', ').join([Identifier(col) for col in compare_columns]))
        return check_template.format(
            compare=columns_sql, values=values, index=Identifier(index_col), table=Identifier(table), using=using_sql
        )

    @staticmethod
    def __build_update_cte(
        data_df: pd.DataFrame, table: str, index_col: str, data_types_dict: Dict, coalesce: bool = True
    ) -> Composed:
        """
        Builds the cte statement for update
        """
        cte_template = SQL(
            """with {columns} as (
                              {values})
                              update {table} updt {set_columns}
                              from S
                              where updt.{index} = S.{index}"""
        )

        columns_sql = SQL("S (") + SQL(', ').join(Identifier(col) for col in data_df.columns) + SQL(")")
        values_template = SQL("(") + SQL(', ').join([Placeholder() for _ in range(len(data_df.columns))]) + SQL(")")
        values = SQL('values ') + SQL(', \n').join([values_template for _ in range(len(data_df))])

        set_field_template = "{field} = {update_field}"
        cast_field_template = "S.{field}::{data_type}"
        coalesce_template = "coalesce({casted_field}, updt.{field})"

        list_set_fields = []
        df_columns = list(data_df.columns)
        update_columns = list(filter(lambda x: x != index_col, df_columns))
        for update_column in update_columns:
            data_type = data_types_dict.get(update_column)
            casted_field = cast_field_template.format(field=update_column, data_type=data_type)
            coalesce_field = coalesce_template.format(casted_field=casted_field, field=update_column)
            update_field = coalesce_field if coalesce else casted_field
            set_field = set_field_template.format(field=update_column, update_field=update_field)
            list_set_fields.append(SQL(set_field))

        set_fields = SQL('set ') + SQL(', ').join(list_set_fields)

        return cte_template.format(
            columns=columns_sql,
            values=values,
            table=Identifier(table),
            set_columns=set_fields,
            index=Identifier(index_col),
        )

    @staticmethod
    def __build_data_types_query(data_df: pd.DataFrame, table: str) -> Composed:
        select_template = SQL(
            """select {columns}
                                 from {table}
                                 limit 1"""
        )
        column_template = SQL('pg_typeof({column}) as {column}')
        columns = list(data_df.columns)
        columns_typeof_list = [column_template.format(column=Identifier(column)) for column in columns]
        columns_sql = SQL(', ').join(columns_typeof_list)
        return select_template.format(columns=columns_sql, table=Identifier(table))

    def bulk_insert(self, data_df: pd.DataFrame, table: str, index_col: str = None, cols_to_insert: List = None):
        if data_df.empty:
            return

        index = False if index_col is None else True

        if index:
            data_df = self.__set_index_col_value(data_df, index_col)

        data_df = self.__prepare_df(data_df, index_col, cols_to_insert)

        data_df.to_sql(name=table, con=self.engine, method='multi', if_exists='append', index=index)

    def bulk_update(
        self, data_df: pd.DataFrame, table: str, index_col: str, cols_to_update: List = None, coalesce: bool = True
    ):
        if data_df.empty:
            return

        if index_col is None:
            raise ValueError('Error. Index col must be defined for update operation')

        data_df = self._prepare_df_for_update(data_df, index_col, cols_to_update)

        data_types_query = self.__build_data_types_query(data_df, table)

        with self.engine.begin() as conn:
            data_types_df = pd.read_sql(data_types_query.as_string(self.engine.raw_connection().cursor()), con=conn)

        data_types_dict, *_ = data_types_df.to_dict('records')
        update_query = self.__build_update_cte(data_df, table, index_col, data_types_dict, coalesce=coalesce)

        with self.engine.begin() as conn:
            conn.execute(
                update_query.as_string(self.engine.raw_connection().cursor()), list(chain.from_iterable(data_df.values))
            )

    def prepare_bulk_upsert(
        self, data_df: pd.DataFrame, table: str, index_col: str, compare_columns: List, update_columns: List
    ):
        upsert_select = self.__build_upsert_query(data_df[compare_columns], table, index_col, compare_columns)
        query_params = list(chain.from_iterable(data_df[compare_columns].values))
        updated_query_params = [int(value) if isinstance(value, np.int64) else value for value in query_params]

        with self.engine.begin() as _:
            existing_data_ids = pd.read_sql(
                upsert_select.as_string(self.engine.raw_connection().cursor()),
                self.engine,
                params=tuple(updated_query_params),
            )

        update_data = data_df.merge(existing_data_ids, on=compare_columns, suffixes=('_delme', ''))
        selected_cols = list(set([index_col] + update_columns))
        update_data = update_data[selected_cols]
        index_col_suffix = "_index_col_suffix"
        index_col_to_remove = index_col + index_col_suffix
        insert_data = data_df.merge(existing_data_ids, how='outer', on=compare_columns, suffixes=(index_col_suffix, ''))
        insert_data = insert_data[insert_data[index_col].isna()]
        insert_data_columns = set(insert_data.columns)
        if index_col_to_remove in insert_data_columns:
            insert_data = insert_data.rename(columns={index_col_to_remove: index_col, index_col: index_col_to_remove})

        insert_data = insert_data.groupby(compare_columns).first().reset_index()

        return update_data, insert_data

    def bulk_upsert(
        self,
        data_df: pd.DataFrame,
        table: str,
        index_col: str,
        compare_columns: List,
        cols_to_update: List = None,
        cols_to_insert: List = None,
        coalesce: bool = True,
    ) -> None:
        cols_to_update = cols_to_update if cols_to_update is not None else []
        cols_to_insert = cols_to_insert if cols_to_insert is not None else []

        if index_col is None:
            raise ValueError('Error. Index col must be defined for update operation')

        self.__validate_upsert(data_df, index_col, compare_columns)
        data_update, data_insert = self.prepare_bulk_upsert(data_df, table, index_col, compare_columns, cols_to_update)
        self.bulk_insert(data_insert, table, index_col, cols_to_insert)
        self.bulk_update(data_update, table, index_col, cols_to_update, coalesce=coalesce)

    def get_data_from_db(self, query: str, params: Any = None) -> pd.DataFrame:
        with self.engine.connect() as conn:
            df = pd.read_sql(sql=query, params=params, con=conn)
        return df
