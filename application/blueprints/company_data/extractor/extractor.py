from datetime import datetime
import os
from typing import List
from uuid import uuid4

import pandas as pd
from werkzeug.datastructures.file_storage import FileStorage
from werkzeug.datastructures.structures import ImmutableMultiDict
from werkzeug.utils import secure_filename

from application.blueprints.company_data.extractor.dataframe_properties import get_dataframe_properties
from application.blueprints.company_data.extractor.extractor_result import ExtractorResult
from application.exceptions import InvalidDataException

ALLOWED_CONTENT_TYPE = 'text/csv'
ALLOWED_EXTENSIONS = 'csv'


class Extractor:
    @staticmethod
    def is_csv_file(filename: str) -> bool:
        *_, extension = filename.split('.')
        if extension and extension.lower() == ALLOWED_EXTENSIONS:
            return True
        return False

    def get_content_file(self, key: str, file: FileStorage) -> pd.DataFrame:
        if file.content_type != ALLOWED_CONTENT_TYPE:
            raise InvalidDataException(f'{file.filename} file for the resource type {key} is not a csv file')

        safe_filename = secure_filename(file.filename)

        if not self.is_csv_file(safe_filename):
            raise InvalidDataException(f'Filename extension is not csv: resource type {key}, filename: {file.filename}')

        if not os.path.exists('tmp'):
            os.mkdir('tmp')

        safe_filename = f'tmp/{safe_filename.rstrip(".csv")}_{str(datetime.now())}_{uuid4()}.csv'
        file.save(safe_filename)

        dataframe_properties = get_dataframe_properties(key)
        df = pd.read_csv(
            safe_filename,
            names=dataframe_properties.columns_names,
        )

        if df.empty:
            raise InvalidDataException(f'{file.filename} file for the key {key} is empty')

        return df

    def get_content_files(self, cleaned_file_keys: List[str], total_files: ImmutableMultiDict) -> ExtractorResult:
        extractor_result_dict = {}

        for key in cleaned_file_keys:
            list_files = total_files.getlist(key)
            extractor_result_dict[key] = [self.get_content_file(key, file) for file in list_files]

        extractor_result = ExtractorResult(**extractor_result_dict)
        return extractor_result
