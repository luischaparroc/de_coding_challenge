import os
from typing import Dict, List

from application.blueprints.report.quarter import sql_templates
from application.utils.connector.connector_config import ConnectorConfig
from application.utils.connector.supported_connectors import SupportedConnectors


class Extractor:
    @staticmethod
    def get_quarterly_information() -> List[Dict]:
        connector_config = ConnectorConfig(
            user=os.environ.get('DB_USERNAME'),
            password=os.environ.get('DB_PASSWORD'),
            host=os.environ.get('DB_HOST'),
            database=os.environ.get('DB_NAME'),
        )
        connector = SupportedConnectors.POSTGRES.get_connector_instance(connector_config)
        df_quarterly_information = connector.get_data_from_db(
            query=sql_templates.EMPLOYEES_BY_QUARTER_QUERY, params={'year': '2021'}
        )
        quarterly_information = df_quarterly_information.to_dict('records')
        return quarterly_information
