from enum import Enum

from application.utils.connector.abstract_connector import AbstractConnector
from application.utils.connector.postgres_connector import PostgresConnector


class SupportedConnectors(Enum):

    POSTGRES = 'POSTGRES'

    def __init__(self, connector_type: str):
        self.connector_type = connector_type
        self.builders = {'POSTGRES': PostgresConnector}

    def get_connector_instance(self, config) -> AbstractConnector:
        connector_instance = self.builders.get(self.connector_type)(config)
        return connector_instance
