import os

from application.blueprints.company_data.loader.departments_loader import DepartmentsLoader
from application.blueprints.company_data.loader.hired_employees_loader import HiredEmployeesLoader
from application.blueprints.company_data.loader.jobs_loader import JobsLoader
from application.blueprints.company_data.loader.loader_result import FilteredLoaderResult
from application.blueprints.company_data.transformer.transformer_result import TransformerResult
from application.utils.connector.connector_config import ConnectorConfig
from application.utils.connector.supported_connectors import SupportedConnectors


def load_information(transformer_result: TransformerResult) -> FilteredLoaderResult:
    connector_config = ConnectorConfig(
        user=os.environ.get('DB_USERNAME'),
        password=os.environ.get('DB_PASSWORD'),
        host=os.environ.get('DB_HOST'),
        database=os.environ.get('DB_NAME'),
    )
    connector = SupportedConnectors.POSTGRES.get_connector_instance(connector_config)

    departments_loader = DepartmentsLoader(connector)
    jobs_loader = JobsLoader(connector)
    hired_employees_loader = HiredEmployeesLoader(connector)

    departments_loader.set_next(jobs_loader)
    jobs_loader.set_next(hired_employees_loader)

    filtered_result = FilteredLoaderResult()

    departments_loader.load(transformer_result, filtered_result)

    return filtered_result
