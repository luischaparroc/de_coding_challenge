from flask import Blueprint, jsonify, make_response, request

from application.blueprints.company_data.extractor.extractor import Extractor
from application.blueprints.company_data.loader.loader import load_information
from application.blueprints.company_data.transformer.transformer import transform_information
from application.exceptions import InvalidDataException

company_data_blueprint = Blueprint('company_data', __name__, url_prefix='/api/v1/company_data')

ALLOWED_FILE_KEYS = ['departments', 'hired_employees', 'jobs']


@company_data_blueprint.route(rule='/', methods=['POST'], strict_slashes=False)
def receive_company_data():
    try:
        total_files = request.files
        file_keys = total_files.keys()
        cleaned_file_keys = list(filter(lambda x: x in ALLOWED_FILE_KEYS, file_keys))

        # ETL process
        extractor_result = Extractor().get_content_files(cleaned_file_keys, total_files)
        transformer_result, filtered_result = transform_information(extractor_result)
        load_information(transformer_result)

        response = make_response(jsonify({'success': 'Success Request'}), 200)

    except InvalidDataException as e:
        response = make_response(jsonify({'error': str(e)}), 400)

    except Exception as e:
        response = make_response(jsonify({'error': str(e)}), 500)

    return response
