from flask import Blueprint, jsonify, make_response

from application.blueprints.report.department_employees.extractor import Extractor as DepartmentExtractor
from application.blueprints.report.quarter.extractor import Extractor as QuarterExtractor

reports_blueprint = Blueprint('reports', __name__, url_prefix='/api/v1/reports')


@reports_blueprint.route(rule='/employees-by-quarter', methods=['GET'], strict_slashes=False)
def return_quarter_report():
    try:
        # ETL process
        extractor_result = QuarterExtractor.get_quarterly_information()
        response = make_response(jsonify(extractor_result), 200)

    except Exception as e:
        response = make_response(jsonify({'error': str(e)}), 500)

    return response


@reports_blueprint.route(rule='/most-hired-departments', methods=['GET'], strict_slashes=False)
def return_most_hired_departments_report():
    try:
        # ETL process
        extractor_result = DepartmentExtractor.get_most_hired_departments_information()
        response = make_response(jsonify(extractor_result), 200)

    except Exception as e:
        response = make_response(jsonify({'error': str(e)}), 500)

    return response
