import os

from flask import Flask, jsonify, make_response

from application.blueprints.company_data.endpoints import company_data_blueprint
from application.models import Department, HiredEmployee, Job  # noqa
from extensions import db, migrate

app = Flask(__name__)
app.register_blueprint(company_data_blueprint)
app.config['FLASK_ENV'] = os.environ.get('FLASK_ENV')
app.config['SQLALCHEMY_DATABASE_URI'] = os.environ.get('DATABASE_URL')
app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
db.init_app(app)
migrate.init_app(app, db)


@app.errorhandler(404)
def not_found(error):
    """Returns JSON response with 404 status"""
    return make_response(jsonify({"error": "Not found"}), 404)


if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
