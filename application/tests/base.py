import os
import unittest

from app import app
from extensions import db

from application.models import (
    Department,
    HiredEmployee,
    Job
)


class BaseTestClass(unittest.TestCase):

    def setUp(self) -> None:
        app.config['TESTING'] = True
        app.config['DEBUG'] = False
        app.config['WTF_CSRF_ENABLED'] = False

        self.app = app.test_client()

    def tearDown(self) -> None:
        pass
