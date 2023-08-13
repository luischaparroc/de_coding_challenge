from datetime import datetime
from typing import List

from sqlalchemy.dialects.postgresql import UUID

from extensions import db


class Department(db.Model):

    __tablename__ = 'departments'

    id = db.Column(db.Integer(), primary_key=True)
    department = db.Column(db.String(), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    index_uuid = db.Column(UUID(as_uuid=True), nullable=False)
    hired_employees = db.relationship('HiredEmployee', backref='departments', lazy=True)

    def __init__(self, id_department: int, department: str) -> None:
        self.id = id_department
        self.department = department

    def save(self):
        db.session.add(self)
        db.session.commit()


class Job(db.Model):

    __tablename__ = 'jobs'

    id = db.Column(db.Integer(), primary_key=True)
    job = db.Column(db.String(), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    index_uuid = db.Column(UUID(as_uuid=True), nullable=False)
    hired_employees = db.relationship('HiredEmployee', backref='jobs', lazy=True)

    def __init__(self, id_job: int, job: str) -> None:
        self.id = id_job
        self.job = job

    def save(self):
        db.session.add(self)
        db.session.commit()


class HiredEmployee(db.Model):

    __tablename__ = 'hired_employees'

    id = db.Column(db.Integer(), primary_key=True)
    name = db.Column(db.String(), nullable=False)
    hire_datetime = db.Column(db.String(), nullable=False)
    department_id = db.Column(db.Integer(), db.ForeignKey('departments.id'), nullable=False)
    job_id = db.Column(db.Integer(), db.ForeignKey('jobs.id'), nullable=False)
    created_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, nullable=False, default=datetime.utcnow, onupdate=datetime.utcnow)
    index_uuid = db.Column(UUID(as_uuid=True), nullable=False)

    def __init__(self, distance: float, message: List[str]) -> None:
        self.distance = distance
        self.message = message

    def save(self):
        db.session.add(self)
        db.session.commit()
