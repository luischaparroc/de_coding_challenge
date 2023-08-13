FROM python:3.10-slim-buster

RUN apt-get update && apt-get install -y gcc libssl-dev libffi-dev libpq-dev

WORKDIR /de-coding-challenge

# set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1

RUN pip install --upgrade pip
COPY requirements.txt .
RUN pip install -r requirements.txt

COPY . .
