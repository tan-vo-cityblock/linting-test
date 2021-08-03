FROM python:3.6-slim-buster

COPY . /load_monthly_data
WORKDIR /load_monthly_data

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
RUN mkdir -p /airflow/xcom
