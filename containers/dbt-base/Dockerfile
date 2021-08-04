FROM python:3.7.9-slim

# Install git
RUN apt-get update && \
    apt-get upgrade -y && \
    apt-get install -y git

# Install DBT
RUN pip install --upgrade pip
RUN pip install dbt==0.19.1
