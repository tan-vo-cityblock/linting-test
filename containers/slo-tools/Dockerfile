FROM python:alpine3.6

COPY ./requirements.txt /requirements.txt

WORKDIR /

RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install -r requirements.txt

COPY . /
