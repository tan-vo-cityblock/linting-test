FROM us.gcr.io/cityblock-data/python-base:latest

WORKDIR /elation

COPY ./requirements.txt ./requirements.txt

RUN pip install --upgrade pip

RUN apt-get update \
    && apt-get install --no-install-recommends -y g++ \
    && pip install --no-cache-dir -r requirements.txt \
    && apt-get purge --auto-remove -y g++

COPY . .
