FROM python:3.7.9-slim

# update/upgrade available packages in image
# install python-dev which has header files for Python C API
RUN apt-get update -y && \
    apt-get upgrade -y && \
    apt-get install --no-install-recommends -y \
    python-dev

WORKDIR /python-base

COPY ./requirements.txt ./requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

