FROM us.gcr.io/cityblock-data/python-base:latest

WORKDIR /zendesk

COPY ./requirements.txt ./requirements.txt

RUN pip install --upgrade pip
RUN pip install --no-cache-dir -r requirements.txt

COPY . .
