FROM nickgryg/alpine-pandas:3.6.8
ARG CLOUD_SDK_VERSION=237.0.0
ENV CLOUD_SDK_VERSION=$CLOUD_SDK_VERSION
ENV PATH /google-cloud-sdk/bin:$PATH

RUN apk update

RUN apk --no-cache add \
        python3-dev \
        curl \
        py-crcmod \
        openssh-client \
        libffi-dev \
        openssl-dev \
        git \
        gnupg \
        make \
    && curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    tar xzf google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    rm google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    ln -s /lib /lib64 && \
    gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true && \
    gcloud config set metrics/environment github_docker_image && \
    gcloud --version


WORKDIR /cloud-sql-to-bq-mirroring
COPY . /cloud-sql-to-bq-mirroring

RUN pip install --upgrade pip
RUN pip install -r requirements.txt

RUN wget https://dl.google.com/cloudsql/cloud_sql_proxy.linux.amd64 -O cloud_sql_proxy
RUN chmod +x cloud_sql_proxy
RUN mkdir /cloudsql
RUN mkdir -p /airflow/xcom
