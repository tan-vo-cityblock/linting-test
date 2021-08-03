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
    && curl -O https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    tar xzf google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    rm google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz && \
    ln -s /lib /lib64 && \
    gcloud config set core/disable_usage_reporting true && \
    gcloud config set component_manager/disable_update_check true && \
    gcloud config set metrics/environment github_docker_image && \
    gcloud --version

# TODO: abstract above into container `gcp-base-pandas`: https://jira.cityblock.com/browse/PLAT-1192

COPY . /elation-utils
WORKDIR /elation-utils

RUN pip install --upgrade pip
RUN pip install -r requirements.txt
