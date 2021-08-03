# The SHA is ugly but we want to maintain behavior with this container versus trusting "latest"
# SHA is tied to release on Mar 9th 2020
FROM gcr.io/cloud-builders/gcloud-slim@sha256:10b8e0a4cb8afced35f72aa52b49dd546113b553410e62850bbf4995200f2195

ENV APTIBLE_PKG_NAME=aptible-toolbelt_0.16.3%2B20191024181014~ubuntu.16.04-1_amd64.deb
ENV APTIBLE_URL=https://omnibus-aptible-toolbelt.s3.amazonaws.com/aptible/omnibus-aptible-toolbelt/master/194/pkg/${APTIBLE_PKG_NAME}

RUN apt update && apt upgrade -y && apt install -y curl

RUN curl -O ${APTIBLE_URL} && \
    apt install -y ./${APTIBLE_PKG_NAME}

WORKDIR commons-aptible-ssh
COPY . ./

ENTRYPOINT ["/bin/bash", "commons-aptible-ssh.sh"]
