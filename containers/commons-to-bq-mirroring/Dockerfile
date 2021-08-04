# The SHA is ugly but we want to maintain behavior with this container versus trusting "latest"
FROM gcr.io/cloud-builders/gsutil@sha256:de96a8abb00651f590eb1449ff20c46ad63b61582627879900b97f99038c0881

ARG APTIBLE_PKG_NAME=aptible-toolbelt_0.16.3%2B20191024181402~debian.8.10-1_amd64.deb
ARG APTIBLE_URL=https://omnibus-aptible-toolbelt.s3.amazonaws.com/aptible/omnibus-aptible-toolbelt/master/194/pkg/${APTIBLE_PKG_NAME}
ARG NODE_URL=https://deb.nodesource.com/setup_12.x

RUN apt update && apt upgrade -y && apt install -y curl postgresql gcc-multilib make cpanminus vim git

RUN curl -O ${APTIBLE_URL} && \
    apt install -y ./${APTIBLE_PKG_NAME}

RUN curl -sL ${NODE_URL} | bash - && \
    apt install -y nodejs

RUN cpanm install Text::CSV_XS Getopt::Long

WORKDIR /commons-to-bq-mirroring
COPY . /commons-to-bq-mirroring

CMD ["/bin/bash", "-c"]
