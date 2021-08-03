FROM gcr.io/cityblock-data/dbt-base:latest

RUN mkdir -p /dbt

# Copying over dbt project
COPY analysis /dbt/analysis
COPY data /dbt/data
COPY macros /dbt/macros
COPY models /dbt/models
COPY snapshots /dbt/snapshots
COPY dbt_project.yml /dbt
COPY packages.yml /dbt

# Copying over profiles (need to ensure dbt runs are pointing to this for this image)
RUN mkdir -p /profiles
COPY cloud_build/prod/profiles.yml /profiles

WORKDIR /dbt


# Environment Variables For Bigquery Project Path
ENV CITYBLOCK_ANALYTICS cityblock-analytics
