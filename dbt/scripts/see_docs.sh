#!/bin/bash


if [[ -z $(pip list --disable-pip-version-check | grep -F dbt) ]]; then
  echo "dbt Python dependency not available! pip install dbt or change to Python env containing it."
  exit 1
fi

echo "Removing target contents should they exist..."
rm -rf ../target
mkdir -p ../target
echo "Synchronizing dbt docs files from Cloud Storage to your computer..."
gsutil -m rsync -r gs://cbh-analytics-artifacts/dbt_docs ../target
export CITYBLOCK_ANALYTICS=cityblock-analytics
echo "Serving up dbt docs on your browser - LEAVE THIS TERMINAL RUNNING AND CTRL + C TO EXIT (closes dbt docs)"
dbt docs serve --profiles-dir $(cd .. && pwd)/cloud_build/prod
