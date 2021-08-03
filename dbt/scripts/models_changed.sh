#!/bin/sh

children=+  # always run downstream models

if [ ${_BUILD_ID} ]; then  # this env var is always set in Cloud Builds environment and SHOULD not be in local
    models=$(git diff --name-only --no-index $1 $2 | grep '\.sql$' | awk -F '/' '{ print $NF }' | sed "s/\.sql$/${children}/g" | tr '\n' ' ')
else
    git fetch
    git merge origin/master
    models=$(git diff origin/master --name-only | grep '\.sql$' | awk -F '/' '{ print $NF }' | sed "s/\.sql$/${children}/g" | tr '\n' ' ')
fi


if [ -z ${models} ]; then
    echo "No models updated, nothing to run"
else
    echo "Preview of run command: dbt run --models: ${models}"
    if [ $3 ]; then
        echo "Writing models to file: $3"
        echo ${models} > $3
    fi
fi
