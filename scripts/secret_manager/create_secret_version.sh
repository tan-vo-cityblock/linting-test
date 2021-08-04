#!/bin/bash
# Script for adding a secret version to a secret.
# See for more: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets#add-secret-version

# Args: secret-id=$1, path-to-secret-data=$2
# Return: Adds the secret data to the secret-id.

set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)

# source the $PROJECT variable
source $ROOT/scripts/secret_manager/setup_env.sh

echo Creating secret version for secret \"$1\" with data in file \"$2\".
gcloud secrets versions add $1 --data-file=$2 --project=$PROJECT \
&& echo Success \
&& gcloud secrets versions list $1 --project=$PROJECT

