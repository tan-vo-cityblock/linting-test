#!/bin/bash
# Script for listing all secrets.
# See for more: https://cloud.google.com/secret-manager/docs/managing-secrets#listing_secrets

# Args: [optional] secret_id=$1
# Return: prints list of all secrets to terminal.

set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)

# source the $PROJECT variable
source $ROOT/scripts/secret_manager/setup_env.sh

if [[ $# -eq 0 ]] ; then
    gcloud secrets list --project=$PROJECT
else
    gcloud secrets versions list $1 --project=$PROJECT
fi

