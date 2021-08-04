#!/bin/bash
# Script for pulling a secret version and saving locally.
# See for more: https://cloud.google.com/secret-manager/docs/creating-and-accessing-secrets#access

# Args: secret-id=$1, path-to-file-to-save-secret-data=$2
# Return: saves the secret locally at the filepath passed in.
set -euo pipefail

ROOT=$(git rev-parse --show-toplevel)

# source the $PROJECT variable
source $ROOT/scripts/secret_manager/setup_env.sh

SAVE_DIR=$(cd $(dirname $2) && pwd -P)

if git -C $SAVE_DIR rev-parse 2> /dev/null ; then
    echo Whoops, try not to save secrets to a git directory: \"$SAVE_DIR\".
    echo Re-run with a different filepath that is not a git directory.
else
    echo Pulling secret \"$1\" to save in directory \"$SAVE_DIR\".
    gcloud secrets versions access latest --secret=$1 --project=$PROJECT > $2 \
    && echo Successfully saved \"$1\" secret data to file \"$2\"
fi

