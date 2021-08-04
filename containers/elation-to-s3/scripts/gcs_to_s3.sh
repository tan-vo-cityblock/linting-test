#! /bin/bash

# Replace "/path/to/gsutil/" with the path of your gsutil installation.
#PATH="$PATH":/path/to/gsutil/

# Replace "/home/username/" with the path of your home directory in Linux/Mac.
# The ".boto" file contains the settings that helps you connect to Google Cloud Storage.
#export BOTO_CONFIG="/home/username/.boto"

BUCKET=$1
FOLDER=$2
DATE=$3
S3PATH=$4

# A gsutil command to move the data
gsutil -m rsync -r gs://$BUCKET/$FOLDER/$DATE/ s3://$S3PATH
