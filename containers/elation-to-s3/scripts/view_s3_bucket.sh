#! /bin/bash

# View the file names currently in Elation bucket, specifically see the time they were uploaded
# Takes param for s3 path

PATH=$1

aws s3 ls s3://$PATH | sort