#! /bin/bash

# View the file names currently in Able Health bucket, specifically see the time they were uploaded
aws s3 ls s3://ablehealth-data-transfer/cityblock/import/ | sort