#!/bin/bash

# grant access via policy for Compute Instance Admin (contains compute.instances.start) role in
# https://cloud.google.com/compute/docs/access/managing-access-to-resources#bind-member
# data-team Google group should have access to run this script

gcloud compute instances start rstudio-server --project cityblock-rstudio --zone us-east4-a
echo "Instance has started - access at https://rstudio.cityblock.engineering"
