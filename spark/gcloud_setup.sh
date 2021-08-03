#!/bin/bash

set -euo pipefail

source ./util.sh
install_util_verify
source .env

info "Beginning gcloud setup..."

info "Entering $CBH_SPARK_CONFIG_DIR"
cd $CBH_SPARK_CONFIG_DIR

if which gcloud &> /dev/null; then
    warn "gcloud already installed"
elif [[ ! -d ~/google-cloud-sdk ]]; then
    info "Install google-cloud-sdk"
    curl https://sdk.cloud.google.com > install.sh
    bash install.sh --disable-prompts
fi

info "Ensuring that google-cloud-sdk is up-to-date"
bash -c "yes | gcloud components update"
    
if [[ $(gcloud config configurations list | grep -c $EMAIL'[ ]\+'$PROJECT_ID) -lt 1 ]]; then
    info "Creating new gcloud configuration for user $EMAIL and project $PROJECT_ID"
    gcloud config configurations create personal --activate
    gcloud config set account $EMAIL
    gcloud config set project $PROJECT_ID # TODO do this after project is authorized
elif [[ -z $(gcloud config get-value account 2>&1 | grep $EMAIL) \
     || -z $(gcloud config get-value project 2>&1 | grep $PROJECT_ID) ]]; then
    config=$(gcloud config configurations list | grep $EMAIL'[ ]\+'$PROJECT_ID | head -n1 | cut -d' ' -f1)
    info "Switching to configuration '$config', which uses user $EMAIL and project $PROJECT_ID"
    gcloud config configurations activate $config
else
    warn "Active configuration already uses user $EMAIL and project $PROJECT_ID"
fi

if [[ -n $(gcloud config get-value compute/zone 2>&1 | grep '(unset)') ]]; then
    info "Setting default compute region/zone to us-east-1c"
    gcloud config set compute/zone us-east-1c
else
    warn "Default compute region/zone already configured"
fi

if [[ -z $(gcloud auth list 2>&1 | grep '\* \+'$EMAIL) ]]; then
    info "Logging into GCP as user $EMAIL"
    gcloud auth login $EMAIL
else
    warn "Already logged into GCP as $EMAIL"
fi
   
if [[ ! -f ~/.config/gcloud/application_default_credentials.json ]]; then
    info "Setting $EMAIL credentials as application-default"
    gcloud auth application-default login
else
    warn "application-default credentials already set"
fi

if [[ -z $(gcloud iam service-accounts list 2>&1 | grep $CBH_SPARK_SVC_ACCT_EMAIL) ]]; then
    info "Creating spark user service account $CBH_SPARK_SVC_ACCT_EMAIL in $PROJECT_ID"
    gcloud iam service-accounts create spark-user \
           --display-name "Cityblock Spark user service account" \
           --description "Authenticates developer machine spark instances with GCP services"
else
    warn "Service account $CBH_SPARK_SVC_ACCT_EMAIL already exists"
fi

if ! gsutil ls gs://$CBH_SPARK_TEMP_BUCKET &> /dev/null; then
    info "Creating spark temp bucket $CBH_SPARK_TEMP_BUCKET"
    gsutil mb gs://$CBH_SPARK_TEMP_BUCKET
else
    warn "Spark temp bucket $CBH_SPARK_TEMP_BUCKET already exists"
fi

if ! bq ls spark_test &> /dev/null; then
    info "Creating spark_test dataset"
    bq mk spark_test
else
    warn "spark_test dataset in project $PROJECT_ID already exists"
fi

if [[ ! -f $CBH_SPARK_SVC_ACCT_KEYFILE ]]; then
    info "Generating key for $CBH_SPARK_SVC_ACCT_KEYFILE"
    gcloud iam service-accounts keys create $CBH_SPARK_SVC_ACCT_KEYFILE \
           --iam-account=$CBH_SPARK_SVC_ACCT_EMAIL
else
    warn "Keyfile already exists for $CBH_SPARK_SVC_ACCT_EMAIL"
fi

# Permissions

info "Making $CBH_SPARK_SVC_ACCT_EMAIL an owner of $CBH_SPARK_TEMP_BUCKET"
gsutil acl ch -u $CBH_SPARK_SVC_ACCT_EMAIL:OWNER gs://$CBH_SPARK_TEMP_BUCKET

info "Granting bigquery.dataEditor in $PROJECT_ID to $CBH_SPARK_SVC_ACCT_EMAIL"
gcloud projects add-iam-policy-binding $PROJECT_ID \
       --member="serviceAccount:$CBH_SPARK_SVC_ACCT_EMAIL" \
       --role=roles/bigquery.dataEditor
info "Granting bigquery.readSessionUser in $PROJECT_ID to $CBH_SPARK_SVC_ACCT_EMAIL"
gcloud projects add-iam-policy-binding $PROJECT_ID \
       --member "serviceAccount:$CBH_SPARK_SVC_ACCT_EMAIL" \
       --role=roles/bigquery.readSessionUser
info "Granting bigquery.jobUser in $PROJECT_ID to $CBH_SPARK_SVC_ACCT_EMAIL"
gcloud projects add-iam-policy-binding $PROJECT_ID \
       --member "serviceAccount:$CBH_SPARK_SVC_ACCT_EMAIL" \
       --role=roles/bigquery.jobUser
info "Granting storage.admin in $PROJECT_ID to $CBH_SPARK_SVC_ACCT_EMAIL"
gcloud projects add-iam-policy-binding $PROJECT_ID \
       --member "serviceAccount:$CBH_SPARK_SVC_ACCT_EMAIL" \
       --role=roles/storage.admin

info "gcloud setup complete!\n"

function print_shell_init_message {
    if ! $1 -c "which gcloud" &> /dev/null; then
        info "Run the following to auto-initialize google-cloud-sdk in your shell"
        cat <<END
cat <<EOF >> $2
source ~/google-cloud-sdk/path.$1.inc
source ~/google-cloud-sdk/completion.$1.inc
EOF
source $2
END
    else
        warn "google-cloud-sdk is already initialized in your shell!"
    fi
}

if [[ -n `shell_name` ]]; then
    print_shell_init_message `shell_name` `shell_config`
else
    warn "Your shell is not bash, zsh, or fish. Defaulting to google-cloud-sdk's"
    warn "bash initialization scripts."
    print_shell_init_message bash .bash_profile
fi

