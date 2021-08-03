#!/bin/bash
set -o errexit

usage() {
  cat <<EOM
  Usage:
  $(basename $0) -e <env> -p <database> -b <bucket> -d <date>
  -e    Environment of Commons database to mirror (either "staging" or "production").
  -p    Name of the Postgres database we are planning to export.
  -b    Name of the GCS bucket we are writing sending the export to.
  -i    GCP project id that we are running mirroring in.
  -d    Date suffix to attach to exported files (defaults to date with YYYYmmdd format).
EOM
  exit 0
}

DATE_SUFFIX=$(date +'%Y%m%d')
CONNECTION_FILE="connection_params.txt"

# kill any remaining background processes
function cleanup() {
  kill $(jobs -p)
  rm ${CONNECTION_FILE}
  # "aptible db:tunnel" escapes the "jobs -p" kill, so we have to hunt it down.
  pkill -f 'ssh [^ ]*\.aptible.\in '
}
trap cleanup EXIT

while getopts "e:p:b:i:d:" OPTION; do
  case ${OPTION} in
  e) COMMONS_ENV=${OPTARG};;
  p) COMMONS_DATABASE=${OPTARG};;
  b) GCS_BUCKET_NAME=${OPTARG};;
  i) GCP_MIRROR_PROJECT_ID=${OPTARG};;
  d) DATE_SUFFIX=${OPTARG};;
  *) usage;;
  esac
done

if [[ -z "${COMMONS_ENV}" ]] || [[ -z "${COMMONS_DATABASE}" ]] || [[ -z "${GCS_BUCKET_NAME}" ]] || [[ -z "${GCP_MIRROR_PROJECT_ID}" ]];
  then usage
fi

echo "Running Commons mirror for [env: ${COMMONS_ENV}, database: ${COMMONS_DATABASE}, bucket: ${GCS_BUCKET_NAME}, project: ${GCP_MIRROR_PROJECT_ID}, date: ${DATE_SUFFIX}]"

# auth into aptible with creds supplied to the script's environment (most likely through KMS + Airflow)
aptible login --email="${APTIBLE_USER}" --password="${APTIBLE_PASSWORD}"

echo "Logged in through Aptible for [user: ${APTIBLE_USER}]"

DB_BACKUP_HANDLE="${COMMONS_DATABASE}-mirror-job-${DATE_SUFFIX}"

# deprovision backup with the same handle if it exists
aptible db:deprovision ${DB_BACKUP_HANDLE} || true

# create and restore a backup of the Commons DB we are mirroring
aptible db:backup ${COMMONS_DATABASE}
LATEST_BACKUP_ID=$(aptible backup:list ${COMMONS_DATABASE} | head -1 | awk '{print $1}' | sed 's/.$//')
aptible backup:restore "${LATEST_BACKUP_ID}" --handle ${DB_BACKUP_HANDLE} --container-size 15360

echo "Creating and restoring Commons DB backup [handle: ${DB_BACKUP_HANDLE}]"

# tunnel into the restored backup and parse connection parameters
aptible db:tunnel ${DB_BACKUP_HANDLE} 2>&1 | tee ${CONNECTION_FILE} &

for i in $(seq 1 20); do
  sleep 1;
  if [[ -e ${CONNECTION_FILE} ]]; then
    if grep 'Password: ' ${CONNECTION_FILE}; then
      HOST=$(perl -ne '/\* Host: (.*)/ and print "$1"' ${CONNECTION_FILE})
      PORT=$(perl -ne '/\* Port: (.*)/ and print "$1"' ${CONNECTION_FILE})
      PASSWORD=$(perl -ne '/\* Password: (.*)/ and print "$1"' ${CONNECTION_FILE})
      USER=$(perl -ne '/\* Username: (.*)/ and print "$1"' ${CONNECTION_FILE})
      DBNAME=$(perl -ne '/\* Database: (.*)/ and print "$1"' ${CONNECTION_FILE})
      break;
    fi
  fi
done

if [[ -z "${PASSWORD}" ]]; then
  echo "Unable to get connection details when attempting to db:tunnel [handle: ${DB_BACKUP_HANDLE}]"
  exit 1
fi

echo "Created and extracted Postgres connection elements from aptible db:tunnel"

# wait until the backup is officially ready to accept connections
RESTORATION_WAIT_INTERVAL=60
NUM_TRIES=60
for i in $(seq 1 ${NUM_TRIES}); do
  sleep ${RESTORATION_WAIT_INTERVAL};
  if pg_isready --dbname=${DBNAME} --host=${HOST} --port=${PORT} --username=${USER} | grep "accepting connections"; then
    break;
  fi

  echo "Waited $(( i*${RESTORATION_WAIT_INTERVAL} )) seconds for aptible backup to restore [handle: ${DB_BACKUP_HANDLE}]"
  if [[ $i -eq ${NUM_TRIES} ]]; then
    echo "Unable to start database backup after $(( i*${RESTORATION_WAIT_INTERVAL} )) seconds [handle: ${DB_BACKUP_HANDLE}]"
    exit 1
  fi
done

# run the actual exports
PSQL_CONNECTION_STRING="host=${HOST} port=${PORT} dbname=${DBNAME} user=${USER} password=${PASSWORD}"

# list out all table names
RELEVANT_COMMONS_TABLES_QUERY="(SELECT tablename FROM pg_tables WHERE tablename NOT LIKE ALL(ARRAY['pg_%', 'sql_%', 'knex%', 'google_auth', 'note_draft', 'claim_encounter_cache', 'patient_event']))"
TABLES_FILE="/commons-to-bq-mirroring/table_names.txt"
psql "${PSQL_CONNECTION_STRING}" -c "\COPY ${RELEVANT_COMMONS_TABLES_QUERY} TO '${TABLES_FILE}' CSV"

echo "Ran PSQL query to dump relevant tables names to [file: ${TABLES_FILE}]"

gcloud auth activate-service-account --key-file=${GOOGLE_APPLICATION_CREDENTIALS}

echo "Finished gcloud authentication to copy exported schemas and data"

# export all the things!
cat ${TABLES_FILE} | while read line
do
  # queries
  SCHEMA_QUERY="(SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE (table_name = '${line}' AND table_schema = 'public') ORDER BY ORDINAL_POSITION)"
  SCHEMA_FILE="${COMMONS_ENV}_export_${line}_schema_${DATE_SUFFIX}"
  DATA_FILE="${COMMONS_ENV}_export_${line}_data_${DATE_SUFFIX}"

  # copy schema file for table to export folder (quietly to not create excessive logging output)
  psql --quiet "${PSQL_CONNECTION_STRING}" -c "\COPY ${SCHEMA_QUERY} TO STDOUT CSV" | \
    gsutil cp - gs://${GCS_BUCKET_NAME}/${SCHEMA_FILE}

  echo "Exported schema successfully [table: ${line}, bucket: ${GCS_BUCKET_NAME}]"

  # copy data for table to temp file in export folder (quietly to not create excessive logging output)
  psql --quiet "${PSQL_CONNECTION_STRING}" -c "\COPY \"${line}\" TO STDOUT CSV" | \
    perl ./clean_psql_csv_export.pl | \
    gsutil cp - gs://${GCS_BUCKET_NAME}/${DATA_FILE}

  echo "Cleaned and exported data successfully [table: ${line}, bucket: ${GCS_BUCKET_NAME}]"

done

echo "Ran PSQL queries to export and copied exports to GCS [bucket: gs://${GCS_BUCKET_NAME}]"

# export builder tables to GCS to hook into the rest of mirroring
if [[ ${COMMONS_ENV} == "prod" ]]; then
  BUILDER_EXPORT_COMMAND="npm run jobs:export-builder-tables:production"
else
  BUILDER_EXPORT_COMMAND="npm run jobs:export-builder-tables:${COMMONS_ENV}"
fi

GCP_MIRROR_TABLE_NAMES_FILE="exported_builder_table_names"
aptible ssh --app=${COMMONS_DATABASE} ${BUILDER_EXPORT_COMMAND}

gsutil cp gs://${GCS_BUCKET_NAME}/${GCP_MIRROR_TABLE_NAMES_FILE} .
cat ${GCP_MIRROR_TABLE_NAMES_FILE} >> ${TABLES_FILE}

echo "Exported builder tables to GCS [bucket: gs://${GCS_BUCKET_NAME}]"

# write out table names to xcom file (do some funky formatting to get the xcom string to work)
AIRFLOW_XCOM_DIR="/airflow/xcom"
AIRFLOW_XCOM_FILE="${AIRFLOW_XCOM_DIR}/return.json"
mkdir -p ${AIRFLOW_XCOM_DIR}
cat ${TABLES_FILE} | sed ':a;N;$!ba;s/\n/,/g' | awk '{print "\x22" $1 "\x22"}' > ${AIRFLOW_XCOM_FILE}
chmod +x ${AIRFLOW_XCOM_DIR}

echo "Wrote table names to xcom directory for next pod [file: ${AIRFLOW_XCOM_FILE}]"

# deprovision the backup database
aptible db:deprovision ${DB_BACKUP_HANDLE}

echo "Deprovisioned restored aptible backup [handle: ${DB_BACKUP_HANDLE}]"
