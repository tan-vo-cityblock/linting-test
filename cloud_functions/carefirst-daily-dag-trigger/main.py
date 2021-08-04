import logging
from typing import Dict
import re

from google.auth.transport.requests import Request

import cloud_composer
import decryptor
from google.cloud import storage


DAG_NAME = "load_daily_carefirst_v2"
GCS_BUCKET = "cbh_sftp_drop"
GCS_PREFIX = "carefirst/drop/"
WEEKLY_FILE_PREFIXES = [
    "CBH_CAREFIRST_FACILITYCLAIM_EXTRACT_",
    "CBH_CAREFIRST_PROFESSIONALCLAIM_EXTRACT_",
    "CBH_CAREFIRST_DIAGASSOCIATION_EXTRACT_",
    "CBH_CAREFIRST_PROCASSOCIATION_EXTRACT_",
    "CBH_CAREFIRST_PHARMACYCLAIM_EXTRACT_",
    "CBH_CAREFIRST_LABRESULT_EXTRACT_",
    "CBH_CAREFIRST_PROVIDER_EXTRACT_",
]


def trigger_daily_carefirst_member_ingestion_dag(data: Dict, context=None):
    storage_client = storage.Client()

    def ignore_because(msg):
        logging.info(f"{msg} Ignoring {filepath}")

    filepath: str = data["name"]
    if not filepath.startswith(GCS_PREFIX):
        return ignore_because("Not a CareFirst file.")

  
    if filepath.endswith(".pgp"):
        try:
            decryptor.decrypt_pgp_file(filepath,storage_client,GCS_BUCKET)
        except Exception as e:
            logging.error(e)
        return ignore_because("New file is encrypted .pgp file. The decryptor module will load decrypted version as txt file")
    
    filename = filepath.replace(GCS_PREFIX, "")
    incoming_prefix, incoming_datestr, txt = re.compile(
        "([A-Z_]+)(\d\d\d\d\d\d\d\d)(\.txt)").match(filename).groups()
    if incoming_prefix in WEEKLY_FILE_PREFIXES:
        return ignore_because("New file is weekly file.")

    required_files = [{
        "prefix": "CBH_CAREFIRST_MEMBERDEMO_EXTRACT_",
        "satisfied": False
    }, {
        "prefix": "CBH_CAREFIRST_MEMBERMONTH_EXTRACT_",
        "satisfied": False
    }]

    for requirement in required_files:
        requirement["satisfied"] = (
            incoming_prefix == requirement["prefix"] or
            storage_client.bucket(GCS_BUCKET).blob(
                f"{GCS_PREFIX}{requirement['prefix']}{incoming_datestr}.txt").exists()
        )
        if requirement["satisfied"] is False:
            return ignore_because(f"Missing file required for dag. Need file {GCS_PREFIX}{requirement['prefix']}YYYYMMDD.txt.")

    logging.info(f"Triggering DAG with this date: {incoming_datestr}")
    cloud_composer.prod.dag(DAG_NAME).trigger({
        "conf": {"carefirstMemberDailyDate": incoming_datestr}
    })
