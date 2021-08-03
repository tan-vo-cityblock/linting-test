import re
import os
from collections import defaultdict
from datetime import datetime

from airflow import macros
from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook

LOCAL_PREPROCESSING_DIR = "/home/airflow/gcs/data/payer_suspect_coding_data/"
EMBLEM_SFTP_SUBDIR = "emblem"
CONNECTICARE_SFTP_SUBDIR = "connecticare_production"


class PreProcessPayerData:
    def __init__(self, latest_date: datetime, bucket_prefixes: [str]):
        self.client = GoogleCloudStorageHook(
            google_cloud_storage_conn_id="process_payer_suspect"
        )
        bucket_name_pattern = r"^gs://(.*?)/"
        file_sub_path_pattern = r"^gs://.*?/(.*$)"
        self.date = latest_date
        self.bucket_prefixes = defaultdict(list)
        for bucket_prefix in bucket_prefixes:
            bucket_name = re.search(bucket_name_pattern, bucket_prefix).group(1)
            file_sub_path = re.search(file_sub_path_pattern, bucket_prefix).group(1)
            self.bucket_prefixes[bucket_name].append(file_sub_path)

    def get_most_recent_relevant_files(self):
        downloaded_file_names = []
        file_name_pattern = r"^.+/(.+\..+)$"
        partner_name_pattern = r"({}|{}).*?/".format(
            EMBLEM_SFTP_SUBDIR, CONNECTICARE_SFTP_SUBDIR
        )
        for bucket_name, file_list in self.__list_most_recent_relevant_files().items():
            for file in file_list:
                file_name_suffix = re.search(file_name_pattern, file).group(1)
                partner_name = re.search(partner_name_pattern, file).group(1)
                local_path_dir = f"{LOCAL_PREPROCESSING_DIR}{partner_name}"
                if not os.path.exists(local_path_dir):
                    os.makedirs(local_path_dir)
                full_local_file_path = f"{local_path_dir}/{file_name_suffix}"
                self.client.download(bucket_name, file, full_local_file_path)
                downloaded_file_names.append(full_local_file_path)
        return downloaded_file_names

    def __list_most_recent_relevant_files(self):
        most_recent_relevant_files = {}
        for bucket_name, file_list in self.__get_all_relevant_files().items():
            files_in_bucket_updated_after_execution = filter(
                lambda file_name: self.client.is_updated_after(
                    bucket_name, file_name, self.date
                ),
                file_list,
            )
            most_recent_relevant_files[
                bucket_name
            ] = files_in_bucket_updated_after_execution
        return most_recent_relevant_files

    def __get_all_relevant_files(self):
        recent_files = defaultdict(list)
        for bucket_name, file_prefix_list in self.bucket_prefixes.items():
            for file_prefix in file_prefix_list:
                recent_files[bucket_name] += self.client.list(
                    bucket=bucket_name, prefix=file_prefix, delimiter=".xlsx"
                )
                recent_files[bucket_name] += self.client.list(
                    bucket=bucket_name, prefix=file_prefix, delimiter=".csv"
                )
                recent_files[bucket_name] += self.client.list(
                    bucket=bucket_name, prefix=file_prefix, delimiter=".txt"
                )
        return recent_files


def main(gcs_bucket_prefixes: [str], **context):
    # if run for the first time, it should only pull the files one day before the last run
    date_to_pull_from = context.get("prev_execution_date_success") or datetime.strptime(
        macros.ds_add(context["ds"], -1), "%Y-%m-%d"
    )
    return PreProcessPayerData(
        date_to_pull_from, gcs_bucket_prefixes
    ).get_most_recent_relevant_files()
