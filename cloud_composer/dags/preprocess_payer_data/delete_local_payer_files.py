from collections import defaultdict
import re

from airflow.contrib.hooks.gcs_hook import GoogleCloudStorageHook


class DeleteLocalFiles:
    def __init__(self, bucket_prefixes: [str]):
        self.client = GoogleCloudStorageHook(
            google_cloud_storage_conn_id="process_payer_suspect"
        )
        bucket_name_pattern = r"^gs://(.*?)/"
        file_sub_path_pattern = r"^gs://.*?/(.*$)"
        self.bucket_prefixes = defaultdict(list)
        for bucket_prefix in bucket_prefixes:
            bucket_name = re.search(bucket_name_pattern, bucket_prefix).group(1)
            file_sub_path = re.search(file_sub_path_pattern, bucket_prefix).group(1)
            self.bucket_prefixes[bucket_name].append(file_sub_path)

    def delete_temp_files(self):
        for bucket_name, file_list in self.__get_all_relevant_files().items():
            for file in file_list:
                self.client.delete(bucket_name, file)

    def __get_all_relevant_files(self):
        recent_files = defaultdict(list)
        for bucket_name, file_prefix_list in self.bucket_prefixes.items():
            for file_prefix in file_prefix_list:
                recent_files[bucket_name] += self.client.list(
                    bucket=bucket_name, prefix=file_prefix
                )
        return recent_files


def main(gcs_bucket_prefixes: [str]):
    return DeleteLocalFiles(gcs_bucket_prefixes).delete_temp_files()
