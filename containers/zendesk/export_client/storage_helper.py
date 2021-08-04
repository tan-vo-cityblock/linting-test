import logging
from typing import List, Optional

from google.cloud.storage import Client

logger = logging.getLogger(__name__)


class StorageHelper:
    # TODO Make DRY with code in
    # 'mixer/containers/cloud-sql-to-bq-mirroring/gcp_helper.py'
    """Helper class to abstract out commons functions for up/downloading to GCS."""

    def __init__(self, project: str):
        self.client = Client(project)
        self.project = project

    def upload_json_for_bq(
        self, bucket_name: str, gcs_path: str, json_strs: List[str]
    ) -> Optional[str]:

        if not json_strs:
            return None

        bucket = self.client.bucket(bucket_name)
        final_gcs_path = f"{gcs_path}.json"

        blob = bucket.blob(final_gcs_path)

        # json must be one, new-line delimited string to be uploaded to bq
        json_str_nld = "\n".join(json_strs) + "\n"

        blob.upload_from_string(json_str_nld, content_type="application/json")

        full_gcs_uri = f"gs://{bucket_name}/{final_gcs_path}"
        logger.info(f"Uploaded json object {blob.name} to {full_gcs_uri}.")

        return full_gcs_uri

    def compose_json_objects(
        self, bucket_name: str, gcs_path_chunk_prefix: str
    ) -> Optional[str]:
        bucket = self.client.bucket(bucket_name)
        all_blob_chunks = list(
            self.client.list_blobs(bucket, prefix=gcs_path_chunk_prefix)
        )

        if not all_blob_chunks:
            return None

        final_gcs_path = f"{gcs_path_chunk_prefix}.json"

        # Check that composite object, which will have blob.name = final_gcs_path,
        # doesn't already exist.
        for blob in all_blob_chunks:
            if blob.name == final_gcs_path:
                raise ValueError(
                    f'Composite object at "{final_gcs_path}" already exists.'
                )

        composed_blob = bucket.blob(final_gcs_path)
        composed_blob.compose(all_blob_chunks)
        logger.info(f"Composed final blob at {composed_blob.name}.")

        for blob in all_blob_chunks:
            blob.delete()
            logger.info(f"Deleted component blob at {blob.name} after composing")

        full_gcs_uri = f"gs://{bucket_name}/{final_gcs_path}"
        return full_gcs_uri
