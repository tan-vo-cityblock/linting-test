import json
import logging
from abc import ABC, abstractmethod
from datetime import date, datetime
from typing import List, Optional

from export_client.bq_helper import BQHelper, ZendeskExportMetadata
from export_client.storage_helper import StorageHelper
from google.cloud.bigquery import SchemaField
from util import get_kb_size, rgetattr
from zenpy import Zenpy

logger = logging.getLogger(__name__)


class GeneratorHelper:
    def __init__(self, method: str, cursor: bool = False, time: bool = False):
        self.method = method
        self.cursor = cursor
        self.time = time

    def __str__(self):
        return str(vars(self))


class BaseExporter(ABC):
    """Base Class for holding common logic used in export of Zendesk models.
    Subclasses that run incremental vs bulk exports will inherit this parent.
    """

    # roughly speaking, a 1000KB (1MB) size list of zendesk python objects equates to approx.
    # 25MB sized new line delimted json formatted string that is uploaded to GCS.
    MAX_CHUNK_SIZE_KB = 1000

    def __init__(
        self,
        project_id: str,
        dataset_name: str,
        bucket_name: str,
        zendesk_secret_path: str,
    ):
        self.project_id = project_id
        self.bucket_name = bucket_name
        self.dataset_name = dataset_name

        self.bq_helper = BQHelper(self.project_id)
        self.storage_helper = StorageHelper(self.project_id)

        with open(zendesk_secret_path, "r") as secret_file:
            zendesk_secret_dict = json.load(secret_file)

        # proactive_ratelimit=0 is the floor for the X-Rate-Limit-Remaining value in Zendesk response header
        # Once X-Rate-Limit-Remaining decrements to 0 (from 700 (10 for incremental) requests in 1 min),
        # we pause for proactive_ratelimit_request_interval=60 secs.
        self.zenpy_client = Zenpy(
            **zendesk_secret_dict,
            proactive_ratelimit=0,
            proactive_ratelimit_request_interval=60,
        )

        self.export_metadata_table = self.bq_helper.get_or_create_table(
            project_id=self.project_id,
            dataset_id=dataset_name,
            table_id=ZendeskExportMetadata.METADATA_TABLE_NAME,
            schema=ZendeskExportMetadata.METADATA_SCHEMA,
        )

    @staticmethod
    def _format_datetime_for_gcs_path(dt: datetime) -> str:
        return dt.strftime("%Y%m%d%H%M%S")

    @staticmethod
    def _clean_models_arg(models_arg: List[str], all_models: List[str]) -> List[str]:
        deduped_models_arg = list(set(models_arg))

        if not all(model in all_models for model in deduped_models_arg):
            raise ValueError(
                f"Models passed in as args {deduped_models_arg} must be a subset of:\n{all_models}"
            )
        return deduped_models_arg

    def _get_model_generator(
        self,
        generator_helper: GeneratorHelper,
        start_cursor: Optional[str] = None,
        start_time: Optional[datetime] = None,
    ):

        logger.info(f"Getting model generator with config: {generator_helper}")

        model_generator_method = rgetattr(self.zenpy_client, generator_helper.method)

        if generator_helper.cursor and generator_helper.time:
            return model_generator_method(
                start_time=start_time, cursor=start_cursor, paginate_by_time=False
            )
        elif generator_helper.cursor and start_cursor:
            return model_generator_method(cursor=start_cursor)
        elif generator_helper.time and start_time:
            return model_generator_method(start_time=start_time)
        else:
            return model_generator_method()

    def _mk_gcs_path(
        self,
        table_name: str,
        inserted_at: datetime,
        chunk_postfix: Optional[int] = None,
    ) -> str:
        inserted_at_path = self._format_datetime_for_gcs_path(inserted_at)
        gcs_path = f"{table_name}/{inserted_at_path}/{table_name}"
        if chunk_postfix is not None:
            gcs_path = f"{gcs_path}_{chunk_postfix}"
        return gcs_path

    def _write_to_gcs(
        self,
        table_name: str,
        inserted_at: datetime,
        json_strs: List[str],
        chunk_postfix: Optional[int] = None,
    ) -> str:

        gcs_path = self._mk_gcs_path(
            table_name=table_name, chunk_postfix=chunk_postfix, inserted_at=inserted_at
        )

        return self.storage_helper.upload_json_for_bq(
            bucket_name=self.bucket_name, gcs_path=gcs_path, json_strs=json_strs
        )

    # TODO: make params more compact by abstracting into dict or data class
    def _write_to_bq(
        self,
        table_name: str,
        gcs_uri: str,
        allow_overwrite: bool = False,
        bq_partition: Optional[date] = None,
        bq_latest_view_query: Optional[str] = None,
        schema: Optional[List[SchemaField]] = None,
    ):
        self.bq_helper.load_json_from_gcs(
            gcs_uri=gcs_uri,
            dataset_name=self.dataset_name,
            table_name=table_name,
            allow_schema_updates=True,
            allow_overwrite=allow_overwrite,
            bq_partition=bq_partition,
            schema=schema,
        )
        logger.info(f"Uploaded model '{table_name}' json from GCS to BQ")

        if bq_partition or bq_latest_view_query:
            self.bq_helper.update_view_for_latest(
                dataset_name=self.dataset_name,
                table_name=table_name,
                bq_partition=bq_partition,
                custom_view_query=bq_latest_view_query,
            )
            logger.info(f"Updated latest view for model '{table_name}'.")

    def _persist_metadata(self, metadata: ZendeskExportMetadata):
        metadata_row = [metadata.to_json_dict()]

        bq_errors = self.bq_helper.client.insert_rows_json(
            table=self.export_metadata_table, json_rows=metadata_row
        )
        if len(bq_errors) > 0:
            for error in bq_errors:
                logger.error(f"{error}\n")
            raise RuntimeError(
                "FAILURE: Error(s) occured while streaming export metadata to BQ. See logs above."
            )

    def _persist_model(
        self,
        model_generator,
        model_name: str,
        bq_inserted_at: datetime,
        bq_partition: Optional[date] = None,
        bq_latest_view_query: Optional[str] = None,
        allow_overwrite: bool = False,
    ) -> int:
        chunk = []
        export_count = 0

        logger.info(
            f"Iterating through model generator and persisting model '{model_name}'"
        )
        for model_obj in model_generator:
            model_json_str = BQHelper.update_zendesk_json_for_bq(
                model_obj.to_json(),
                inserted_at=bq_inserted_at,
                inserted_at_col=ZendeskExportMetadata.BQ_INSERTED_AT_COL,
            )

            chunk.append(model_json_str)
            chunk_size_kb = get_kb_size(chunk)
            export_count += 1

            if chunk_size_kb >= self.MAX_CHUNK_SIZE_KB:
                logger.info(
                    (
                        f"Pulled {len(chunk)} {model_name}(s) with size {chunk_size_kb} KB"
                        ">= to max chunk size {self.MAX_CHUNK_SIZE_KB} kb.\nPersisting chunk to GCS."
                    )
                )
                self._write_to_gcs(
                    table_name=model_name,
                    inserted_at=bq_inserted_at,
                    chunk_postfix=export_count,
                    json_strs=chunk,
                )
                chunk = []

        self._write_to_gcs(
            table_name=model_name,
            inserted_at=bq_inserted_at,
            chunk_postfix=export_count,
            json_strs=chunk,
        )

        gcs_path_chunk_prefix = self._mk_gcs_path(
            table_name=model_name, inserted_at=bq_inserted_at
        )
        full_composed_gcs_uri = self.storage_helper.compose_json_objects(
            bucket_name=self.bucket_name, gcs_path_chunk_prefix=gcs_path_chunk_prefix
        )

        if full_composed_gcs_uri:
            self._write_to_bq(
                table_name=model_name,
                gcs_uri=full_composed_gcs_uri,
                bq_partition=bq_partition,
                allow_overwrite=allow_overwrite,
                bq_latest_view_query=bq_latest_view_query,
            )
        logger.info(f"Persisted {export_count} '{model_name}' model(s)")
        return export_count

    @abstractmethod
    def run_export(self):
        pass
