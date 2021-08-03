import logging
from datetime import datetime
from timeit import default_timer
from typing import List

from export_client.bq_helper import ZendeskExportMetadata
from google.cloud.bigquery.table import RowIterator
from google.cloud.exceptions import NotFound
from pytz import utc
from util import (
    all_are_none,
    all_are_not_none,
    datetime_utc_from_date,
    only_one_arg_set,
)

from exporter.base_exporter import BaseExporter, GeneratorHelper

logger = logging.getLogger(__name__)


class IncrementalExporter(BaseExporter):
    """Class for bulk exports of Zendesk models that don't provide an incremental endpoint."""

    DEFAULT_START_TIME = utc.localize(datetime(2019, 9, 30))

    CURSOR_ONLY = {"cursor": True}
    TIME_ONLY = {"time": True}
    TIME_AND_CURSOR = {**CURSOR_ONLY, **TIME_ONLY}
    # TODO:
    # uncomment ticket_metric_event when Zendesk fixes endpoint to include 'end_of_stream'
    # value in response as indicated in docs: https://developer.zendesk.com/rest_api/docs/support/incremental_export
    # Until then, ticket_metric_event export will run infinitely as there is no way to determine when
    # export is complete for a given time cursor.
    # uncomment ticket_event and ticket_audit when PLAT-1868 is merged in.
    # Confirm if we have ChatAPI enabled and add incremental export.
    # Confirm if we have TalkAPI enabled and add incremental export.
    INCREMENT_GENERATOR_HELPERS = {
        "ticket": GeneratorHelper(method="tickets.incremental", **TIME_AND_CURSOR),
        # "ticket_event": GeneratorHelper(method="tickets.events", **TIME_ONLY),
        # "ticket_audit": GeneratorHelper(method="tickets.audits", **CURSOR_ONLY),
        "user": GeneratorHelper(method="users.incremental", **TIME_ONLY),
        "organization": GeneratorHelper(
            method="organizations.incremental", **TIME_ONLY
        ),
        # "ticket_metric_event": GeneratorHelper(
        #     method="tickets.metrics_incremental", **TIME_ONLY
        # ),
        "nps_recipient": GeneratorHelper(
            method="nps.recipients_incremental", **TIME_ONLY
        ),
        "nps_response": GeneratorHelper(
            method="nps.responses_incremental", **TIME_ONLY
        ),
    }

    def __init__(
        self,
        models: List[str],
        start_date: str = None,
        start_cursor: str = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.start_date = start_date
        self.start_cursor = start_cursor
        self.models = models

    @property
    def models(self) -> List[str]:
        return self._models

    @models.setter
    def models(self, vals: List[str]):
        all_incremental_models = list(self.INCREMENT_GENERATOR_HELPERS.keys())
        self._models = (
            all_incremental_models
            if not vals
            else self._clean_models_arg(
                models_arg=vals, all_models=all_incremental_models
            )
        )

    @staticmethod
    def _unix_timestamp_to_utc_dt(timestamp: int):
        return datetime.utcfromtimestamp(timestamp).replace(tzinfo=utc)

    def _get_latest_view_query(self, model_name):
        # see docs excluding dupes:
        # https://developer.zendesk.com/rest_api/docs/support/incremental_export#excluding-duplicate-items

        if model_name == "ticket_event":
            number_over_query = "PARTITION BY id ORDER BY created_at DESC"
        else:
            number_over_query = "PARTITION BY id ORDER BY updated_at DESC"

        return f"""
        SELECT latest.* EXCEPT(row_num)
        FROM
            (
                SELECT model.*, ROW_NUMBER() OVER ({number_over_query}) AS row_num
                FROM `{self.bq_helper.project}.{self.dataset_name}.{model_name}` AS model
            ) AS latest
        WHERE row_num = 1
        """

    def _set_start_time_or_cursor(
        self, model_name: str, start_date: str = None, start_cursor: str = None
    ):
        if all_are_not_none(start_date, start_cursor):
            raise ValueError(
                "You cannot set both start_date and start_cursor. Set one or none."
            )
        start_time = datetime_utc_from_date(start_date) if start_date else None

        if start_time:
            return start_time, None
        if start_cursor:
            return None, start_cursor

        # here means start_time is None and start_cursor is None
        return self._query_for_start_time_or_cursor(model_name)

    def _query_for_start_time_or_cursor(self, model_name: str):
        full_metadata_table_id = f"{self.project_id}.{self.dataset_name}.{ZendeskExportMetadata.METADATA_TABLE_NAME}"

        query = f"""
        SELECT * FROM {full_metadata_table_id}
        WHERE table_name = "{model_name}"
        ORDER BY bq_inserted_at DESC LIMIT 1
        """
        query_job = self.bq_helper.client.query(query)

        try:
            logger.info(
                f"Run query to pull start_cursor or start_time from {full_metadata_table_id}"
            )
            rows_iterable = query_job.result()
            return self._parse_metadata_row_for_start_time_or_cursor(rows_iterable)
        except NotFound as err:
            logger.warning(
                f"{err}\nSetting export start time to default: {self.DEFAULT_START_TIME}."
            )
            return self.DEFAULT_START_TIME, None

    def _parse_metadata_row_for_start_time_or_cursor(self, rows_iterable: RowIterator):
        rows = list(rows_iterable)
        len_rows = len(rows)

        if len_rows == 0:
            logger.warning(
                f"Setting export start time to default: {self.DEFAULT_START_TIME}."
            )
            return self.DEFAULT_START_TIME, None

        if len_rows > 1:
            raise ValueError(
                f"Query for latest metadata row returned {len_rows} rows instead of 1 row. Fix query."
            )

        row = rows[0]
        start_cursor = row[ZendeskExportMetadata.END_CURSOR_COL]
        start_time = row[ZendeskExportMetadata.END_TIME_COL]

        if not only_one_arg_set(start_time, start_cursor):
            raise ValueError(
                (
                    f"Query for latest metadata row returned values for {ZendeskExportMetadata.END_CURSOR_COL}: {start_cursor} and "
                    f"{ZendeskExportMetadata.END_TIME_COL}: {start_time} when only one value should be set."
                )
            )

        return start_time, start_cursor

    def _get_end_time_and_end_cursor(
        self, model_generator, start_time: datetime, start_cursor: str
    ):
        end_time = (
            self._unix_timestamp_to_utc_dt(model_generator.end_time)
            if hasattr(model_generator, "end_time") and model_generator.end_time
            else None
        )
        end_cursor = (
            model_generator.after_cursor
            if hasattr(model_generator, "after_cursor")
            else None
        )

        if all_are_not_none(end_time, end_cursor):
            raise ValueError(
                (
                    f"Both {ZendeskExportMetadata.END_TIME_COL}: {end_time} and {ZendeskExportMetadata.END_CURSOR_COL}: {end_cursor} are set"
                    f"when only one value should be set. This is an unexpected bug in the zenpy client or underlying Zendesk API."
                )
            )

        if all_are_none(end_time, end_cursor):
            return start_time, start_cursor
        else:
            return end_time, end_cursor

    def run_export(self):
        logger.info(
            f"Starting incremental export of Zendesk models {self.models} in project {self.project_id}.\n"
        )

        bq_inserted_at = datetime.now(utc)
        for model_name in self.models:
            run_start_time = default_timer()

            start_time, start_cursor = self._set_start_time_or_cursor(
                model_name=model_name,
                start_date=self.start_date,
                start_cursor=self.start_cursor,
            )

            logger.info(
                f'Exporting model "{model_name}" with start_time "{start_time}" or start_cursor "{start_cursor}"..'
            )
            model_generator = self._get_model_generator(
                generator_helper=self.INCREMENT_GENERATOR_HELPERS[model_name],
                start_cursor=start_cursor,
                start_time=start_time,
            )

            export_count = self._persist_model(
                model_generator=model_generator,
                model_name=model_name,
                bq_inserted_at=bq_inserted_at,
                bq_latest_view_query=self._get_latest_view_query(model_name),
            )

            end_time, end_cursor = self._get_end_time_and_end_cursor(
                model_generator=model_generator,
                start_time=start_time,
                start_cursor=start_cursor,
            )

            run_length_sec = int(default_timer() - run_start_time)
            logger.info(
                f"Exported {export_count} Zendesk {model_name}(s) in {run_length_sec} seconds."
            )

            metadata = ZendeskExportMetadata(
                table_name=model_name,
                bq_inserted_at=bq_inserted_at,
                start_time=start_time,
                start_cursor=start_cursor,
                end_cursor=end_cursor,
                end_time=end_time,
                export_count=export_count,
                run_length_sec=run_length_sec,
            )

            self._persist_metadata(metadata)
            logger.info(
                f"Saved Zendesk '{model_name}(s)' metadata row to BigQuery table {metadata.METADATA_TABLE_NAME}.\n"
            )
        logger.info(
            f"Success, incremental export of Zendesk models {self.models} complete."
        )
