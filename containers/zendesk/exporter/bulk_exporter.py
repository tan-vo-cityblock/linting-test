import logging
from datetime import datetime
from timeit import default_timer
from typing import List

from pytz import utc

from export_client.bq_helper import BQHelper, ZendeskExportMetadata
from exporter.base_exporter import BaseExporter, GeneratorHelper

logger = logging.getLogger(__name__)


class BulkExporter(BaseExporter):
    """Class for bulk exports of Zendesk models that don't provide an incremental endpoint."""

    # TODO:
    # - confirm if we have Help Center API and implement export.
    # - confirm if we have satisfaction_ratings on for our account and export it along w/ Satisfaction Reasons
    # - confirm if we have JIRA API and implement export.
    # - Implement custom TagApi to export tags
    BULK_GENERATOR_HELPERS = {
        "group": GeneratorHelper(method="groups"),
        "user_field": GeneratorHelper(method="user_fields"),
        "macro": GeneratorHelper(method="macros"),
        "organization_membership": GeneratorHelper(method="organization_memberships"),
        "organization_field": GeneratorHelper(method="organization_fields"),
        "suspended_ticket": GeneratorHelper(method="suspended_tickets"),
        "brand": GeneratorHelper(method="brands"),
        "sharing_agreement": GeneratorHelper(method="sharing_agreements"),
        "ticket_skip": GeneratorHelper(method="skips"),
        "activity": GeneratorHelper(method="activities"),
        "group_membership": GeneratorHelper(method="group_memberships"),
        "ticket_metric": GeneratorHelper(method="ticket_metrics"),
        "ticket_field": GeneratorHelper(method="ticket_fields"),
        "ticket_form": GeneratorHelper(method="ticket_forms"),
        "view": GeneratorHelper(method="views"),
        "sla_policy": GeneratorHelper(method="sla_policies"),
        "support_address": GeneratorHelper(method="recipient_addresses"),
        "dynamic_content": GeneratorHelper(method="dynamic_content"),
        "target": GeneratorHelper(method="targets"),
        "custom_agent_role": GeneratorHelper(method="custom_agent_roles"),
        "trigger": GeneratorHelper(method="triggers"),
        "automation": GeneratorHelper(method="automations"),
        # "jira_link": GeneratorHelper(method="jira_links"), see TODO above
        # "tag": GeneratorHelper(method="tags"), see TODO above
        # "satisfaction_rating": GeneratorHelper(
        #     method="satisfaction_ratings"
        # ),  # see TODO above
    }

    def __init__(self, models: List[str], **kwargs):
        super().__init__(**kwargs)
        self.models = models

    @property
    def models(self) -> List[str]:
        return self._models

    @models.setter
    def models(self, vals: List[str]):
        all_bulk_models = list(self.BULK_GENERATOR_HELPERS.keys())
        self._models = (
            all_bulk_models
            if not vals
            else self._clean_models_arg(models_arg=vals, all_models=all_bulk_models)
        )

    def run_export(self):
        logger.info(
            f"Starting bulk export of Zendesk models {self.models} in project {self.project_id}.\n"
        )

        bq_inserted_at = datetime.now(utc)
        bq_partition = bq_inserted_at.date()

        for model_name in self.models:
            run_start_time = default_timer()

            model_generator = self._get_model_generator(
                generator_helper=self.BULK_GENERATOR_HELPERS[model_name]
            )

            export_count = self._persist_model(
                model_generator=model_generator,
                model_name=model_name,
                bq_inserted_at=bq_inserted_at,
                bq_partition=bq_partition,
                allow_overwrite=True,
            )

            run_length_sec = int(default_timer() - run_start_time)
            logger.info(
                f"Exported {export_count} Zendesk '{model_name}(s)' to BigQuery in {run_length_sec} seconds."
            )

            metadata = ZendeskExportMetadata(
                table_name=model_name,
                bq_inserted_at=bq_inserted_at,
                export_count=export_count,
                run_length_sec=run_length_sec,
                bq_partition=BQHelper._mk_date_for_query(bq_partition),
            )

            self._persist_metadata(metadata)
            logger.info(
                f"Saved Zendesk '{model_name}(s)' metadata row to BigQuery table {metadata.METADATA_TABLE_NAME}.\n"
            )
        logger.info(f"Success, bulk export of Zendesk models {self.models} complete.")
