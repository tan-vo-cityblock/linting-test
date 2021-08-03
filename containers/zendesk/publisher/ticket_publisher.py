import json
import logging
import os
from datetime import datetime
from typing import Any, Dict, List, Tuple, Union

from export_client.bq_helper import BQHelper
from google.cloud.bigquery import SchemaField
from google.cloud.pubsub import PublisherClient
from pytz import utc
from util import (
    date_utc_from_date,
    datetime_to_iso_str,
    json_serialize,
    only_one_arg_set,
)

from .transform_error import TicketUserJoinError

logger = logging.getLogger(__name__)


class TicketPublisher:
    """Class for transforming Zendesk tickets and publishing to pubsub
    The published messages are also saved to BQ.

    Using the saved messages in BQ, this class can also re-publish previously sent messages.
    """

    ATTRIBUTE_KEY_ID = "ticket_id"
    ATTRIBUTE_KEY_MODEL = "model"
    ATTRIBUTE_KEY_UPDATED_AT = "updated_at"

    INSERTED_AT_COL = "inserted_at"
    ATTRIBUTES_COL = "attributes"
    PAYLOAD_COL = "payload"
    TOPIC_COL = "topic"
    MESSAGE_ID_COL = "message_id"

    PUBSUB_TABLE_SCHEMA = [
        SchemaField(
            name=INSERTED_AT_COL,
            field_type=BQHelper.BQ_TIMESTAMP,
            mode=BQHelper.BQ_REQUIRED,
        ),
        SchemaField(
            name=TOPIC_COL, field_type=BQHelper.BQ_STRING, mode=BQHelper.BQ_REQUIRED
        ),
        SchemaField(
            name=MESSAGE_ID_COL,
            field_type=BQHelper.BQ_STRING,
            mode=BQHelper.BQ_REQUIRED,
        ),
        SchemaField(
            name=ATTRIBUTES_COL,
            field_type=BQHelper.BQ_STRING,
            mode=BQHelper.BQ_REQUIRED,
        ),
        SchemaField(
            name=PAYLOAD_COL, field_type=BQHelper.BQ_STRING, mode=BQHelper.BQ_REQUIRED
        ),
    ]

    def __init__(
        self,
        zendesk_project_id: str,
        zendesk_dataset_id: str,
        pubsub_project_id: str,
        pubsub_dataset_id: str,
        pubsub_topic_id: str,
        replay_from_date: str = None,
        replay_to_date: str = None,
    ):
        self.zendesk_project_id = zendesk_project_id
        self.zendesk_dataset_id = zendesk_dataset_id
        self.pubsub_project_id = pubsub_project_id
        self.pubsub_dataset_id = pubsub_dataset_id

        # all pubsub resources live in cityblock-data project regardless of environment.
        # update cityblock-data to self.pubsub_project_id if we ever split pubsub by env.
        self.pubsub_topic = f"projects/cityblock-data/topics/{pubsub_topic_id}"

        self.bq_helper = BQHelper(self.zendesk_project_id)
        self.pubsub_client = PublisherClient()

        self.pubsub_table = self.bq_helper.get_or_create_table(
            project_id=self.pubsub_project_id,
            dataset_id=self.pubsub_dataset_id,
            table_id="zendesk_ticket",
            schema=self.PUBSUB_TABLE_SCHEMA,
        )
        self.ticket_user_join_error_table = self.bq_helper.get_or_create_table(
            project_id=self.zendesk_project_id,
            dataset_id=self.zendesk_dataset_id,
            table_id=TicketUserJoinError.TABLE_NAME,
            schema=TicketUserJoinError.BQ_SCHEMA,
        )

        self.tickets_with_user_join_errors = (
            self._query_for_tickets_with_user_join_errors()
        )

        if only_one_arg_set(replay_from_date, replay_to_date):
            raise ValueError(
                "Both replay_from_date and replay_to_date must be set or both must be none"
            )

        self.replay_from_date = replay_from_date
        self.replay_to_date = replay_to_date

    def _query_for_tickets_with_user_join_errors(self) -> List[Tuple[int, datetime]]:
        full_table_id = self.ticket_user_join_error_table.full_table_id.replace(
            ":", "."
        )
        ticket_id_col = "ticket_id"
        ticket_updated_at_col = "ticket_updated_at"
        query = f"""
        SELECT DISTINCT {ticket_id_col}, {ticket_updated_at_col}
        FROM `{full_table_id}`
        """

        query_job = self.bq_helper.client.query(query)

        tickets_with_user_join_errors = []
        for row in query_job:
            ticket_id = row[ticket_id_col]
            ticket_updated_at = row[ticket_updated_at_col]
            tickets_with_user_join_errors.append((ticket_id, ticket_updated_at))
        return tickets_with_user_join_errors

    def _query_for_zendesk_users_with_cbh_ids(self) -> Dict[str, Union[str, int]]:
        user_table_ref = (
            f"{self.zendesk_project_id}.{self.zendesk_dataset_id}.user_latest"
        )
        id_col = "id"
        external_id_col = "external_id"
        email_col = "email"
        role_col = "role"

        query_end_user = f"""
        SELECT {id_col}, {external_id_col} FROM {user_table_ref}
        WHERE {role_col} = "end-user"
        AND {external_id_col} IS NOT NULL
        """
        query_admin_and_agent = f"""
        SELECT {id_col}, {email_col} FROM {user_table_ref}
        WHERE {role_col} = "agent"
        OR  {role_col} = "admin"
        AND {email_col} IS NOT NULL
        """
        logger.info("Running query to pull zendesk users.")
        query_job_end_user = self.bq_helper.client.query(query_end_user)
        query_job_admin_and_agent = self.bq_helper.client.query(query_admin_and_agent)

        users_with_cbh_ids = {}
        for row in query_job_end_user:
            user_id = row[id_col]
            cbh_id = row[external_id_col]
            users_with_cbh_ids[user_id] = cbh_id

        end_user_count = len(users_with_cbh_ids)
        logger.info(
            f"Pulled {end_user_count} zendesk 'end-user' users with external_ids."
        )

        for row in query_job_admin_and_agent:
            user_id = row[id_col]
            cbh_email = row[email_col]
            users_with_cbh_ids[user_id] = cbh_email

        admin_agent_count = len(users_with_cbh_ids) - end_user_count
        logger.info(
            f"Pulled {admin_agent_count} zendesk 'admin' and 'agent' users with emails."
        )

        return users_with_cbh_ids

    def _transform_user_ids_in_ticket(
        self,
        ticket_dict: Dict[str, Any],
        users_with_cbh_ids: Dict[str, Union[str, int]],
        inserted_at: datetime,
    ) -> Tuple[Dict[str, Any], List[TicketUserJoinError]]:

        ticket_user_id_fields = [
            "requester_id",
            "submitter_id",
            "assignee_id",
            "collaborator_ids",
            "email_cc_ids",
            "follower_ids",
        ]

        ticket_id = ticket_dict.get("id")
        ticket_updated_at = ticket_dict.get("updated_at")
        all_errors = []

        for user_id_field in ticket_user_id_fields:
            user_id_val = ticket_dict.get(user_id_field, None)
            if not user_id_val:
                continue

            is_list_user_id_val = isinstance(user_id_val, list)
            user_id_vals = user_id_val if is_list_user_id_val else [user_id_val]
            user_ids_filtered = list(filter(None, user_id_vals))

            try:
                # access users_with_cbh_ids dict with [], instead of .get,
                # to trigger KeyError if key/val doesn't exist.
                cbh_ids = [users_with_cbh_ids[user_id] for user_id in user_ids_filtered]
                ticket_dict[user_id_field] = (
                    cbh_ids if is_list_user_id_val else cbh_ids[0]
                )
            except KeyError:
                errs = [
                    TicketUserJoinError(
                        ticket_id=ticket_id,
                        ticket_updated_at=ticket_updated_at,
                        ticket_field=user_id_field,
                        user_id=user_id,
                        inserted_at=inserted_at,
                    )
                    for user_id in user_ids_filtered
                ]
                all_errors += errs

        return ticket_dict, all_errors

    def _write_user_join_errors_to_bq(self, errors: List[TicketUserJoinError]):
        new_errors = [
            error
            for error in errors
            if (error.ticket_id, error.ticket_updated_at)
            not in self.tickets_with_user_join_errors
        ]
        new_error_rows = [error.to_json_dict() for error in new_errors]

        if not new_error_rows:
            return

        bq_errors = self.bq_helper.client.insert_rows_json(
            table=self.ticket_user_join_error_table, json_rows=new_error_rows
        )
        if len(bq_errors) > 0:
            for error in bq_errors:
                logger.error(f"{error}\n")
            raise RuntimeError(
                "FAILURE: Error(s) occured while saving ticket user join errors to BQ. See logs above."
            )

    def _get_unpublished_transformed_ticket_query(self):
        """ Reads the `.sql` file and returns it as a String object"""
        file_name = "unpublished_transformed_ticket"
        file = os.path.join(os.path.curdir, "publisher/sql", file_name + ".sql")
        with open(file) as f:
            sql_str = f.read().replace("\n", "")

        sql_str = sql_str.format(
            zendesk_project_id=self.zendesk_project_id,
            zendesk_dataset_id=self.zendesk_dataset_id,
            pubsub_project_id=self.pubsub_project_id,
            pubsub_dataset_id=self.pubsub_dataset_id,
            attribute_key_id=self.ATTRIBUTE_KEY_ID,
            attribute_key_updated_at=self.ATTRIBUTE_KEY_UPDATED_AT,
        )
        return sql_str

    def publish_messages(self):
        users_with_cbh_ids = self._query_for_zendesk_users_with_cbh_ids()

        logger.info("Running query to pull unpublished transformed zendesk tickets.")
        query = self._get_unpublished_transformed_ticket_query()
        query_job = self.bq_helper.client.query(query)

        bq_errors = []
        user_join_error_count = 0
        published_count = 0
        logger.info(
            "Looping through query results to transform user ids in ticket and publish."
        )
        inserted_at = datetime.now(utc)
        for ticket_row in query_job:
            ticket_dict = dict(ticket_row)
            ticket_id = ticket_dict.get("id")

            ticket_dict, transform_errors = self._transform_user_ids_in_ticket(
                ticket_dict=ticket_dict,
                users_with_cbh_ids=users_with_cbh_ids,
                inserted_at=inserted_at,
            )

            if transform_errors:
                user_join_error_count += 1
                self._write_user_join_errors_to_bq(transform_errors)
                continue

            ticket_id = ticket_dict.get("id")
            ticket_updated_at = ticket_dict.get("updated_at")

            attributes = {
                self.ATTRIBUTE_KEY_ID: str(ticket_id),
                self.ATTRIBUTE_KEY_UPDATED_AT: datetime_to_iso_str(ticket_updated_at),
                self.ATTRIBUTE_KEY_MODEL: "ticket",
            }
            attributes_json = json.dumps(attributes, default=json_serialize)
            ticket_json = json.dumps(ticket_dict, default=json_serialize)
            pubsub_ticket_row = {
                self.TOPIC_COL: self.pubsub_topic,
                self.INSERTED_AT_COL: datetime_to_iso_str(inserted_at),
                self.ATTRIBUTES_COL: attributes_json,
                self.PAYLOAD_COL: ticket_json,
            }

            future = self.pubsub_client.publish(
                topic=self.pubsub_topic, data=ticket_json.encode(), **attributes
            )
            message_id = future.result()

            pubsub_ticket_row[self.MESSAGE_ID_COL] = message_id
            errors = self.bq_helper.client.insert_rows_json(
                table=self.pubsub_table, json_rows=[pubsub_ticket_row]
            )
            bq_errors += errors
            published_count += 1

        logger.info(
            (
                f"Published {published_count} zendesk ticket(s) to PubSub topic {self.pubsub_topic}"
                f" and saved messages to BQ {self.pubsub_table.full_table_id}.\n"
            )
        )

        if user_join_error_count:
            logger.warning(
                (
                    f"Did not publish {user_join_error_count} tickets b/c of user join errors."
                    f" See {self.ticket_user_join_error_table.full_table_id} table in BQ for more."
                )
            )

        if len(bq_errors) > 0:
            # TODO: Implement better error handling / notification for bq/pubsub via PLAT-1883
            for error in bq_errors:
                logger.error(f"{error}\n")
            raise RuntimeError(
                "FAILURE: Error(s) occured while saving pubsub messages to BQ. See logs above."
            )

    def _get_from_and_to_date_for_query(self):
        valid_from_date = date_utc_from_date(self.replay_from_date)
        valid_to_date = date_utc_from_date(self.replay_to_date)

        from_date_str = self.bq_helper._mk_date_for_query(valid_from_date)
        to_date_str = self.bq_helper._mk_date_for_query(valid_to_date)

        return from_date_str, to_date_str

    def replay_published_messages(self):
        pubsub_table_ref = f"{self.pubsub_project_id}.{self.pubsub_dataset_id}.{self.pubsub_table.table_id}"

        from_date_str, to_date_str = self._get_from_and_to_date_for_query()

        query = f"""
        SELECT * FROM {pubsub_table_ref}
        WHERE inserted_at
        BETWEEN TIMESTAMP("{from_date_str}") AND TIMESTAMP("{to_date_str}")
        """
        logger.info(
            (
                f"Running query in {pubsub_table_ref} for messages published"
                f" between {from_date_str} inclusive and {to_date_str} to replay"
            )
        )
        query_job = self.bq_helper.client.query(query)
        count = 0
        for row in query_job:
            attributes = row["attributes"]
            payload = row["payload"]

            attributes_dict = json.loads(attributes)

            self.pubsub_client.publish(
                topic=self.pubsub_topic, data=payload.encode(), **attributes_dict
            )
            count += 1
        logger.info(
            f"Re-published {count} zendesk ticket(s) to PubSub topic {self.pubsub_topic}."
        )
