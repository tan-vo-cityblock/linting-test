from datetime import datetime
import json
from export_client.bq_helper import BQHelper
from google.cloud.bigquery import SchemaField
from util import json_serialize


class TicketUserJoinError(Exception):
    """Class for throwing/logging of errors related to joining zendesk users
    to internal cbh users in Zendesk tickets.

    Will write to `project.dataset.TICKET_USER_JOIN_ERROR`

    TODO: At some point, abstract out for any container image to use.
    """

    TABLE_NAME = "TICKET_USER_JOIN_ERROR"

    BQ_SCHEMA = [
        SchemaField("ticket_id", BQHelper.BQ_INTEGER, BQHelper.BQ_REQUIRED),
        SchemaField("ticket_updated_at", BQHelper.BQ_TIMESTAMP, BQHelper.BQ_REQUIRED),
        SchemaField("ticket_field", BQHelper.BQ_STRING, BQHelper.BQ_REQUIRED),
        SchemaField("user_id", BQHelper.BQ_INTEGER, BQHelper.BQ_REQUIRED),
        SchemaField("inserted_at", BQHelper.BQ_TIMESTAMP, BQHelper.BQ_REQUIRED),
    ]

    def __init__(
        self,
        ticket_id: str,
        ticket_updated_at: str,
        ticket_field: str,
        user_id: str,
        inserted_at: datetime,
    ):
        self.ticket_id = ticket_id
        self.ticket_updated_at = ticket_updated_at
        self.ticket_field = ticket_field
        self.user_id = user_id
        self.inserted_at = inserted_at

    def to_json_dict(self):
        class_dict = vars(self)
        serialized_dict = json.loads(json.dumps(class_dict, default=json_serialize))
        return serialized_dict
