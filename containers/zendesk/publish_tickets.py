import argparse
import logging

from publisher.ticket_publisher import TicketPublisher
from util import all_are_not_none, only_one_arg_set

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s:%(levelname)s-%(message)s",
)
logger = logging.getLogger(__name__)


def argument_parser():
    cli = argparse.ArgumentParser(
        description="Argument parser for publishing transformed zendesk tickets pubsub and bq."
    )
    cli.add_argument(
        "--zendesk_project",
        type=str,
        required=True,
        help="Project ID where Zendesk data lives",
    )
    cli.add_argument(
        "--zendesk_dataset",
        type=str,
        required=True,
        help="BQ Dataset where Zendesk data lives",
    )
    cli.add_argument(
        "--pubsub_project",
        type=str,
        required=True,
        help="Project ID where PubSub resources and bq data lives",
    )
    cli.add_argument(
        "--pubsub_dataset",
        type=str,
        required=True,
        help="BQ Dataset where PubSub data lives",
    )
    cli.add_argument(
        "--pubsub_topic",
        type=str,
        required=True,
        help="PubSub topic to publish Zendesk data to",
    )

    cli.add_argument(
        "--replay_from_date",
        type=str,
        default=None,
        help="From date, inclusive, to re-publish pubsub ticket messages.",
    )

    cli.add_argument(
        "--replay_to_date",
        type=str,
        default=None,
        help="To date to re-publish pubsub ticket messages.",
    )
    return cli


if __name__ == "__main__":
    """Publish transformed Zendesk tickets or re-publish messages
    previously published and saved in BQ.
    """
    args = argument_parser().parse_args()

    logger.info(f"Running main with args:\n{args}\n")

    replay_from_date = args.replay_from_date
    replay_to_date = args.replay_to_date

    if only_one_arg_set(replay_from_date, replay_to_date):
        raise ValueError(
            "Both replay_from_date and replay_to_date must be set or both must be none"
        )

    ticket_publisher = TicketPublisher(
        zendesk_project_id=args.zendesk_project,
        zendesk_dataset_id=args.zendesk_dataset,
        pubsub_project_id=args.pubsub_project,
        pubsub_dataset_id=args.pubsub_dataset,
        pubsub_topic_id=args.pubsub_topic,
        replay_from_date=replay_from_date,
        replay_to_date=replay_to_date,
    )

    if all_are_not_none(replay_from_date, replay_to_date):
        ticket_publisher.replay_published_messages()
    else:
        ticket_publisher.publish_messages()
