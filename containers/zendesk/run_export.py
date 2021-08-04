import argparse
import logging
import os

from exporter.bulk_exporter import BulkExporter
from exporter.incremental_exporter import IncrementalExporter

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s:%(levelname)s-%(message)s",
)
logger = logging.getLogger(__name__)


def argument_parser():
    cli = argparse.ArgumentParser(
        description="Argument parser for exporting Zendesk models to BQ."
    )
    cli.add_argument(
        "--project", type=str, required=True, help="Project ID we are exporting to"
    )
    cli.add_argument(
        "--bucket", type=str, required=True, help="GCS bucket we are exporting to"
    )
    cli.add_argument(
        "--dataset", type=str, required=True, help="BQ Dataset name we are exporting to"
    )
    cli.add_argument(
        "--start_date", type=str, default=None, help="Start date for exporting tickets."
    )
    cli.add_argument(
        "--start_cursor",
        type=str,
        default=None,
        help="Start cursor for exporting tickets.",
    )
    cli.add_argument(
        "--zendesk_secret_path",
        type=str,
        default=os.environ.get("ZENDESK_SECRET_PATH"),
        help="Path to json file holding Zendesk secrets",
    )
    cli.add_argument(
        "--incremental_export",
        action="store_true",
        help="Add flag if incremental exporter should run",
    )
    cli.add_argument(
        "--incremental_models",
        nargs="*",
        type=str,
        default=[],
        help="Pass in list of incremental models to export: --incremental_models e f g",
    )

    cli.add_argument(
        "--bulk_export",
        action="store_true",
        help="Add flag if bulk exporter should run",
    )
    cli.add_argument(
        "--bulk_models",
        nargs="*",
        type=str,
        default=[],
        help="Pass in list of bulk models to export: --bulk_models a b c",
    )
    return cli


if __name__ == "__main__":
    args = argument_parser().parse_args()

    logger.info(f"Running main with args:\n{args}\n")

    should_bulk_export = args.bulk_export
    should_incremental_export = args.incremental_export
    both_exports_set = should_bulk_export and should_incremental_export
    no_exports_set = not should_bulk_export and not should_incremental_export

    if both_exports_set or no_exports_set:
        raise ValueError(
            "You must include either an --increment_export or a --bulk_export flag but not both."
        )

    common_args = {
        "project_id": args.project,
        "bucket_name": args.bucket,
        "dataset_name": args.dataset,
        "zendesk_secret_path": args.zendesk_secret_path,
    }

    if should_incremental_export:
        # For more on increment endpoint ratelimit, see: https://developer.zendesk.com/rest_api/docs/support/introduction#endpoint-specific-rate-limits
        zendesk_incremental_exporter = IncrementalExporter(
            models=args.incremental_models,
            start_date=args.start_date,
            start_cursor=args.start_cursor,
            **common_args,
        )
        zendesk_incremental_exporter.run_export()
    elif should_bulk_export:
        zendesk_bulk_exporter = BulkExporter(models=args.bulk_models, **common_args)
        zendesk_bulk_exporter.run_export()
