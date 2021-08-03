# TODO put into generic `get_latest_drop_date.py` and remove entirely
import sys
import re
from datetime import datetime
from typing import List
from functools import partial

from google.cloud import storage
from google.cloud.storage import Blob

import common_utils


class RetrieveLatestDate:
    """ Uses Google Cloud Storage API to get latest date a partner dropped claims data to our SFTP drop bucket"""

    def __init__(self, partner: str):
        self.client = storage.Client(project="cityblock-data")
        self.bucket = self.client.get_bucket("cbh_sftp_drop")

        # partner specific attributes
        self.prefix = PARTNER_INFO.get(partner)["prefix"]
        self.cleaning_f = PARTNER_INFO.get(partner)["clean"]
        self.determine_latest_date = PARTNER_INFO.get(partner)["latest_date"]

    def get_latest_date(self) -> str:
        """Traverses through cloud storage bucket to find latest date the claims data was dropped by partner.
        Date is available in the file (blob) name in a particular format, this varied by partner and partner specific
        algorithms are used to extract this date.

        We first apply an initial filter to ensure a valid date is present in the blobs.
        Then we apply a partner specific filter to ensure the valid monthly data dump files are present.
        Finally we obtain the latest date from this list and ensure all the valid partner files are present for this date.

        Returns:
            date in the form of `YYYYMMdd` (eg. 20190531)

        Raises: RuntimeError if no date is found, most likely means the data format for drop has changed
        """
        sftp_blobs = self.bucket.list_blobs(prefix=self.prefix)
        initially_cleaned_blobs = filter(self._has_numbers, sftp_blobs)
        partner_cleaned_blobs = list(filter(self.cleaning_f, initially_cleaned_blobs))
        ordered_dates_from_blobs = self._obtain_dates_from_blobs(partner_cleaned_blobs)
        most_recent_date = self.determine_latest_date(ordered_dates_from_blobs, partner_cleaned_blobs)

        return most_recent_date

    @staticmethod
    def determine_latest_date(ordered_dates: List[str],
                              partner_cleaned_blobs: List[Blob],
                              required_files: List[str]) -> str:
        """Using the list of dates from blobs, the blobs themselves, and the required files for the partner, we
        determine what the latest date is.

        Done with 3 nested loops
        1) loop through the dates (from most recent to oldest)
        2) loop through the required files - appended with the date from (1)
        3) loop through the blobs

        If all the files in (2) are found in (3) - we break out and return the date we are current in for (1)

        Args:
            ordered_dates:          dates obtained from the blobs
            partner_cleaned_blobs:  full blob from partner
            required_files:         files that must be present in order to run a monthly load job

        Returns:
            latest date if found

        Raises:
            RuntimeError if no dates are found (we exhaust our loops)
        """
        for date in ordered_dates:
            files_with_date = list(map(lambda f: f + date, required_files))
            found_files = set()
            for file in files_with_date:
                for blob in partner_cleaned_blobs:
                    if file in blob.name:
                        found_files.add(file)
            if len(found_files) == len(files_with_date):
                return date
            else:
                continue
        raise RuntimeError("unable to find latest date for {}".format(sys.argv[1]))

    # Emblem specific functions
    @staticmethod
    def clean_emblem(blob) -> bool:
        name = blob.name
        is_text_file = name.endswith(".txt")
        has_cbgn = name.startswith("emblem/drop/CB_COHORT_PHARMACYCLAIM_EXTRACT")
        return is_text_file and has_cbgn

    # CCI specific functions TODO add specific funtions once CCI delivers PBM data
    @staticmethod
    def clean_cci(blob) -> bool:
        name = blob.name
        full_cci_prefix = ["connecticare_production/drop/" + file for file in CCI_FILES]
        is_text_file = name.endswith(".txt")
        has_cci_file_prefix = name.startswith(tuple(full_cci_prefix))
        return is_text_file and has_cci_file_prefix

    @staticmethod
    def _has_numbers(blob) -> bool:
        """ Ensures a number is present in the blob name"""
        return bool(re.search(r'\d', blob.name))

    @staticmethod
    def _obtain_dates_from_blobs(blobs: List[Blob]) -> List[str]:
        """Iterates through blobs and strips the first sequence of number it finds, this is assumed to be the date
        in the `YYYYMMdd` format

        Args:
            blobs: List of Google Cloud Storage Blobs

        Returns:
            List of numbers (dates) ordered from highest to lowest

        """
        def _is_str_a_date(string) -> bool:
            try:
                datetime.strptime(string, "%Y%m%d")
                return True
            except ValueError:
                return False

        strip_number_from_name = map(lambda b: re.findall(r'\d+', b.name)[0], blobs)
        filter_for_dates = filter(_is_str_a_date, strip_number_from_name)
        ordered_dates = sorted(set(filter_for_dates), reverse=True)
        return ordered_dates


EMBLEM_FILES = [
    "CB_COHORT_PHARMACYCLAIM_EXTRACT"
]

CCI_FILES = [
    'Pharmacy_'
]

PARTNER_INFO = {
    "emblem": {
        "prefix": "emblem/drop",
        "clean": RetrieveLatestDate.clean_emblem,
        "latest_date": partial(RetrieveLatestDate.determine_latest_date, required_files=EMBLEM_FILES)
    },
    "cci": {
        "prefix": "connecticare_production/drop",
        "clean": RetrieveLatestDate.clean_cci,
        "latest_date": partial(RetrieveLatestDate.determine_latest_date, required_files=CCI_FILES)
    }
}

if __name__ == "__main__":
    partner_cloud_storage = RetrieveLatestDate(sys.argv[1])
    latest_date = partner_cloud_storage.get_latest_date()
    common_utils.write_str_to_sidecar(latest_date)
