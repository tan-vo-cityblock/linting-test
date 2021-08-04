import re
from argparse import ArgumentParser
from datetime import datetime
from functools import partial
from typing import List, Optional, Callable

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
        most_recent_date = self.determine_latest_date(ordered_dates_from_blobs,
                                                      partner_cleaned_blobs)

        return most_recent_date

    @staticmethod
    def determine_latest_date(ordered_dates: List[str],
                              partner_cleaned_blobs: List[Blob],
                              required_files: List[str],
                              format_filename: Callable[[str, str], str]) -> str:
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
            format_filename:        format filename to include delivery date

        Returns:
            latest date if found

        Raises:
            RuntimeError if no dates are found (we exhaust our loops)
        """
        for date in ordered_dates:
            files_with_date = list(map(lambda f: format_filename(f, date), required_files))
            found_files = set()
            for file in files_with_date:
                for blob in partner_cleaned_blobs:
                    print(f'File is {file}\tBlob is {blob.name}')
                    if file in blob.name:
                        found_files.add(file)
            if len(found_files) == len(files_with_date):
                return date
            else:
                continue
        raise RuntimeError(
            "unable to find latest date for files: [{}]".format(", ".join(required_files)))

    # Emblem specific functions
    @staticmethod
    def clean_emblem(blob) -> bool:
        name = blob.name
        is_text_file = name.endswith(".txt")
        has_cb_cohort = name.startswith("emblem/drop/CB_")
        return is_text_file and has_cb_cohort

    # CCI specific functions
    @staticmethod
    def clean_cci(blob, required_files) -> bool:
        name = blob.name
        full_cci_prefix = ["connecticare_production/drop/" + file for file in required_files]
        is_text_file = name.endswith(".txt")
        has_cci_file_prefix = name.startswith(tuple(full_cci_prefix))
        return is_text_file and has_cci_file_prefix

    # Tufts specific functions
    @staticmethod
    def clean_tufts(blob, required_files):
        name = blob.name
        date_string = RetrieveLatestDate._obtain_date_string_from_blob(blob)
        if not date_string:
            return False
        files_with_prefix_and_date = [
            'tufts_production/drop/' + RetrieveLatestDate.replace_date_macro(f, date_string)
            for f in required_files]
        return name in files_with_prefix_and_date

    # Carefirst specific function
    @staticmethod
    def clean_carefirst(blob) -> bool:
        name = blob.name
        is_text_file = name.endswith(".txt")
        has_prefix = name.startswith("carefirst/drop/")
        return is_text_file and has_prefix

    @staticmethod
    def _has_numbers(blob) -> bool:
        """ Ensures a number is present in the blob name"""
        return bool(re.search(r'\d', blob.name))

    @staticmethod
    def _is_str_a_date(string) -> bool:
        try:
            if bool(re.search(r'\d{14}', string)):
                datetime.strptime(string, "%Y%m%d%H%M%S")
            else:
                datetime.strptime(string, "%Y%m%d")
            return True
        except ValueError:
            return False

    @staticmethod
    def _obtain_date_string_from_blob(blob: Blob) -> Optional[str]:
        """Returns the first date or timestamp string in a blob's name if there is one. Otherwise, returns None.
        """
        # Facets files include digits in the file names so select only digit strings of a certain length to be dates.
        matches = re.findall(r'\d{3,}', blob.name)
        if len(matches) < 1:
            return None
        date_string = matches[0]
        if not RetrieveLatestDate._is_str_a_date(date_string):
            return None
        return date_string

    @staticmethod
    def _obtain_dates_from_blobs(blobs: List[Blob]) -> List[str]:
        """Iterates through blobs and strips the first sequence of number it finds, this is assumed to be the date
        in either of `YYYYMMdd` or `YYYYMMddHHmmss` formats.

        Args:
            blobs: List of Google Cloud Storage Blobs

        Returns:
            List of numbers (dates) ordered from highest to lowest

        """

        # Remove hours/seconds data if necessary to pass this date forward. Keep date the same otherwise.
        def abbreviate_date(date) -> str:
            return date[:8]

        # Facets files include digits in the file names so select only digit strings of a certain length to be dates.
        date_strings = [d for d in map(RetrieveLatestDate._obtain_date_string_from_blob, blobs) if
                        d is not None]
        abbreviated_dates = map(lambda d: abbreviate_date(d), date_strings)
        ordered_dates = sorted(set(abbreviated_dates), reverse=True)
        return ordered_dates

    @staticmethod
    def append_date(filename: str, date: str) -> str:
        return filename + date

    @staticmethod
    def replace_date_macro(filename: str, date: str) -> str:
        return filename.format(date=date)


# TODO handle `CityBlock_Member_Crosswalk_` somehow (date is `YYYYmm` format, no `dd` like in the rest)
EMBLEM_FILES = [
    "COHORT_FACILITYCLAIM_EXTRACT",
    "COHORT_PROFCLAIM_EXTRACT",
    "COHORT_PHARMACYCLAIM_EXTRACT",
    "COHORT_MEMBERMONTH_EXTRACT",
    "COHORT_LABRESULT_EXTRACT",
    "COHORT_MEMBERDEMO_EXTRACT",
    "COHORT_DIAGASSOCIATION_EXTRACT",
    "COHORT_PROCASSOCIATION_EXTRACT",
    "COHORT_EMPLOYERGROUPACCOUNT",
    "COHORT_PROVIDER",
]

EMBLEM_VIRTUAL_FILES = [
    "VIC_FACILITYCLAIM_EXTRACT",
    "VIC_PROFCLAIM_EXTRACT",
    "VIC_PHARMACYCLAIM_EXTRACT",
    "VIC_MEMBERMONTH_EXTRACT",
    "VIC_LABRESULT_EXTRACT",
    "VIC_MEMBERDEMO_EXTRACT",
    "VIC_DIAGASSOCIATION_EXTRACT",
    "VIC_PROCASSOCIATION_EXTRACT",
    "VIC_EMPLOYERGROUPACCOUNT",
    "VIC_PROVIDER",
]

CCI_FILES = [
    "ProviderDetail_",
    "ProviderAddresses_",
    "Member_",
    "Medical_",
    "MedicalDiagnosis_",
    "MedicalICDProc_",
    "CodeDetails_",
    "ControlTotals_",
    "Lab_"
]

CCI_FACETS_FILES = [
    "CCI_CITYBLOCK_MULTI_PHRMCLM_EDL_FW01_PROD_",
    "CCI_CITYBLOCK_MULTI_PAIDCLM_EDL_FM01_PROD_",
    "CCI_CITYBLOCK_MULTI_REVECLM_EDL_FM01_PROD_",
    "CCI_CITYBLOCK_MULTI_PROV_EDL_FM01_PROD_"
]

CCI_FACETS_WEEKLY_FILES = [
    "CCI_CITYBLOCK_MULTI_MEMELIG_FA_FW01_PROD_"
]

TUFTS_FILES = [
    'CBUNIFY_{date}_MEDCLAIM.txt',
    'CBUNIFY_{date}_Provider.txt',
    'CBUNIFY_{date}_RXCLAIM.txt'
]

CAREFIRST_WEEKLY_FILES = [
    'CBH_CAREFIRST_FACILITYCLAIM_EXTRACT_',
    'CBH_CAREFIRST_PROFESSIONALCLAIM_EXTRACT_',
    'CBH_CAREFIRST_DIAGASSOCIATION_EXTRACT_',
    'CBH_CAREFIRST_PROCASSOCIATION_EXTRACT_',
    'CBH_CAREFIRST_PHARMACYCLAIM_EXTRACT_',
    'CBH_CAREFIRST_LABRESULT_EXTRACT_',
    'CBH_CAREFIRST_PROVIDER_EXTRACT_'
]

PARTNER_INFO = {
    "emblem": {
        "prefix": "emblem/drop",
        "clean": RetrieveLatestDate.clean_emblem,
        "latest_date": partial(RetrieveLatestDate.determine_latest_date,
                               required_files=EMBLEM_FILES,
                               format_filename=RetrieveLatestDate.append_date),
    },
    "emblem_virtual": {
        "prefix": "emblem/drop",
        "clean": RetrieveLatestDate.clean_emblem,
        "latest_date": partial(RetrieveLatestDate.determine_latest_date,
                               required_files=EMBLEM_VIRTUAL_FILES,
                               format_filename=RetrieveLatestDate.append_date),
    },
    "cci": {
        "prefix": "connecticare_production/drop",
        "clean": partial(RetrieveLatestDate.clean_cci, required_files=CCI_FILES),
        "latest_date": partial(RetrieveLatestDate.determine_latest_date,
                               required_files=CCI_FILES,
                               format_filename=RetrieveLatestDate.append_date),
    },
    "cci_facets": {
        "prefix": "connecticare_production/drop",
        "clean": partial(RetrieveLatestDate.clean_cci, required_files=CCI_FACETS_FILES),
        "latest_date": partial(RetrieveLatestDate.determine_latest_date,
                               required_files=CCI_FACETS_FILES,
                               format_filename=RetrieveLatestDate.append_date),
    },
    "cci_facets_weekly": {
        "prefix": "connecticare_production/drop",
        "clean": partial(RetrieveLatestDate.clean_cci, required_files=CCI_FACETS_WEEKLY_FILES),
        "latest_date": partial(RetrieveLatestDate.determine_latest_date,
                               required_files=CCI_FACETS_WEEKLY_FILES,
                               format_filename=RetrieveLatestDate.append_date),
    },
    "tufts": {
        "prefix": "tufts_production/drop",
        "clean": partial(RetrieveLatestDate.clean_tufts, required_files=TUFTS_FILES),
        "latest_date": partial(RetrieveLatestDate.determine_latest_date,
                               required_files=TUFTS_FILES,
                               format_filename=RetrieveLatestDate.replace_date_macro),
    },
    "carefirst_weekly": {
        "prefix": "carefirst/drop",
        "clean": RetrieveLatestDate.clean_carefirst,
        "latest_date": partial(RetrieveLatestDate.determine_latest_date,
                               required_files=CAREFIRST_WEEKLY_FILES,
                               format_filename=RetrieveLatestDate.append_date),
    }
}

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("--partner", help=f"One of {', '.join(PARTNER_INFO.keys())}",
                        required=False)
    parser.add_argument("--debug", action="store_true", help="Print result to stdout",
                        required=False)
    args = parser.parse_args()

    partner_cloud_storage = RetrieveLatestDate(partner=args.partner)
    latest_date = partner_cloud_storage.get_latest_date()
    if args.debug is not None:
        print(f"Found latest date: {latest_date}")
    common_utils.write_str_to_sidecar(latest_date)
