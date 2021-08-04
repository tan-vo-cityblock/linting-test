import logging
from textwrap import dedent
from typing import Dict, List, Optional, Union

from cf.composer.dag_triggers import DagTriggerEffect
from cf.gcs.events import GCSBlobPath
from cityblock import load_to_silver
from cityblock.load_to_silver.matcher import LoadToSilverPatternMatcher
from cityblock.load_to_silver.pattern import LoadToSilverPattern
from cityblock.sftp import GCS_BUCKET
from google.cloud import storage


class PartnerDagTriggerEffect(DagTriggerEffect):
    partner_slug: str
    ext: str
    dag_id: str
    precheck_patterns: List[LoadToSilverPattern]

    def __init__(
        self,
        partner_slug: str,
        dag_id: str,
        precheck_patterns: Union[List[LoadToSilverPattern], List[str]],
        ext: Optional[str] = "csv",
    ) -> None:
        super().__init__(dag_id=dag_id)
        self.partner_slug = partner_slug
        self.storage_client = storage.Client()
        self.ext = ext
        self.dag_id = dag_id

        if all(isinstance(patt, str) for patt in precheck_patterns):
            self.precheck_patterns = list(
                map(load_to_silver.pattern.compile, precheck_patterns)
            )
        else:
            self.precheck_patterns = precheck_patterns

    def _expected_gcs_fullpath(self, pattern: LoadToSilverPattern, datestr: str):
        return f"{self.partner_slug}/drop/{pattern.with_date(datestr)}.{self.ext}"

    def conf(self, matcher: LoadToSilverPatternMatcher, blob_path: GCSBlobPath) -> Dict:
        datestr = matcher.pattern.get_date(blob_path.filename_noext)
        return {"date": datestr}

    def precheck(
        self, matcher: LoadToSilverPatternMatcher, blob_path: GCSBlobPath
    ) -> bool:
        datestr = matcher.pattern.get_date(blob_path.filename_noext)
        for pattern in self.precheck_patterns:
            is_this_blob = pattern.match(blob_path.filename_noext)

            if not is_this_blob:
                expected_gcs_fullpath = self._expected_gcs_fullpath(pattern, datestr)
                is_in_gcs = (
                    self.storage_client.bucket(GCS_BUCKET)
                    .blob(expected_gcs_fullpath)
                    .exists()
                )
                if not is_in_gcs:
                    logging.info(
                        dedent(
                            f"""Missing file required for trigging dag:{self.dag_id}.
                            File not found: gs://{expected_gcs_fullpath}.
                            Requires: {','.join([
                                self._expected_gcs_fullpath(pattern, datestr)
                                for pattern in
                                self.precheck_patterns])})"""
                        )
                    )
                    return False

        return True
