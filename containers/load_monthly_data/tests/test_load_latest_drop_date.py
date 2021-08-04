import pytest
from google.cloud.storage import Blob
from mock import Mock, PropertyMock

from get_latest_drop_date import RetrieveLatestDate, CCI_FILES, TUFTS_FILES


def mock_blob(name: str) -> Blob:
    blob = Mock(spec=Blob)
    type(blob).name = PropertyMock(return_value=name)
    return blob


class TestRetrieveLatestDate:
    valid_emblem_blob = mock_blob(
        name="emblem/drop/CB_COHORT_MEMBERMONTH_EXTRACT_CITYBLOCK20190610.txt")
    valid_cci_blob = mock_blob(name="connecticare_production/drop/ProviderDetail_20191610.txt")

    emblem_blob_1 = mock_blob(
        name="emblem/drop/CB_COHORT_PROFESSIONALCLAIM_EXTRACT_CITYBLOCK20190610.txt")
    emblem_blob_2 = mock_blob(name="emblem/drop/CB_COHORT_LABRESULT_EXTRACT_CITYBLOCK20190610.txt")

    tufts_claims_blob = mock_blob(name="tufts_production/drop/CBUNIFY_20200725_MEDCLAIM.txt")
    tufts_member_blob = mock_blob(name="tufts_production/drop/CBUNIFY_MemberRoster_20200725.txt")

    @pytest.mark.skip(reason="TODO currently failing and needs fixing")
    def test_clean_emblem(self):
        assert RetrieveLatestDate.clean_emblem(blob=self.valid_emblem_blob)

    @pytest.mark.skip(reason="TODO currently failing and needs fixing")
    def test_clean_cci(self):
        assert RetrieveLatestDate.clean_cci(blob=self.valid_cci_blob, required_files=CCI_FILES)

    def test_clean_tufts(self):
        assert RetrieveLatestDate.clean_tufts(blob=self.tufts_claims_blob,
                                              required_files=TUFTS_FILES)
        assert RetrieveLatestDate.clean_tufts(blob=self.tufts_member_blob,
                                              required_files=TUFTS_FILES)

    def test_determine_latest_date(self):
        dates = ["20190701", "20190610", "20190401"]
        partner_blobs = [self.valid_emblem_blob, self.emblem_blob_1, self.emblem_blob_2]
        required_files = [f + "_EXTRACT_CITYBLOCK" for f in
                          ["MEMBERMONTH", "PROFESSIONALCLAIM", "LABRESULT"]]
        latest_date = RetrieveLatestDate.determine_latest_date(dates, partner_blobs, required_files,
                                                               RetrieveLatestDate.append_date)

        assert latest_date == "20190610"

    @pytest.mark.skip(reason="TODO currently failing and needs fixing")
    def test_no_latest_date(self):
        with pytest.raises(RuntimeError) as error:
            random_dates = ["20160510", "20110701"]
            partner_blobs = [self.valid_emblem_blob, self.emblem_blob_1, self.emblem_blob_2]
            required_files = [f + "_EXTRACT_CITYBLOCK" for f in
                              ["MEMBERMONTH", "PROFESSIONALCLAIM", "LABRESULT"]]
            RetrieveLatestDate.determine_latest_date(random_dates, partner_blobs, required_files,
                                                     RetrieveLatestDate.append_date)

        assert "unable to find latest date" in str(error)

    def test_determine_latest_date_tufts(self):
        dates = ["20190701", "20200725", "20201231"]
        partner_blobs = [self.tufts_member_blob, self.tufts_claims_blob]
        required_files = ['CBUNIFY_MemberRoster_{date}.txt', 'CBUNIFY_{date}_MEDCLAIM.txt']
        latest_date = RetrieveLatestDate.determine_latest_date(dates, partner_blobs, required_files,
                                                               RetrieveLatestDate.replace_date_macro)

        assert latest_date == "20200725"
