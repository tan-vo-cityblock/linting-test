import unittest

import mock
from cf.gcs.events import GCSBlobPath
from cityblock.load_to_silver.matcher import LoadToSilverPatternMatcher
from cityblock.load_to_silver.pattern import LoadToSilverPattern
from cityblock.partner.dag_trigger_effect import PartnerDagTriggerEffect
from mock.mock import MagicMock


class TestPartnerDagTriggerEffect(unittest.TestCase):
    def test_always_patterns(self):
        pdte_strs = PartnerDagTriggerEffect(
            "fakehealth", "fake_dag_id", ["hello_%s", "goodbye_%s",],
        )
        assert all(
            isinstance(patt, LoadToSilverPattern)
            for patt in pdte_strs.precheck_patterns
        )

        pdte_patts = PartnerDagTriggerEffect(
            "fakehealth", "fake_dag_id", ["hello_%s", "goodbye_%s",],
        )
        assert all(
            isinstance(patt, LoadToSilverPattern)
            for patt in pdte_patts.precheck_patterns
        )

    def test_conf(self):
        pdte = PartnerDagTriggerEffect(
            "fakehealth", "fake_dag_id", ["hello_%s", "goodbye_%s",],
        )

        matcher = LoadToSilverPatternMatcher("somewhere", "hello_%s")
        bp = GCSBlobPath("somewhere/hello_50204802.txt")
        assert pdte.conf(matcher, bp)["date"] == "50204802"

    @mock.patch("google.cloud.storage.Blob.exists", MagicMock(return_value=True))
    def test_precheck_go(self):
        pdte = PartnerDagTriggerEffect(
            "fakehealth", "fake_dag_id", ["hello_%s", "goodbye_%s",],
        )

        matcher = LoadToSilverPatternMatcher("somewhere", "hello_%s")
        bp = GCSBlobPath("somewhere/hello_50204802.txt")
        assert pdte.precheck(matcher, bp) is True

    @mock.patch("google.cloud.storage.Blob.exists", MagicMock(return_value=False))
    def test_precheck_missing(self):
        pdte = PartnerDagTriggerEffect(
            "fakehealth", "fake_dag_id", ["hello_%s", "goodbye_%s",],
        )

        matcher = LoadToSilverPatternMatcher("somewhere", "hello_%s")
        bp = GCSBlobPath("somewhere/hello_50204802.txt")
        assert pdte.precheck(matcher, bp) is False
