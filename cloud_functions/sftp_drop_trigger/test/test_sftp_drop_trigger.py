from cityblock.cloud_composer import CityblockDag, CloudComposerCluster
import os
import sys
import unittest

sys.path.append(os.path.realpath(".."))
import mock

from main import cf
import cityblock

mock_cluster = CloudComposerCluster("fakeproject", "fakelocation", "fakename")


class TestSFTPDropTrigger(unittest.TestCase):
    @mock.patch("cityblock.partner.dag_trigger_effect.PartnerDagTriggerEffect")
    def test_smoke(self, MockPartnerDagTriggerEffect):
        MockPartnerDagTriggerEffect.precheck = mock.MagicMock(return_value=True)

        fake_dag = CityblockDag(mock_cluster, "fake_dag_id")
        fake_dag.trigger = mock.MagicMock(name="trigger", return_value="ok")
        cityblock.cloud_composer.prod.dag = mock.MagicMock(
            name="dag", return_value=fake_dag
        )

        cf({"name": "healthyblue/drop/NCMT_MEDENCCLMPHD_BCBS_CB_20210721.TXT"})

        cityblock.cloud_composer.prod.dag.assert_called_once_with(
            "healthyblue_professional_v1"
        )
        fake_dag.trigger.assert_called_once()

        cf({"name": "healthyblue/drop/NCMT_MEDENCCLMPHD_BCBS_CB_20210721.TXT"})
        assert fake_dag.trigger.call_count == 2

