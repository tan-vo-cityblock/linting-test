import unittest

import cityblock
import mock
from cityblock.cloud_composer import CityblockDag, CloudComposerCluster
from mock.mock import Mock

from cf.composer.dag_triggers import DagTriggerEffect, dag_trigger
from cf.define.types import LiteGCFMatcher
from cf.gcs.events import GCSBlobPath

fake_dag_id = "abc_dag"
fake_date = "20210709"
mock_cluster = CloudComposerCluster("fakeproject", "fakelocation", "fakename")


class TestDagTriggers(unittest.TestCase):
    def fake_dag(self):

        fake_dag = CityblockDag(mock_cluster, fake_dag_id)
        fake_dag.trigger = Mock(name="trigger", return_value="ok")
        cityblock.cloud_composer.prod.dag = Mock(name="dag", return_value=fake_dag)

        return fake_dag

    def fake_matcher(self):
        return LiteGCFMatcher(lambda bp: True, "matches")

    def test_dag_trigger_effect(self):
        class OverridesDTE(DagTriggerEffect):
            def __init__(self) -> None:
                super().__init__(dag_id=fake_dag_id, cluster=mock_cluster)

        dte = OverridesDTE()
        dte.precheck = mock.Mock(name="precheck", return_value=True)
        dte.conf = mock.Mock(name="conf", return_value={"date": fake_date})

        fake_dag = self.fake_dag()
        bp = GCSBlobPath("fake/path/ok.csv")

        dte.effect(self.fake_matcher(), bp)
        dte.precheck.assert_called_once()
        dte.conf.assert_called_once()
        fake_dag.trigger.assert_called_once_with(conf={"date": fake_date})

    def test_lite_dag_trigger_effect(self):
        precheck_fn = Mock(return_value=True)
        conf_fn = Mock(return_value={"date": "dfoisdjf"})
        dte = dag_trigger(dag_id=fake_dag_id, precheck_fn=precheck_fn, conf_fn=conf_fn,)
        bp = GCSBlobPath("fake/path/ok.csv")
        fake_dag = self.fake_dag()

        dte.effect(self.fake_matcher(), bp)
        precheck_fn.assert_called_once()
        conf_fn.assert_called_once()
        fake_dag.trigger.assert_called_once_with(conf={"date": "dfoisdjf"})


if __name__ == "__main__":
    unittest.main()
