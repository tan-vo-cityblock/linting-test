import unittest
from airflow.models import DagBag


class TestDagBag(unittest.TestCase):
    def test_fill(self):
        dag_bag = DagBag()
        assert len(dag_bag.import_errors) == 0, "No Import Failures"
