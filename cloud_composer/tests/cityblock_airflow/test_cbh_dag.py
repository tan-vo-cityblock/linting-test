import unittest
from dags.airflow_utils.cityblock.airflow.dags import cbh_DAG
from airflow.models import DAG


class TestCBHDAG(unittest.TestCase):
    def test_adding_tasks(self):
        steps, dag = cbh_DAG("some_dag")
        assert len(dag.task_dict) == 0
        steps.LoadToSilver(
            task_id="some_lts",
            input_config_paths=[],
            delivery_date="20210302",
            output_project="fakeout",
        )
        assert len(dag.task_dict) == 1


if __name__ == "__main__":
    unittest.main()
