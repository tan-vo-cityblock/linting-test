import unittest
import datetime
from airflow.models.dag import DAG
from airflow.contrib.kubernetes import secret
from dags.airflow_utils.cityblock.airflow.context import CityblockDAGContext


class TestCBHDAGContext(unittest.TestCase):
    def test_creation(self):
        sd = datetime.datetime(2021, 1, 1)
        ctx = CityblockDAGContext(dag_id="some_dag_id", start_date=sd)
        assert isinstance(ctx.dag, DAG)
        assert ctx.dag.start_date == sd

    def test_adding_secrets(self):
        ctx = CityblockDAGContext(dag_id="some_dag_id")
        assert len(ctx.secrets) == 0
        ctx.add_secret(
            secret.Secret(
                deploy_type="env",
                deploy_target="USER",
                secret="ablehealth-prod-secrets",
                key="user",
            )
        )
        assert len(ctx.secrets) == 1


if __name__ == "__main__":
    unittest.main()
