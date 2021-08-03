import unittest
import mock
from airflow.contrib.kubernetes import secret
from dags.airflow_utils.cityblock.airflow.steps import CityblockDAGStepCreator
from dags.airflow_utils.cityblock.airflow.context import CityblockDAGContext


class TestCBHDAGStepCreator(unittest.TestCase):
    def test_creation(self):
        ctx = CityblockDAGContext(dag_id="some_dag_id")
        steps = CityblockDAGStepCreator(ctx)
        assert isinstance(steps.ctx, CityblockDAGContext)

    def test_adding_secrets(self):
        ctx = CityblockDAGContext(dag_id="some_dag_id")
        steps = CityblockDAGStepCreator(ctx)
        assert len(steps.ctx.secrets) == 0
        steps.add_secret(
            secret.Secret(
                deploy_type="env",
                deploy_target="USER",
                secret="ablehealth-prod-secrets",
                key="user",
            )
        )
        assert len(steps.ctx.secrets) == 1

        k = steps.Kubernetes(image="fakeimage", task_id="ok", cmds=["echo", "hi"])

        assert len(k.secrets) == 1

    def test_kube(self):
        ctx = CityblockDAGContext(dag_id="some_dag_id")
        steps = CityblockDAGStepCreator(ctx)
        assert len(ctx._dag.task_dict) == 0
        k = steps.Kubernetes(image="fakeimage", task_id="ok", cmds=["echo", "hi"])
        assert k.image == "fakeimage"
        assert len(ctx._dag.task_dict) == 1

    def test_scio(self):
        ctx = CityblockDAGContext(dag_id="some_dag_id")
        steps = CityblockDAGStepCreator(ctx)
        assert len(ctx._dag.task_dict) == 0
        s = steps.Scio(task_id="scio_step", cmds=["bin/SomeJob", "--ok"])

        assert s.image == "gcr.io/cityblock-data/mixer_scio_jobs:latest"
        assert len([cmd for cmd in s.cmds if cmd.startswith("--environment")]) == 1
        assert len([cmd for cmd in s.cmds if cmd.startswith("--project")]) == 1
        assert len([cmd for cmd in s.cmds if cmd == "--runner=DataflowRunner"]) == 1
        assert len(ctx._dag.task_dict) == 1

    def test_load_to_silver(self):
        ctx = CityblockDAGContext(dag_id="some_dag_id")
        steps = CityblockDAGStepCreator(ctx)
        assert len(ctx._dag.task_dict) == 0
        lts = steps.LoadToSilver(
            task_id="scio_step",
            input_config_paths=[
                "somepartner/professional.txt",
                "somepartner/facility.txt",
            ],
            delivery_date="20210101",
            output_project="some-project",
        )

        assert lts.image == "gcr.io/cityblock-data/mixer_scio_jobs:latest"
        assert "--runner=DataflowRunner" in lts.cmds
        assert "--deliveryDate=20210101" in lts.cmds
        assert "--inputConfigBucket=cbh-partner-configs" in lts.cmds
        assert (
            "--inputConfigPaths=somepartner/professional.txt,somepartner/facility.txt"
            in lts.cmds
        )
        assert "--outputProject=some-project" in lts.cmds

        assert len(ctx._dag.task_dict) == 1

    @mock.patch("airflow.models.taskinstance.TaskInstance")
    @mock.patch("airflow.models.dagrun.DagRun")
    def test_xcom_from_conf(self, MockTaskInstance, MockDagRun):
        ctx = CityblockDAGContext(dag_id="some_dag_id")
        steps = CityblockDAGStepCreator(ctx)
        assert len(ctx._dag.task_dict) == 0

        mock_task_instance = MockTaskInstance()
        mock_task_instance.xcom_push = mock.MagicMock()

        mock_dag_run = MockDagRun()
        mock_dag_run.conf = {"some_key_in_conf": "some_val"}

        xc, xcom_pull = steps.XComFromConf(
            task_id="pull_conf_task_id", conf_key="some_key_in_conf",
        )

        xc.python_callable(task_instance=mock_task_instance, dag_run=mock_dag_run)
        mock_task_instance.xcom_push.assert_called_once_with(
            key="some_key_in_conf", value=mock_dag_run.conf["some_key_in_conf"]
        )

        assert (
            xcom_pull
            == "{{task_instance.xcom_pull( task_ids='pull_conf_task_id', key='some_key_in_conf')}}"
        )


if __name__ == "__main__":
    unittest.main()
