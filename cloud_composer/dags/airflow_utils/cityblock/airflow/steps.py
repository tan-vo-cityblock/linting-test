import sys, os
from functools import reduce

# add cloud_composer/dags to sys.path
(
    lambda path_here: sys.path.insert(
        0,
        reduce(
            lambda prev, n: os.path.dirname(prev),
            range(len(path_here.split("/"))),
            os.path.realpath(__file__),
        ),
    )
)("dags/airflow_utils/cityblock/airflow/steps.py")

from typing import List, Tuple, Union

from airflow.contrib.kubernetes import secret

from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.operators.python_operator import PythonOperator

from dags.airflow_utils.cityblock.airflow.context import CityblockDAGContext
from dags.airflow_utils.cityblock.airflow.util import task_id_to_name, xcom_jinga

DEFAULT_KUBE_ENV_VARS = {
    "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json"
}


class CityblockDAGStepCreator:
    _cbh_dag_ctx: CityblockDAGContext

    def __init__(self, cbh_dag_ctx: CityblockDAGContext) -> None:
        self._cbh_dag_ctx = cbh_dag_ctx

    @property
    def ctx(self):
        return self._cbh_dag_ctx

    def add_secret(self, secret: secret.Secret):
        self.ctx.add_secret(secret)

    def Kubernetes(
        self,
        image: str,
        task_id: str,
        cmds: List[str],
        secrets: List[secret.Secret] = [],
        namespace="default",
        env_vars=DEFAULT_KUBE_ENV_VARS,
        image_pull_policy="Always",
        get_logs=False,
        arguments: Union[List[str], None] = None,
    ):
        return KubernetesPodOperator(
            dag=self.ctx.dag,
            image=image,
            namespace=namespace,
            name=task_id_to_name(task_id),
            task_id=task_id,
            secrets=secrets or self.ctx.secrets,
            env_vars=env_vars,
            get_logs=get_logs,
            image_pull_policy=image_pull_policy,
            cmds=cmds,
            arguments=arguments,
        )

    def Scio(
        self,
        task_id: str,
        cmds: List[str],
        secrets: List[secret.Secret] = [],
        project="cityblock-data",
        environment="prod",
        get_logs=False,
    ):
        return self.Kubernetes(
            image="gcr.io/cityblock-data/mixer_scio_jobs:latest",
            task_id=task_id,
            secrets=secrets or self.ctx.secrets,
            get_logs=get_logs,
            cmds=cmds
            + [
                f"--environment={environment}",
                f"--project={project}",
                "--runner=DataflowRunner",
            ],
        )

    def LoadToSilver(
        self,
        task_id: str,
        input_config_paths: List[str],
        delivery_date: str,
        output_project: str,
        secrets: List[secret.Secret] = [],
        project="cityblock-data",
        environment="prod",
    ):
        return self.Scio(
            task_id=task_id,
            environment=environment,
            project=project,
            secrets=secrets or self.ctx.secrets,
            cmds=[
                "bin/LoadToSilverRunner",
                f"--deliveryDate={delivery_date}",
                "--inputConfigBucket=cbh-partner-configs",
                f"--inputConfigPaths={','.join(input_config_paths)}",
                f"--outputProject={output_project}",
            ],
        )

    def Attribution(
        self,
        task_id: str,
        deployTo: str,
        bigQueryProject: str,
        bigQueryDataset: str,
        secrets: List[secret.Secret] = [],
        project="cityblock-data",
        environment="prod",
        get_logs=False,
    ):
        return self.Scio(
            task_id=task_id,
            environment=environment,
            project=project,
            secrets=secrets or self.ctx.secrets,
            get_logs=get_logs,
            cmds=[
                "bin/AttributionRunner",
                f"--deployTo={deployTo}",
                f"--bigQueryProject={bigQueryProject}",
                f"--bigQueryDataset={bigQueryDataset}",
                "--tempLocation=gs://internal-tmp-cbh/temp",
            ],
        )

    def UpdateBQViews(
        self,
        task_id: str,
        project: str,
        dataset: str,
        views: List[str],
        date: str,
        secrets: List[secret.Secret] = [],
    ):
        return self.Kubernetes(
            task_id=task_id,
            image="gcr.io/cityblock-data/load_monthly_data:latest",
            secrets=secrets or self.ctx.secrets,
            cmds=[
                "python",
                "./update_views.py",
                f"--project={project}",
                f"--dataset={dataset}",
                f'--views={",".join(views)}',
                f"--date={date}",
            ],
        )

    def XComFromConf(
        self, task_id: str, conf_key: str, to_xcom_key: Union[str, None] = None
    ) -> Tuple[PythonOperator, str]:
        xcom_key_name = to_xcom_key or conf_key
        # handy for cloud function triggered dags

        def parse_config_to_xcom(**context):
            # Expected filename example: 20210119
            context["task_instance"].xcom_push(
                key=xcom_key_name, value=context["dag_run"].conf[conf_key]
            )

        xcom_pull_str = xcom_jinga(task_id=task_id, key=xcom_key_name)

        python_operator_step = PythonOperator(
            dag=self.ctx.dag,
            task_id=task_id,
            python_callable=parse_config_to_xcom,
            provide_context=True,
        )

        return python_operator_step, xcom_pull_str

    def BQShardsExist(
        self,
        task_id: str,
        project: str,
        dataset: str,
        tables: List[str],
        date: str,
        secrets: List[secret.Secret] = [],
    ) -> Tuple[KubernetesPodOperator, str]:
        step = self.Kubernetes(
            task_id=task_id,
            image="gcr.io/cityblock-data/load_monthly_data:latest",
            secrets=secrets or self.ctx.secrets,
            cmds=[
                "python",
                "./bq_shard_exists.py",
                f"--project={project}",
                f"--dataset={dataset}",
                f'--tables={",".join(tables)}',
                f"--date={date}",
            ],
        )
        return step, xcom_jinga(task_id)

    def DBT(self, task_id: str, dbt_run_cmd: str):
        secret_mount_dbt = secret.Secret(
            deploy_type="volume",
            deploy_target="/var/secrets/google",
            secret="tf-svc-dbt-prod",
            key="key.json",
        )
        return self.Kubernetes(
            image="gcr.io/cityblock-orchestration/dbt:latest",
            task_id=task_id,
            cmds=["/bin/bash", "-c"],
            secrets=[secret_mount_dbt],
            # TODO: convention was to pass this in as multiline string. why?
            arguments=[dbt_run_cmd],
            env_vars={
                "GOOGLE_APPLICATION_CREDENTIALS": "/var/secrets/google/key.json",
                "DBT_PROFILES_DIR": "/profiles",
            },
            namespace="default",
            image_pull_policy="Always",
        )
