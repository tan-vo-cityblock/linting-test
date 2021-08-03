from typing import List, Optional
from airflow.models import Variable
from airflow.models.dag import DAG
from airflow.contrib.kubernetes import secret
import datetime

DEFAULT_ARGS = {
    "owner": "cityblock",
    "depends_on_past": False,
    "email": [Variable.get("slack_email")],
    "email_on_retry": False,
    "retries": 0,
    # KubernetesPodOperator: overriding default - we want a fresh pod on restarts
    "reattach_on_restart": False,
}


class CityblockDAGContext:
    _dag: DAG
    _secrets: List[secret.Secret]

    def __init__(
        self,
        dag_id: str,
        schedule_interval: Optional[str] = None,
        start_date: datetime.datetime = datetime.datetime(2021, 1, 1),
        catchup=False,
    ) -> None:
        self._dag = DAG(
            dag_id=dag_id,
            start_date=start_date,
            schedule_interval=schedule_interval,
            default_args=DEFAULT_ARGS,
            catchup=catchup,
        )
        self._secrets = []

    @property
    def dag(self):
        return self._dag

    def add_secret(self, secret: secret.Secret):
        self._secrets.append(secret)

    @property
    def secrets(self):
        return self._secrets
