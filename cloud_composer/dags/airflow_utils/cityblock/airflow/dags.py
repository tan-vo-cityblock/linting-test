import datetime
from typing import Optional, Tuple


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
)("dags/airflow_utils/cityblock/airflow/dags.py")


from dags.airflow_utils.cityblock.airflow.context import CityblockDAGContext
from dags.airflow_utils.cityblock.airflow.steps import CityblockDAGStepCreator

from airflow.models import DAG

# our version of airflow actually ignores a file if the string DAG isn't in there
def cbh_DAG(
    dag_id: str,
    schedule_interval: Optional[str] = None,
    start_date: datetime.datetime = datetime.datetime(2021, 1, 1),
    catchup=False,
) -> Tuple[CityblockDAGStepCreator, DAG]:
    ctx = CityblockDAGContext(
        dag_id=dag_id,
        start_date=start_date,
        schedule_interval=schedule_interval,
        catchup=catchup,
    )
    steps = CityblockDAGStepCreator(ctx)
    return steps, ctx.dag
