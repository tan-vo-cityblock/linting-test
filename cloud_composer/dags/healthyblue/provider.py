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
)("dags/healthyblue/provider.py")
from dags.airflow_utils.cityblock.airflow.dags import cbh_DAG
from dags.healthyblue.vocab import PAYER_GCP_PROJECT, service_account_secret

steps, dag = cbh_DAG("healthyblue_provider_v1")
steps.add_secret(service_account_secret)

pull_conf, xcom_date = steps.XComFromConf(task_id="pull_conf_date", conf_key="date",)

to_silver = steps.LoadToSilver(
    task_id="load_to_silver",
    input_config_paths=["healthyblue_silver/provider.txt"],
    delivery_date=xcom_date,
    output_project=PAYER_GCP_PROJECT,
)
silver_views = steps.UpdateBQViews(
    task_id="update_silver_views",
    project=PAYER_GCP_PROJECT,
    dataset="silver_claims",
    views=["provider"],
    date=xcom_date,
)
to_gold = steps.Scio(
    task_id="silver_to_gold",
    cmds=[
        f"bin/ProviderTransformer",
        f"--bigQueryProject={PAYER_GCP_PROJECT}",
        f"--deliveryDate={xcom_date}",
        f"--silverDataset=silver_claims"
        f"--goldDataset=gold_claims"
        f"--goldTable=provider",
    ],
)
gold_views = steps.UpdateBQViews(
    task_id="update_gold_views",
    project=PAYER_GCP_PROJECT,
    dataset="gold_claims",
    views=["provider"],
    date=xcom_date,
)


pull_conf >> to_silver
to_silver >> [to_gold, silver_views]
to_gold >> gold_views

