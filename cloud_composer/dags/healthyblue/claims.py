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
)("dags/healthyblue/claims.py")

from airflow.operators.email_operator import EmailOperator
from airflow.operators.python_operator import BranchPythonOperator

from dags.airflow_utils.cityblock.airflow.dags import cbh_DAG
from typing import List
from dags.healthyblue.vocab import (
    EMAIL_TO,
    PAYER_GCP_PROJECT,
    SLUG,
    service_account_secret,
)


def ClaimsDAG(
    dag_id: str,
    silver_configs: List[str],
    silver_views: List[str],
    gold_transformer: str,
    gold_table: str,
    dbt_run_cmd: str,
):
    steps, dag = cbh_DAG(dag_id)
    steps.add_secret(service_account_secret)

    pull_conf, xcom_date = steps.XComFromConf(
        task_id="pull_conf_date", conf_key="date",
    )

    to_silver = steps.LoadToSilver(
        task_id="load_to_silver",
        input_config_paths=silver_configs,
        delivery_date=xcom_date,
        output_project=PAYER_GCP_PROJECT,  # TODO would probably be better to have this in CBHDag
    )
    silver_views = steps.UpdateBQViews(
        task_id="update_silver_views",
        project=PAYER_GCP_PROJECT,
        dataset="silver_claims",
        views=silver_views,
        date=xcom_date,
    )
    to_gold = steps.Scio(
        task_id="silver_to_gold",
        cmds=[
            f"bin/{gold_transformer}",
            f"--bigQueryProject={PAYER_GCP_PROJECT}",
            f"--deliveryDate={xcom_date}",
            f"--silverDataset=silver_claims"
            f"--goldDataset=gold_claims"
            f"--goldTable={gold_table}",
        ],
    )
    gold_views = steps.UpdateBQViews(
        task_id="update_gold_views",
        project=PAYER_GCP_PROJECT,
        dataset="gold_claims",
        # ASSUMPTION - one view per gold table, named the same way
        views=[gold_table],
        date=xcom_date,
    )

    check_for_professional_and_facility, xcom_claim_shards = steps.BQShardsExist(
        task_id="check_for_professional_and_facility",
        project=PAYER_GCP_PROJECT,
        dataset="gold_claims",
        tables=["Professional", "Facility"],
        date=xcom_date,
    )

    push_claims = steps.Scio(
        task_id="push_claims",
        cmds=[
            "bin/PushPatientClaimsEncounters",
            f"--deliveryDate={xcom_date}",
            f"--sourceProject={PAYER_GCP_PROJECT}",
            "--sourceDataset=gold_claims",
        ],
    )

    dbt = steps.DBT(task_id="dbt", dbt_run_cmd=dbt_run_cmd)
    email = EmailOperator(
        dag=dag,
        task_id="email_stakeholders",
        to=EMAIL_TO,
        subject=f"Airflow: Healthy Blue DAG complete for shard: {xcom_date}",
        html_content=f"""
        <html>
        <p>Load Healthy Blue `{dag.dag_id}` job is complete. The relevant tables should be visible in BigQuery and are
        ready for QA.</p>
        <p>Links:
          <ul>
            <li><a href="{dag.dag_id}">View the DAG here</a></li>
            <li><i>ParseErrors link removed. If you depend on this, please contact DMG.</i></li>
            <li><i>Great Expectations link removed. If you depend on this, please contact DMG.</i></li>
          </ul>
        </p>
        <p><i>Shard deletion option removed. If you depend on deleting shards for your workflow for some reason, please contact DMG.</i>
        <p>This is an automated email, please do not reply to this. Contact DMG (#dmg-and-services-support on Slack)</p>
        </html>
        """,
    )
    push_claims_iff_professional_and_facility = BranchPythonOperator(
        task_id="push_claims_iff_professional_and_facility",
        python_callable=lambda **ctx: [push_claims.task_id]
        if ctx["ti"].xcom_pull(check_for_professional_and_facility.task_id)
        else [],
    )

    pull_conf >> to_silver >> [to_gold, silver_views]
    to_gold >> [gold_views, check_for_professional_and_facility]
    gold_views >> [
        dbt,
        email,
    ]
    (
        check_for_professional_and_facility
        >> push_claims_iff_professional_and_facility
        >> push_claims
    )
    return dag


dbt_run_cmd = """
    dbt deps && 
    dbt run -m source:healthy_blue+ --exclude source:commons+ tag:nightly+	
    """


professional_dag = ClaimsDAG(
    dag_id="healthyblue_professional_v1",
    silver_configs=[
        "healthyblue_silver/professional_header.txt",
        "healthyblue_silver/professional_lines.txt",
    ],
    silver_views=["professional_header", "professional_lines"],
    gold_transformer="ProfessionalTransformer",
    gold_table="Professional",
    dbt_run_cmd=dbt_run_cmd,
)

facility_dag = ClaimsDAG(
    dag_id="healthyblue_institutional_v1",
    silver_configs=[
        "healthyblue_silver/institutional_header.txt",
        "healthyblue_silver/institutional_lines.txt",
    ],
    silver_views=["institutional_header", "institutional_lines",],
    gold_transformer="FacilityTransformer",
    gold_table="Facility",
    dbt_run_cmd=dbt_run_cmd,
)

pharmacy_dag = ClaimsDAG(
    dag_id="healthyblue_pharmacy_v1",
    silver_configs=[
        "healthyblue_silver/pharmacy_header.txt",
        "healthyblue_silver/pharmacy_lines.txt",
    ],
    silver_views=["pharmacy_header", "pharmacy_lines"],
    gold_transformer="PharmacyTransformer",
    gold_table="Pharmacy",
    dbt_run_cmd=dbt_run_cmd,
)
