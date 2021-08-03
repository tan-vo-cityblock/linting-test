""" Common utilities for scripts relevant to loading monthly data"""
import json


def write_str_to_sidecar(string: str) -> None:
    """ Writes a string to the file that is to be used for downstream processing in Airflow context.
    Should be used when `do_xcom_push` is set to `True` for the `KubernetesPodOperator`

    Args:
        string: value to expose on Airflow DAG for downstream processing

    """
    fs = open("/airflow/xcom/return.json", "x")
    fs.write(json.dumps(string))
    fs.close()
