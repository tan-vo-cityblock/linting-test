from typing import Optional


def name_to_task_id(name: str):
    return name.replace("-", "_")


def task_id_to_name(task_id: str):
    return task_id.replace("_", "-")


def xcom_jinga(task_id: str, key: Optional[str] = None) -> str:
    # helper for extremely long xcom_pull syntax
    if key:
        return f"{{{{task_instance.xcom_pull( task_ids='{task_id}', key='{key}')}}}}"
    else:
        return f"{{{{task_instance.xcom_pull( task_ids='{task_id}')}}}}"


def xcom_as_bool(xcom_val: str) -> bool:
    # 2021-07-02 for a while, pattern was to bq_shard_exists.py which wrote a python Bool
    # as a string to sidecar. most dags pulled this xcom then used eval()
    # i, luming, refuse to eval.
    return xcom_val == "True"
