## DAG files

All [DAGs](https://airflow.apache.org/concepts.html#dags) will be contained in this directory

* [Monitoring DAGs](#monitoring-dags)

### Development environment setup

While DAGs should only be tested on the test airflow cluster (instead of locally), you may choose to set up a python 
virtualenv with airflow dependencies to take advantage of IDE features.

```bash
pyenv install 3.6.6
pyenv virtualenv 3.6.6 airflow
pyenv activate airflow
pip install --upgrade pip
pip install --upgrade setuptools
pip install -r requirements.txt
```

*NOTE:* Python 3.6.6 is the [default version on cloud_composer clusters](https://cloud.google.com/composer/docs/concepts/python-version),
and `pip install -r requirements.txt` may fail if you do not use that version.

### Monitoring DAGs
#### Prod
* [Cityblock Cloud Composer Airflow Prod UI](https://r924531339b5480d9-tp.appspot.com)
* [Stackdriver Logs](https://console.cloud.google.com/logs/viewer?resource=cloud_composer_environment%2Flocation%2Fus-east4%2Fenvironment_name%2Fcityblock-composer-env&project=cityblock-orchestration)
* Slack Channel: #airflow_alerts

#### Test
* [Cityblock Cloud Composer Airflow Test UI](https://ub16b5f6f1ad484c4-tp.appspot.com)
* [Stackdriver Logs](https://console.cloud.google.com/logs/viewer?resource=cloud_composer_environment%2Flocation%2Fus-east4%2Fenvironment_name%2Fcityblock-composer-env-test&project=cityblock-orchestration)
* Slack Channel: #airflow_testing

NOTE: For DAG tasks using the `KubernetesPodOperator`,  you can also search for the task's name in project 
[Cityblock Orchestration - K8's Engine - Workloads section](https://console.cloud.google.com/kubernetes/workload?project=cityblock-orchestration&workload_list_tablesize=50).
