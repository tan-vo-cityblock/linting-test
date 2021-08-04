# TODO: move to cityblock.py
from typing import Dict, Optional

import requests
from google.oauth2 import id_token
from urllib.parse import parse_qs, urlparse
from google.auth.transport.requests import Request
import googleapiclient.discovery


class CityblockDag:
    def __init__(self, cluster, name: str):
        self.cluster = cluster
        self.name = name

    def trigger(self, json: Optional[Dict] = None):
        airflow_web_url = self.cluster.describe()["config"]["airflowUri"]
        # iap_client_id refers to client id IAP proxy gcp automatically creates for airflow http
        # there is one client_id per composer environment/airflow cluster
        # google's official recommendation to get it
        # is to hit the airflow web ui unauthenticated
        # to get a redirect url
        # which has a query param client_id
        iap_client_id = parse_qs(urlparse(requests.head(
            airflow_web_url).headers["Location"]).query)["client_id"][0]
        token = id_token.fetch_id_token(Request(), iap_client_id)

        dag_run_url = f"{airflow_web_url}/api/experimental/dags/{self.name}/dag_runs"
        resp = requests.request(
            "POST",
            dag_run_url,
            headers={"Authorization": f"Bearer {token}"},
            json=json,
            timeout=90,
        )

        if resp.status_code == 403:
            raise Exception(
                "Service account does not have permission to "
                "access the IAP-protected Airflow HTTP API."
            )
        elif resp.status_code != 200:
            raise Exception(
                "Bad response from application: {!r} / {!r} / {!r}".format(
                    resp.status_code, resp.headers, resp.text
                )
            )
        else:
            return resp


class CloudComposerCluster:
    project: str
    location: str
    name: str

    def __init__(
        self,
        project: str,
        location: str,
        name: str,
    ):
        self.project = project
        self.location = location
        self.name = name

    def describe(self):
        composer = (googleapiclient.discovery.build(
            "composer", "v1", cache_discovery=False))
        environment = composer.projects().locations().environments().get(
            name=f"projects/{self.project}/locations/{self.location}/environments/{self.name}").execute()
        return environment

    def dag(self, name):
        return CityblockDag(self, name)


prod = CloudComposerCluster(
    "cityblock-orchestration", "us-east4", "prod-airflow"
)
