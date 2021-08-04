from typing import Dict, Optional

import requests
from google.oauth2 import id_token
from urllib.parse import parse_qs, urlparse
from google.auth.transport.requests import Request
import googleapiclient.discovery


class CloudComposerCluster:
    project: str
    location: str
    name: str

    def __init__(
        self, project: str, name: str, location: Optional[str] = "us-east4",
    ):
        self.project = project
        self.name = name
        self.location = location

    def describe(self):
        composer = googleapiclient.discovery.build(
            "composer", "v1", cache_discovery=False
        )
        environment = (
            composer.projects()
            .locations()
            .environments()
            .get(
                name=f"projects/{self.project}/locations/{self.location}/environments/{self.name}"
            )
            .execute()
        )
        return environment

    def web_uri(self):
        return self.describe()["config"]["airflowUri"]

    def iap(self):
        """
        iap_client_id refers to client id IAP proxy gcp automatically creates for airflow http
        there is one client_id per composer environment/airflow cluster
        google's official recommendation to get it
        is to hit the airflow web ui unauthenticated
        to get a redirect url
        which has a query param client_id
        """
        location = requests.head(self.web_uri()).headers["Location"]
        iap_client_id = parse_qs(urlparse(location).query)["client_id"][0]
        token = id_token.fetch_id_token(Request(), iap_client_id)
        return iap_client_id, token

    def dag(self, dag_id):
        return CityblockDag(self, dag_id)


prod = CloudComposerCluster(
    project="cityblock-orchestration", location="us-east4", name="prod-airflow"
)


class CityblockDag:
    def __init__(self, cluster, dag_id: str):
        self.cluster = cluster
        self.dag_id = dag_id

    def trigger(self, conf: Optional[Dict] = None):
        airflow_web_url = self.cluster.describe()["config"]["airflowUri"]
        iap_client_id, token = self.cluster.iap()
        dag_run_url = f"{airflow_web_url}/api/experimental/dags/{self.dag_id}/dag_runs"
        resp = requests.request(
            "POST",
            dag_run_url,
            headers={"Authorization": f"Bearer {token}"},
            json={"conf": conf},
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
