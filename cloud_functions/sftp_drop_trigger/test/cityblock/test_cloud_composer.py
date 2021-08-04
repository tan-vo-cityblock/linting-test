import unittest

import requests
from mock.mock import MagicMock, patch
from requests import Response


from cityblock.cloud_composer import CityblockDag, CloudComposerCluster

fake_head_response = Response()
fake_head_response.status_code = 200
fake_head_response.headers = {"Location": "https://idk.com/?client_id=fake_client_id"}

fake_post_response = Response()
fake_post_response.status_code = 200


fake_environment_description = {"config": {"airflowUri": "fakeairflowuri"}}


class TestCloudComposerCluster(unittest.TestCase):
    @patch("googleapiclient.discovery")
    def test_describe(self, google_discovery):
        environment_get = (
            google_discovery.build.return_value.projects.return_value.locations.return_value.environments.return_value.get
        )
        environment_get.return_value.execute = MagicMock(
            return_value=fake_environment_description
        )

        fake_cluster = CloudComposerCluster("fakeproject", "fake_name")
        airflow_uri = fake_cluster.describe()["config"]["airflowUri"]
        environment_get.assert_called_once()
        assert airflow_uri == fake_environment_description["config"]["airflowUri"]

    def test_dag_selector(self):
        fake_cluster = CloudComposerCluster("fakeproject", "fake_name")
        fake_dag = fake_cluster.dag("fake_dag_id")
        assert fake_dag.cluster.name == "fake_name"
        assert fake_dag.dag_id == "fake_dag_id"

    @patch("requests.head", MagicMock(return_value=fake_head_response))
    @patch("google.oauth2.id_token.fetch_id_token", MagicMock(return_value="faketoken"))
    @patch("googleapiclient.discovery")
    def test_iap(self, google_discovery):
        fake_cluster = CloudComposerCluster("fakeproject", "fake_name")
        google_discovery.build.return_value.projects.return_value.locations.return_value.environments.return_value.get.return_value.execute = MagicMock(
            return_value=fake_environment_description
        )
        client_id, token = fake_cluster.iap()
        assert client_id == "fake_client_id"
        assert token == "faketoken"


class TestCityblockDag(unittest.TestCase):
    @patch(
        "cityblock.cloud_composer.CloudComposerCluster.iap",
        MagicMock(return_value=("fake_client_id", "faketoken")),
    )
    def test_trigger(self):
        # requests_head = MagicMock(return_value=fake_response)
        requests.request = MagicMock(return_value=fake_post_response)
        fake_cluster = CloudComposerCluster("fakeproject", "fake_name")
        fake_cluster.describe = MagicMock(return_value=fake_environment_description)
        fake_dag = fake_cluster.dag("fake_dag_id")

        fake_dag.trigger({"date": "30000101"})
        requests.request.assert_any_call(
            "POST",
            "fakeairflowuri/api/experimental/dags/fake_dag_id/dag_runs",
            headers={"Authorization": "Bearer faketoken"},
            json={"conf": {"date": "30000101"}},
            timeout=90,
        )
