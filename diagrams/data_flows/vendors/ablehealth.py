from diagrams import Cluster, Diagram, Edge
from diagrams.aws.storage import S3
from diagrams.gcp.analytics import BigQuery
from diagrams.gcp.compute import AppEngine
from diagrams.gcp.storage import GCS
from diagrams.onprem.compute import Server

with Diagram(
    name="Able Health",
    filename="ablehealth",
    direction="TB",
    curvestyle="curved"
):
    python_color = "#FFD43B"
    python_edge = Edge(label="Python container", color=python_color)

    with Cluster("Cityblock to Able Health"):
        gcs = GCS("cbh storage")
        BigQuery("cbh data") >> python_edge >> gcs
        gcs >> Edge(label="gsutil") >> S3("ah drop point")

    with Cluster("Able Health to Cityblock"):
        gcs = GCS("cbh storage")
        Server("AH endpoint") >> Edge(label="gsutil") >> gcs
        gcs >> python_edge >> BigQuery("cbh data")
        gcs >> python_edge >> AppEngine("QM Service")
