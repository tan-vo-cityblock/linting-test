from diagrams import Cluster, Diagram, Edge
from diagrams.gcp.analytics import BigQuery
from diagrams.onprem.compute import Server

with Diagram(
    name="QReviews",
    filename="qreviews",
    direction="TB",
    curvestyle="curved"
):
    python_color = "#FFD43B"

    with Cluster("QReviews ingestion"):
        Server("QReviews endpoint") >> Edge(label="Python container", color=python_color) >> BigQuery("cbh data")
