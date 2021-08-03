from diagrams import Cluster, Diagram, Edge
from diagrams.gcp.analytics import BigQuery, PubSub
from diagrams.gcp.storage import GCS
from diagrams.onprem.compute import Server

with Diagram(
    name="Zendesk",
    filename="zendesk",
    direction="TB",
    curvestyle="curved"
):
    python_color = "#FFD43B"
    python_edge = Edge(label="Python container", color=python_color)

    zendesk_endpoint = Server("zendesk endpoint")

    with Cluster("Bulk Export"):
        zendesk_endpoint >> python_edge >> GCS("temp storage") >> python_edge >> BigQuery("cbh data")

    with Cluster("Incremental Export"):
        zendesk_endpoint >> python_edge >> GCS("temp storage") >> python_edge >> BigQuery("cbh data") >> python_edge >> PubSub("zendesk")
