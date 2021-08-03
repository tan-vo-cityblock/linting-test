from diagrams import Cluster, Diagram, Edge
from diagrams.gcp.analytics import BigQuery, Dataflow
from diagrams.gcp.storage import GCS
from diagrams.k8s.compute import Deployment
from diagrams.programming.language import Python

with Diagram(
    name="Emblem Ingestion",
    filename="emblem_ingestion",
    direction="TB",
    curvestyle="curved"
):
    scio_color = "#1AE282"
    python_color = "#FFD43B"
    scio_edge = Edge(label="scio_code", color=scio_color)

    with Cluster("SFTP Server"):
        sftp_job = Deployment("sftp_drop")
        gcs_bucket = GCS("cbh_sftp_drop")
        sftp_job >> gcs_bucket

    with Cluster("Load monthly data (regular and virtual)"):
        silver_data = BigQuery("silver_claims")
        gold_data = BigQuery("gold_claims")
        gcs_bucket >> scio_edge >> Dataflow("LoadToSilverRunner- all claims") >> silver_data
        gcs_bucket >> scio_edge >> Dataflow("crosswalk file") >> silver_data
        silver_data >> scio_edge >> Dataflow("PolishEmblemClaimsDataV2") >> scio_edge >> gold_data
        gold_data >> Edge(label="Python (for GE)", color=python_color) >> BigQuery("gold_claims_flattened")

    with Cluster("Load Prior Auth data (daily)"):
        silver_data = BigQuery("silver_claims")
        gcs_bucket >> scio_edge >> Dataflow("LoadToSilverRunner") >> scio_edge >> silver_data
        silver_data >> Python("update views")

    with Cluster("Load PBM data"):
        silver_data = BigQuery("silver_claims")
        gcs_bucket >> scio_edge >> Dataflow("LoadToSilverRunner") >> scio_edge >> silver_data
        silver_data >> Python("update views")
