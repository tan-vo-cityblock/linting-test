from diagrams import Cluster, Diagram, Edge
from diagrams.gcp.analytics import BigQuery, Dataflow
from diagrams.gcp.storage import GCS
from diagrams.k8s.compute import Deployment
from diagrams.programming.language import Python

with Diagram(
    name="CCI Ingestion",
    filename="cci_ingestion",
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

    with Cluster("Load monthly data (Amysis and Facets)"):
        silver_data = BigQuery("silver_claims")
        gold_data = BigQuery("gold_claims")
        gcs_bucket >> scio_edge >> Dataflow("LoadToSilverRunner- all claims") >> silver_data
        silver_data >> scio_edge >> Dataflow("PolishCCIClaimsData or Facets") >> scio_edge >> gold_data
        gold_data >> Edge(label="scio_code", color=scio_color, forward=True, reverse=True) >> Dataflow("CombineCCIClaims")
        gold_data >> Edge(label="Python container", color=python_color, forward=True, reverse=True) >> Python("combine_provider")
        gold_data >> Edge(label="Python (for GE)", color=python_color) >> BigQuery("gold_claims_flattened")

    with Cluster("Load weekly Amysis (Pharmacy)"):
        silver_data = BigQuery("silver_claims")
        gcs_bucket >> scio_edge >> Dataflow("LoadToSilverRunner") >> silver_data
        silver_data >> Python("update views")

    with Cluster("Load weekly Facets"):
        silver_data = BigQuery("silver_claims")
        gold_data = BigQuery("gold_claims")
        gcs_bucket >> scio_edge >> Dataflow("LoadToSilverRunner") >> silver_data
        silver_data >> scio_edge >> Dataflow("PolishCCIFacetsWeeklyData") >> scio_edge >> gold_data
        silver_data >> Python("update views")
        gold_data >> Python("update views") >> Edge(label="scio_code", color=scio_color) >> Dataflow("PublishMemberAttributionData")
