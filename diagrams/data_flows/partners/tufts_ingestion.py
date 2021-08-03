from diagrams import Cluster, Diagram, Edge
from diagrams.gcp.analytics import BigQuery, Dataflow, PubSub
from diagrams.gcp.storage import GCS
from diagrams.gcp.compute import AppEngine
from diagrams.k8s.compute import Deployment
from diagrams.k8s.compute import Pod
from diagrams.programming.language import Python
from diagrams.oci.compute import Container

with Diagram(
    name="Tufts Ingestion",
    filename="tufts_ingestion",
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

    with Cluster("Load monthly data"):
        silver_data = BigQuery("silver_claims")
        gold_data = BigQuery("gold_claims")
        gcs_bucket >> scio_edge >> Dataflow("LoadToSilverRunner") >> silver_data
        silver_data >> scio_edge >> Dataflow("PolishTuftsData") >> scio_edge >> gold_data
        gold_data >> Edge(label="scio_code", color=scio_color, forward=True, reverse=True) >> Dataflow("CombineTuftsData")
        gold_data >> Edge(label="Python container", color=python_color, forward=True, reverse=True) >> Python("combine_provider")
        gold_data >> Edge(label="Python (for GE)", color=python_color) >> BigQuery("gold_claims_flattened")
        silver_data >> Python("update views")

    with Cluster("Load Prior Auth data (daily)"):
        silver_data = BigQuery("silver_claims")
        gcs_bucket >> scio_edge >> Dataflow("LoadToSilverRunner") >> scio_edge >> silver_data
        silver_data >> Python("update views")

    with Cluster("Load daily member data"):
        silver_data = BigQuery("silver_claims")
        gcs_bucket >> scio_edge >> Dataflow("LoadToSilverRunner") >> scio_edge >> silver_data
        silver_data >> scio_edge >> Dataflow("PolishTuftsDataDaily") >> scio_edge >> BigQuery("gold_claims")
        silver_data >> Python("update views")
        pubsub = PubSub("memberDemographics")
        silver_data >> scio_edge >> Pod("AttributionRunner") >> scio_edge >> AppEngine("Member Service") >> pubsub

    with Cluster("Aptible prod"):
        commons = Container("Commons")
        pubsub >> commons
