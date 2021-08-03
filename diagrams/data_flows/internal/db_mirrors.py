from diagrams import Cluster, Diagram, Edge
from diagrams.gcp.analytics import BigQuery, Dataflow
from diagrams.gcp.database import SQL
from diagrams.gcp.storage import GCS
from diagrams.onprem.database import PostgreSQL, MySQL
from diagrams.programming.language import Python, TypeScript

with Diagram(
    name="Database Mirroring Processes",
    filename="db_mirroring",
    direction="TB",
    curvestyle="curved",
):
    scio_color = "#1AE282"
    python_color = "#FFD43B"

    with Cluster("CloudSQL Mirroring Process (GCP)"):
        sql_db = SQL("PostgreSQL Instance")
        temp_storage = GCS("temp storage (2 days retention)")
        df_jobs = Dataflow("dataflow jobs")
        output_dataset = BigQuery("mirrored dataset")
        scio_process = Edge(label="Scio code", color=scio_color)
        sql_db >> Edge(label="Python container", color=python_color) >> temp_storage
        temp_storage >> scio_process >> df_jobs >> scio_process >> output_dataset
        output_dataset >> Python("update views")

    with Cluster("Commons Mirroring Process"):
        with Cluster("Aptible prod"):
            commons_db = PostgreSQL("Aptible DB Instance")
            builder_tables = TypeScript("export builder tables script")
        with Cluster("GCP"):
            temp_storage = GCS("temp storage (2 days retention)")
            df_jobs = Dataflow("dataflow jobs")
            output_dataset = BigQuery("mirrored dataset")
            scio_process = Edge(label="Scio code", color=scio_color)
            builder_tables >> Edge(label="Aptible CLI / Bash", color="#FFAD0B") >> temp_storage
            commons_db >> Edge(label="Aptible CLI / Bash", color="#FFAD0B") >> temp_storage
            temp_storage >> scio_process >> df_jobs >> scio_process >> output_dataset
            output_dataset >> Edge(label="draftjs transforms (TypeScript container)", forward=True, reverse=True, color="#792B2B") >> temp_storage
            output_dataset >> Python("update views")

    with Cluster("Elation Mirroring Process"):
        with Cluster("Elation prod"):
            elation_db = MySQL("Elation DB Instance")
        with Cluster("GCP"):
            temp_storage = GCS("temp storage (2 days retention)")
            output_dataset = BigQuery("mirrored dataset")
            python_container = Edge(label="Python container", color=python_color)
            elation_db >> python_container >> temp_storage
            temp_storage >> python_container >> output_dataset
