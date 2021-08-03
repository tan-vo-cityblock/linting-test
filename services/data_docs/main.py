import logging

from flask import Flask, render_template, request
from google.cloud import storage

logging.basicConfig(
    level=logging.INFO,
    format="%(name)s:%(levelname)s-%(message)s",
)
logger = logging.getLogger(__name__)

app = Flask(__name__)

CLOUD_STORAGE_BUCKET = "cbh-analytics-artifacts"

INDEX = "index.html"

GE_BASE_DIR = "ge/data_docs"
GE_DIRS = ["expectations", "static", "validations"]
GE_INDEX = f"{GE_BASE_DIR}/{INDEX}"

DBT_BASE_DIR = "dbt_docs"
DBT_INDEX = f"{DBT_BASE_DIR}/{INDEX}"


@app.route("/")
def index():
    return render_template("index.html")


@app.route("/ge", defaults={"path": GE_INDEX})
@app.route("/dbt", defaults={"path": DBT_INDEX})
@app.route("/<path:path>")
def data_docs(path: str):
    # parse the path and determine if you need to prefix GE or DBT
    referrer = request.referrer if request.referrer else ""
    is_ge_path = sum(val in path or val in referrer for val in GE_DIRS) >= 1

    full_path = f"{GE_BASE_DIR}/{path}" if is_ge_path else f"{DBT_BASE_DIR}/{path}"
    full_path = full_path if GE_INDEX not in path and DBT_INDEX not in path else path

    gcs_client = storage.Client()
    bucket = gcs_client.get_bucket(CLOUD_STORAGE_BUCKET)
    try:
        blob = bucket.get_blob(full_path)
        content = blob.download_as_string()
        if blob.content_encoding:
            resource = content.decode(blob.content_encoding)
        else:
            resource = content
    except Exception as e:
        logger.exception(f"Could not download blob. Error: {e}")
        resource = "<p></p>"
    return resource


@app.errorhandler(500)
def server_error(e):
    logger.exception("An error occured during the request.")
    return (
        f"""
        An internal error occured: <pre>{e}</pre>
        See logs for full stacktrace.
        """,
        500,
    )


if __name__ == "__main__":
    # This is used when running locally. Gunicorn is used to run the
    # application on Google App Engine. See entrypoint in app.yaml.
    app.run(host="127.0.0.1", port=8080, debug=True)
