from google.cloud import storage
from google.cloud import pubsub_v1

storage_client = storage.Client()
publisher = pubsub_v1.PublisherClient()

def hello_gcs(event, context):
    """Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")

    pubsub_topic_name = "multiplexer"


    bucket_name = event['bucket']
    blob_name = event['name']

    for i in extract_contents_blob(bucket_name, blob_name):
        publish_to_pubsub(i, pubsub_topic_name)

def extract_contents_blob(bucket_name: str, source_blob_name: str):

    """Returns a generator that contains the contents of a file, which is new line delimited"""

    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(source_blob_name)

    contents = blob.download_as_string()

    for i in contents.decode().split("\n"):
        yield(i)

def publish_to_pubsub(line: str, topic_name: str):

    project_id = "test-env-haris"
    topic_path = publisher.topic_path(project_id, topic_name)

    len_words = str(len(line.split()))
    len_chars = str(len(line))

    byteString = line.encode('utf-8')
    publisher.publish(topic_path, byteString, wordCount=len_words, charCount=len_chars)