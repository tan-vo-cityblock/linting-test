
# Pubsub And Message Queues


### Installing Requirements to Connect to Pubsub and Google Cloud Storage via Python

In your terminal (once you've launched your virtual env) run: <br>

```bash
pip install --upgrade google-cloud-pubsub
pip install --upgrade google-cloud-storage
```

##### Goal
Provided a file in GCS (Google Cloud Storage), we wish to read the contents of that file via a CloudFunction and forward it to a Pubsub topic. We would also
like to inspect into the Pubsub stream and view the contents

## GCS Storage

###### Reference:
https://googleapis.github.io/google-cloud-python/latest/storage/client.html <br>
https://github.com/GoogleCloudPlatform/python-docs-samples/blob/master/storage/cloud-client/quickstart.py

In order to create a connection to GCS we can execute the following code:

```python
from google.cloud import storage
storage_client = storage.Client()
```

##### Optional
The above code uses your service account. If you wish to not do so, you may alternatively create one (via the UI) and run the below code: 

```python
from google.cloud import storage
from google.oauth2 import service_account

service_acct_filepath = <filename>

scopes = ["https://www.googleapis.com/auth/devstorage.full_control", 
          "https://www.googleapis.com/auth/devstorage.read_only",
          "https://www.googleapis.com/auth/devstorage.read_write"]

credentials = service_account.Credentials.from_service_account_file(filename=service_acct_filepath, scopes=scopes)
storage_client = storage.Client()
```

### Coding

Let us begin this section by trying to construct a function which will allow us to read the contents of a file located in GCS and output said file to the screen.
You can continue this example in either an Ipython terminal, a python terminal, a notebook or simply through your favourite editor.

```python

from google.cloud import storage
storage_client = storage.Client()

bucket_name = "<bucket-name-here>"
source_blob_name = "<the-remaining-file-path-to-the-blob-object-here>"

bucket = storage_client.get_bucket(bucket_name)
blob = bucket.blob(source_blob_name)

contents = blob.download_as_string()
print(contents.decode())

```


### Problem

Create a generator function which takes as input a bucket and a blob path, opens the file and yields the contents of a file.

##### Bonus:
How can this problem be optimized for large files? (No need to write code for this)

<br> <br>

## Pubsub

In order to create a connection to PubSub we can execute the following code:

```python
from google.cloud import pubsub_v1
publisher = pubsub_v1.PublisherClient()
```

### Coding

This section of code will be a little more obscure and anticipates that the user will be able to intuitively construct functions from the following code examples:

```python

project_id = # Enter the project belonging to the topic
topic_name = # Enter topic name

topic_path = publisher.topic_path(project_id, topic_name)

line = "hello world"

byteString = line.encode('utf-8')
publisher.publish(topic_path, byteString)
```

Pubsub allows you also to pass a key-value map that you may use to label the data. This allows performance benefits by allowing the user to not have to look into
the contents of the pubsub message or by providing accessibility to checksums. Read the docs on how to do this.

### Problem


- Create a function which populates messages to pubsub by taking as input a line and the topic to write to, and writes the input 

- Allow the function to publish to pubsub, the attributes `word_count`and `char_count` for every line in pubsub

<br> <br>

### Creating a Pubsub Topic via terraform

#### Reference:
https://www.terraform.io/docs/providers/google/r/pubsub_topic.html

Read the above documentation to create a pubsub.tf file in your personal project in terraform. Before committing to master, read the pubsub.tf in the solutions folder
and compare your solution for correctness.


### Constructing a Cloud Function:

Navigate to the [Cloud Functions](https://console.cloud.google.com/functions) page in your personal project and click the 
click the [Create Function](https://console.cloud.google.com/functions/add) button located on the bar. Name your function `sftpMsgReceiver`.

1. Select the `Runtime` as `python`
1. Select `trigger` as `Cloud Storage`
1. Choose `Event Type` as `Finalize/Create` 
1. Choose the scratch bucket we created from chapter 2, i.e. `cbh-<fullname>-scratch`

``

```python
def hello_gcs(event, context):
    """
    Triggered by a change to a Cloud Storage bucket.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    file = event
    print(f"Processing file: {file['name']}.")

    bucket_name = event['bucket']
    blob_name = event['name']
```

### Reading from the pubsub topic

There are several ways you can view the content in a Pub/Sub. You can use the UI or do so programmatically through languages such as python, javascript etc. We will use
a bash script to view the contents. Navigate to `src/pubsub.sh` and run the following commands:

```bash
./pubsub.sh -help
./pubsub.sh <topic-name> <subscription-name>
```

Now move a file to your bucket and you should be able to view the contents of the file populating to Pub/Sub.
