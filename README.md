# Mixer

## Entry-point to all things data
`mixer` contains a variety of projects relevant to the data team at Cityblock.
 Altogether, it can be thought of as the 'data pipeline' in our organization.

Each sub-project listed below can be thought of their own project, however, there are
 'soft' dependencies between them based on the context when adding features.
 This makes it ideal for them to be co-located in this manner.
 An example can be a [pull request such as this](https://github.com/cityblock/mixer/pull/307)
 that introduces new architectural features to our data pipeline.

### On-call playbooks

These subdirectories also include `playbooks` relevant to incident management 
 for the services within them.
 
We maintain a template for new playbooks [here](https://docs.google.com/document/d/1SuRszrXJz2qe5KRk0gZqZsr9S7Jn-R3X71o_scJeucw/edit).

### Personal projects

If you are new to `mixer` and need to create a personal project, head
straight to [personal-projects](terraform/personal-projects/README.md).

## Sub-projects

### [`cloud_composer`](cloud_composer/README.md)
[Airflow managed service](https://cloud.google.com/composer).
Contains DAG python files, documentation on how it is used, and how to add DAGs
 for data processing.

### [`cloud_functions`](cloud_functions/)
[Serverless applications](https://cloud.google.com/functions).
Contains lightweight applications that are geared towards HTTP or Google Cloud infrastructure scripts.

### [`containers`](containers/)
[Container Registry](https://cloud.google.com/container-registry/).
Contains code for our containers. These generally have more 'weight' than applications in
 `cloud_functions` and are deployed in specific use cases.

### [`dbt`](dbt/README.md)
Data orchestrations/transformations for Cityblock's SQL scripts. Majority of our analytics data warehouse will live here.

### [`diagrams`](diagrams/README.md)
[Diagram as code](https://diagrams.mingrammer.com/).
Contains diagrams for various types of flows (like data) in Cityblock tech architecture. Serves as documentation
 and is **not** automated.

### [`partner_configs`](partner_configs/)
Configuration files describing partner data, and how to work with it.

### [`scio-jobs`](scio-jobs/README.md)
[Scio Applications](https://spotify.github.io/scio/).
Scala API for [Apache Beam](https://beam.apache.org/) applications used for more complex
 data transformations. Also contains k8s YAML files for computed fields jobs.
 As the Language Stats bar at the top of the page suggests, these applications make up
 the bulk of `mixer`.

### [`spark`](spark/README.md)
[Spark](https://spark.apache.org/) development tools, jobs, and scripts.
Java execution engine for complex data transformations with high-quality Scala and Python
 APIs. Contains tools for developing and testing spark applications locally as well as running them
 on [Cloud Dataproc](https://cloud.google.com/dataproc).

### [`scripts`](scripts/README.md)
Catch-all for all sorts of scripts originated out of ad-hoc needs. Mostly in Ruby.

### [`sftp`](sftp/README.md)
SFTP drop service used to coordinate data delivery with our partners.

### [`terraform`](terraform/README.md)
[Infrastructure as Code](https://www.terraform.io/).
Contains a variety of modules for our primary infrastructure (Google Cloud Platform)
provisioning and altering. Was adopted after using GCP on console only for some time so not
all live infrastructure is reflected in code.

**See [Secret Management section](terraform/README.md/#secret-management) of the terraform readme to learn how we manage secrets.**
