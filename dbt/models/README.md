{% docs __overview__ %}

## Data Documentation for Cityblock Health


## Models

All of our models within DBT fit into the following buckets

| Folder | Description | Dataset Prefix |
| ------ | ----------- | ----------- |
| sources | Bare minimum data transforms where necessary (casting, renaming mainly, some filtering) | src_, ref_ |
| abstractions | Building blocks we use over and over again | abs_, ftr_, agg_ |
| marts | End user focused tables around specific topics | mrt_ |
| exports | Queries ingested by external data products | xpt_ |

If your new model will create a new dataset in bigquery, add the prefix from the correct group to the name of the schema

{% enddocs %}
