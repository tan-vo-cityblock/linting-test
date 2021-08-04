# Production Deployment Notifier

## What this does

This Cloud Function is a subscriber to all messages of the `cloudbuild` topic in the `cityblock-data` project.
For the messages that ultimately pass the checks, they check published to a Slack channel
(right now [#eng-deployments](https://cityblockhealth.slack.com/archives/C01081PV8DC)
and [#dbt-alerts](https://cityblockhealth.slack.com/archives/C012WE5USF8)
)

## Deploying

This must be done manually when code changes are done, run the following command to do it:

```
cd <LOCATION_OF_MIXER>/mixer/cloud_functions/prod-deployment-notifier
gcloud functions deploy pubsub_to_slack --runtime python37 --trigger-topic cloud-builds --project cityblock-data
```

Please use the option `N` for allowing unauthenticated invocations
[See `gcloud` reference for more flags and their definitions](https://cloud.google.com/sdk/gcloud/reference/functions/deploy)

## Dependencies

The function currently uses [libraries that are already provided in the Python runtime](https://cloud.google.com/functions/docs/writing/specifying-dependencies-python#pre-installed_packages)
[There are also environment variable(s) that is put into the Console UI directly](https://console.cloud.google.com/functions/details/us-central1/cloudbuild-notifier?project=cityblock-data&tab=general)

## Testing

Unfortunately this was not tested in any meaningful way :(

As an enhancement, please [see this section on adding tests](https://cloud.google.com/functions/docs/testing/test-background#pubsub-triggered_functions)

## Debugging

Start at Google Cloud Logging - [here is a starting point for the log query](https://console.cloud.google.com/logs/query;query=resource.type%3D%22cloud_function%22%0Aresource.labels.function_name%3D%22pubsub_to_slack%22?project=cityblock-data)
