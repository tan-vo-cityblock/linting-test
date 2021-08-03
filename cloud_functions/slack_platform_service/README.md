# Slack Platform Service

[Heavily inspired by GCP documentation](https://cloud.google.com/functions/docs/tutorials/slack)

## What this does

This Cloud Function is a URL that is called via the Platform Service app on Slack to do an assortment
of tasks. Currently it does the following:

- On call: Gets primary on call for a given team (Product, Platform, and Data Analytics)

## Deploying

This must be done manually when code changes are made (after merge to `master`), run the following command to do it:

```
cd <LOCATION_OF_MIXER>/mixer/cloud_functions/slack_platform_service
gcloud functions deploy slack_platform_service --runtime python37 --trigger-http --region us-east4 --project cityblock-data
```

Please use the option `y` for allowing unauthenticated invocations
[See `gcloud` reference for more flags and their definitions](https://cloud.google.com/sdk/gcloud/reference/functions/deploy)

## Dependencies

The function currently uses [libraries that are already provided in the Python runtime](https://cloud.google.com/functions/docs/writing/specifying-dependencies-python#pre-installed_packages)

[There are also environment variables that are maintained on Terraform](../../terraform/cityblock-data/cloud_functions.tf)

## Testing

Unfortunately this was not tested in any meaningful way :(

As an enhancement, please [see this section on adding tests](https://cloud.google.com/functions/docs/testing/test-http)

## Debugging

Start at Google Cloud Logging - [here is a starting point for the log query](https://console.cloud.google.com/logs/query;query=resource.type%3D%22cloud_function%22%20resource.labels.function_name%3D%22slack_platform_service%22%20resource.labels.region%3D%22us-east4%22?project=cityblock-data)
