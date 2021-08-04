# GCS Slack Notifier

This application receives events from Google Cloud Storage and publishes them to a Slack incoming webhook,
which is configured in `settings.json`.

## Development

To update the existing cloud function, you first need to decrypt `settings.json.enc`:

```
gcloud kms decrypt --ciphertext-file=settings.json.enc --plaintext-file=settings.json --location=global --keyring=cloud-function-secrets --key=deploy --project=cityblock-data
```

This will allow you to run the code locally using the functions emulator (installed with `npm install -g @google-cloud/functions-emulator`):

```
functions-emulator start
npm run deploy-local
```

Then, you can simulate events by running `functions-emulator call gcsPubsubSlack`.

## Deploying changes

Currently, this application is just deployed for the `cbh_sftp_drop` bucket. To update this deployment, make your changes, ensure the settings.json has been encrypted as described above, and run `tsc` to build the application. If the application builds successfully, you can deploy with `gcloud beta functions deploy gcsPubsubSlack --trigger-bucket="cbh_sftp_drop"`. You can also use this to deploy to other buckets as well, but they will all share the webhook in `settings.json`.

## Changing settings

If you need to change `settings.json`, you'll need to encrypt the changes so that they can be committed in git.
Encrypt with:

```
gcloud kms encrypt --ciphertext-file=settings.json.enc --plaintext-file=settings.json --location=global --keyring=cloud-function-secrets --key=deploy --project=cityblock-data
```
