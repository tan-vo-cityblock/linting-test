# Draft.js Transformer

**Limitations**
- The write project must be the same as the read project, avoid running this in production unless you can't help it!

## Running Locally
- Ensure all dependencies are installed via `npm install`
- Run command as follows:
  ```
  npm run tsc
  npm run transform $GCP_PROJECT $READ_DATASET $READ_TABLE $GCS_BUCKET $WRITE_DATASET $WRITE_TABLE $LOCAL_WRITE_PATH
  ```

## Running on Docker
- Build the image:
  ```
  # cd into this directory
  docker build -t draftjs:dev .
  ```
- Run it with appropriate args:
  ```
  GCP_PROJECT=...
  READ_DATASET=...
  READ_TABLE=...
  GCS_BUCKET=...
  WRITE_DATASET=...
  WRITE_TABLE=...
  LOCAL_WRITE_PATH=...
  docker run -it -v $HOME/.config/gcloud:/root/.config/gcloud \
    draftjs:dev \
    npm run transform $GCP_PROJECT $READ_DATASET $READ_TABLE $GCS_BUCKET $WRITE_DATASET $WRITE_TABLE $LOCAL_WRITE_PATH
  ```
