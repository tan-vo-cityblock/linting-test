# Data Docs Service

Data Docs Service is a [standard App Engine application on the Python 3 runtime](https://cloud.google.com/appengine/docs/standard/python3/runtime) that lives in the `cityblock-analytics` GCloud project.

The app serves html data documentation from prod executions of our data tools [Great Expectations](https://docs.greatexpectations.io/en/latest/reference/core_concepts/data_docs.html) and [dbt](https://docs.getdbt.com/docs/building-a-dbt-project/documentation) at an internal [GCloud IAP](https://cloud.google.com/iap/docs/app-engine-quickstart#enabling_iap) protected site data-docs.cityblock.com.


## Table of Contents

* [Application](#application)
    * [GCP Resources - Prod](#gcp-resources---prod)
* [URL Endpoints](#url-endpoints)
    * [Access Permissions](#access-permissions)
* [Environment Setup](#environment-setup)
    * [Installations](#installations)
    * [Development Setup](#development-setup)
        * [Managing python packages](#managing-python-packages)
    * [Setup Issues and Fixes](#setup-issues-and-fixes)
        * [pipenv - bad interpreter](#pipenv---bad-interpreter)
        * [pipenv - issues with project's `venv`](#pipenv---issues-with-projects-venv)
* [Running Locally](#running-locally)
* [Application Logic](#application-logic)


## Application

### GCP Resources - Prod

_All resources managed in [terraform/cityblock-analytics/[data_docs_app_build_trigger.tf, dns.tf, iam.tf]](../../terraform/cityblock-analytics)._

_See [data_docs/cloudbuild.yaml](cloudbuild.yaml) for build / deploy steps._

- [Cloud Build - `data-docs-deploy-app`](https://console.cloud.google.com/cloud-build/triggers/edit/d8cff4b0-ba38-462d-92e3-26e71a1e6bf7?organizationId=250790368607&project=cityblock-data)

- [App Engine - `default`](https://console.cloud.google.com/appengine?serviceId=default&project=cityblock-analytics)

- [App Engine Error Reporting - `default`](https://console.cloud.google.com/errors?service=default&time=P1D&order=LAST_SEEN_DESC&resolution=OPEN&resolution=ACKNOWLEDGED&project=cityblock-analytics&organizationId=250790368607)

- [GCS Bucket That Feeds App: `cbh-analytics-artifacts`](https://console.cloud.google.com/storage/browser/cbh-analytics-artifacts;tab=objects?forceOnBucketsSortingFiltering=false&organizationId=250790368607&project=cityblock-analytics&prefix=&forceOnObjectsSortingFiltering=false)

- [Identity-Aware Proxy - Auth Layer for @cityblock gsuite users](https://console.cloud.google.com/security/iap?_ga=2.25145010.986274444.1615923822-806452415.1615923822&pli=1&project=cityblock-analytics&folder=&organizationId=&supportedpurview=project)

## URL Endpoints

- Homepage: [data-docs.cityblock.com](https://data-docs.cityblock.com)
- DBT Homepage: [data-docs.cityblock.com/dbt](https://data-docs.cityblock.com/dbt)
- GE Homepage: [data-docs.cityblock.com/ge](https://data-docs.cityblock.com/ge)

### Access Permissions

- Access permissions to get through the IAP layer and access the site are granted in Terraform module `module "iap_data_docs_access" {}` defined in [terraform/cityblock-analytics/iam.tf](../../terraform/cityblock-analytics/iam.tf).
- Simply append the gsuite group `group:some-group@cityblock.com` or gsuite user `user:some-user@cityblock.com` to the modules's member array.

## Environment Setup

_Note that Python setup here differs meaningfully from other python setup instructions that utilize `pyenv-virtualenv` (i.e see [/dbt/README](../../dbt/README.md#installation) or [/containers/README](../../containers/README#python-environment-setup))_

`pyenv-virtualenv` is good for quick / non-complex environment setup to prototype and/or run python scripts. 

For longer running applications, or production scripts, we should utilize `pipenv` as described below as it provides a more mature python package / dependency management system than the simple `requirement.txt`.


### Installations
- Install [Homebrew](https://brew.sh/) to manage packages for macOS.

- Download [pyenv via Homebrew](https://github.com/pyenv/pyenv#installation) to easily managage python versions on your macOS machine.
   - Make sure to follow the full install instructions telling you how to  update your shell config.

- Download [pipx via Homebrew](https://pipxproject.github.io/pipx/installation/) to easily managage global/system-wide end-user python applicatons.

    - Add the below `pipx` shell config to your `.zshrc`, `.bashrc`, or `bash_profile`, if not already there: 
       ```
       # path for pipx packages
       export PATH="$PATH:$HOME/.local/bin"
       ```

- Install [pipenv](https://pipenv.pypa.io/en/latest/#install-pipenv-today) (via `pipx`) to better manage this application's environment and packages:
   ```
   pipx install pipenv
   ```
   - Add the below `pipenv` shell config to your `.zshrc`, `.bashrc`, or `bash_profile`: 
       ```
        # pipenv config
        export PIPENV_VENV_IN_PROJECT=1
        export PIPENV_DEFAULT_PYTHON_VERSION=$(pyenv which python)
       ```
   - `pipenv` manages your python virtual environment in your project's `.venv` directory and replaces `pip` and `virtualenv`.
   - `pipenv` also replaces `requirements.txt` to better manage python app dependencies in a `Pipfile` and `Pipfile.lock`, similar to a node app's `package.json` and `package-lock.json`.
   - Highly encourage reading the [pipenv docs](https://pipenv.pypa.io/en/latest/#further-documentation-guides) for more.


### Development Setup
Once you have your environment set up, you will now need to install this applications python dependencies using [pipenv](https://pipenv.pypa.io/en/latest/#further-documentation-guides).

- Simply run the below to install development and prod packages defined in this app's [Pipfile](Pipfile).
    ```
    cd mixer/services/data_docs
    pipenv install --dev
    ```

#### Managing python packages
- If you need to add or update packages, simply use `pipenv` as you would use `pip` by running the below inside this app's directory, see [pipenv docs](https://pipenv.pypa.io/en/latest/install/#installing-packages-for-your-project) for more:
   ```
   pipenv install some_package
   pipenv update some_package
   ```
   - Add the `--dev` flag to the above to only install local development related packages (i.e. linters).
- This will automatically update the [Pipfile](Pipfile) and [Pipfile.lock](Pipfile.lock) and both should be committed to git.

- Alternatively, you can manually edit the `Pipfile` to add or pin packages. Then simply run `pip install --dev` to update your virtual env to match the `Pipfile`.
   - Without the `--dev` flag, the Pipefile's `[dev-packages]` section will be ignored.

- **Never** manually edit the `Pipfile.lock` file.

- See all [pipenv command flags](https://pipenv.pypa.io/en/latest/cli/#pipenv-install) for reference.

- Finally make sure the `python_version = x.x` defined in the [Pipfile](Pipfile) always matches the `runtime:pythonxx` version defined in the [app.yaml](app.yaml) so your development python version matches prod.


### Setup Issues and Fixes

#### pipenv - bad interpreter

- An error message containing `bad interpreter` when running `pipenv` commands may be due to Homebrew updating it's default python version, or an upgrade to the Homebrew installed `pipx` that manages `pipenv`.

- To fix simply re-install all `pipx` managed packages:
  ```
  pipx reinstall-all
  ```
- See [pipx docs](https://pipxproject.github.io/pipx/installation/#upgrade-pipx) for more.

#### pipenv - issues with project's `venv`
- If you are having other issues related to pipenv not being able resolve installed python package paths in the virtual env `services/data_docs/.venv` (i.e. because the `services/data_docs` directory was renamed)

- To fix, manually delete the `services/data_docs/.venv` directory or do so by running `pipenv --rm` in the project directory.

- Then re-run the `pipenv install --dev` command.

## Running Locally

- Activate the project's virtual env:
   ```
   pipenv shell
   ```

- Run the app:
   ```
   python main.py
   ```

- Visit the http://[host]:[port]/ displayed in the terminal output.

- When done, quit the application by pressing `CTRL+C`

- Finally exit the pipenv virtual env by running the command: `exit`.

## Application Logic

- Application entry point and logic lives in `main.py`.

- The app simply maps routes to paths in the [GCS bucket `cbh-analytics-artifacts`](https://console.cloud.google.com/storage/browser/cbh-analytics-artifacts;tab=objects?forceOnBucketsSortingFiltering=false&organizationId=250790368607&project=cityblock-analytics&prefix=&forceOnObjectsSortingFiltering=false) and loads the underlying html files at the GCS path.

- The initial home / landing page html is defined in the light-weight [templates/index.html](templates/index.html) and corresponding images are stored in the [static directory](static).
