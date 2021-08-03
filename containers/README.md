# Containers

## Table of Contents

* [Description](#description)
* [Deployment](#deployment)
* [Testing](#testing)
* [Python Environment Setup](#python-environment-setup)

## Description

This project contains the specifications for each container that we deploy to our
[container registry](https://console.cloud.google.com/gcr/images/cityblock-data?project=cityblock-data&authuser=0&organizationId=250790368607).
Each folder in this project should contain:
- a `Dockerfile` which provides a set of instructions on how each container should
  be built
- a `README` on the purpose of the container (i.e. which Airflow job is using this
  container)
- the set of scripts that can be run on the container

## Deployment

All of our deployment instructions can be found at the root level of this project
in the `cloudbuild.yaml` file. Each step specifies a docker build given: the build
command that we are executing, the directory under which we are building, and the
timeout on building the image.

The images field at the bottom of the config specifies all of the images that Cloud
Build should push to the container registry. Any images built but not specified in
this field will be discarded on build completion.

## Testing

While testing containers through Airflow is possible, it is cumbersome and takes
significantly longer than testing containers locally. To test locally, you need
to have the container in your local Docker environment. This can either be done
by pulling a remote image from GCR or by building the container locally from any
of the folders in this project. If pulling a remote image, make sure to run the
command `gcloud auth configure-docker` to authenticate to GCR.

Once the container exists locally, you can start the container by running:
```
docker run -it {CONTAINED_ID} /bin/sh
```
This start the container and load you into a running shell on the container at
the specified `WORKDIR` from the Dockerfile. You should then be able to test
whatever scripts exist on your container.

You may frequently need to test scripts that require access to tools on Google
Cloud (e.g. GCS, BigQuery, CloudSQL). You can mount the necessary credentials
upon container start through the following command:
```
// ABS_LOCAL_PATH = absolute local path for gsuite credentials
// ABS_CONTAINER_PATH = absolute path for putting credentials on container

docker run -it -v {ABS_LOCAL_PATH}:{ABS_CONTAINER_PATH} -e GOOGLE_APPLICATION_CREDENTIALS={ABS_CONTAINER_PATH} {CONTAINER_ID} /bin/sh
```

You may find it useful to use the credentials of a service account for more
thorough test of a container (say... the service account for an Airflow job
that will use said container). To test in this manner, do the following:
- navigate to the IAM & Admin panel where your service account exists on
  cloud console
- select the service account to test with
- hit the edit button and select the create key option at the bottom of the
  page
- mount the downloaded JSON credentials using the command above (i.e. replace
  `ABS_LOCAL_PATH` with the path to the downloaded credentials)
- delete the created key when done testing with the service account

## Python Environment Setup

- Install [Homebrew](https://brew.sh/) to manage packages for macOS.

- Download [pyenv via Homebrew](https://github.com/pyenv/pyenv#installation) to easily manage
  python versions on your macOS machine.

- Download [pyenv-virtualenv via Homebrew](https://github.com/pyenv/pyenv-virtualenv#installing-with-homebrew-for-macos-users)
  to easily manage python virtualenvs.

- Create a python virtual environment using the python version defined in
  [python-base/Dockerfile](./python-base/Dockerfile).
  So if the `python-base` python version is 3.6.8, you would run the below, substituting in your `container-name`:
   ```
   pyenv virtualenv 3.6.8 container-name-3.6.8
   ```

- `python-base` was created to standardize on frequently used packages and to facilitate efficient multi-stage
   docker builds.

- Set the newly created virtualenv as your default python environment when inside
  your `containers/container-name` directory:
  ```
  cd ~/mixer/containers/container-name
  pyenv local container-name-3.6.8
  ```

- VS Code is a popular editor to use for development, though you can use any of your choice.
  Follow instructions to [install VS Code and the python extension](https://code.visualstudio.com/docs/python/python-tutorial#_prerequisites).

- Set your [VS Code python interpreter](https://code.visualstudio.com/docs/python/python-tutorial#_select-a-python-interpreter) to be `pyenv-virtualenv container-name-3.6.8`:

- Install the [python-base/requirements.txt](./python-base/requirements.txt) in your newly activated virtualenv:

   ```
   pip install -r ./python-base/requirements.txt
   ```

- If needed, create a container specific `container-name/requirements.txt` to add / install additional packages.

- Though not enforced, engineer(s) on the team like to use [flake8](https://pypi.org/project/flake8/) for
  linting and [black](https://pypi.org/project/black/) for formatting. To move towards standardizing, install
  both in your virtualenv:
  ```
  pip install flake8 black
  ```

