# dbt
Analytic transformations used to power data products.

## Contents
- [Background Reading](#background-reading)
- [Local Setup and Installation](#local-setup-and-installation)
- [See dbt Documentation](#see-dbt-documentation)
- [Development Workflow](#development-workflow)
- [Data and Seeds](#data-and-seeds)
- [Tests](#tests)
- [Exposures](#exposures)

## Background Reading
- [What is dbt](https://dbt.readme.io/docs/overview)?
- Read the [dbt viewpoint](https://dbt.readme.io/docs/viewpoint)
- [dbt Workflow Guide](https://docs.google.com/document/d/1YeQXECo7gAKeg-yEFuP-boHs3RMwmyWDyQiP1ip6mlM/edit?usp=sharing)
- [dbt at Cityblock (June 2019)](https://docs.google.com/presentation/d/1ImWCOAH7hvW7R27aIjaTQ1RFqU4ZVNrR3aCGCzsjLvs/edit#slide=id.p)

## Local Setup and Installation
Use whichever Python environment you prefer, we have used Python 3.6. The following instructions are for setting up one from scratch, we will use `pyenv` in order to get `dbt` up and running. 

### Updates

#### If you're running: macOS Catalina
**Updated**: April 2020

After upgrading to Catalina, multiple dbt developers have experienced workflow disruptions.

Here's what we've learned, and what we'd recommend watching out for when you upgrade.

- **Terminal shell:** Catalina changes the default interactive shell from bash to zsh, and you may notice a message to this effect when you open your shell. For the moment, we would recommend not changing your shell to zsh, as it has caused problems with loading the dbt virtual environment. If for some reason you have changed your shell or are using zsh, you can change it to bash by executing `chsh -s /bin/bash`.

- **BigQuery SSL:** Catalina also seems to disrupt the connection between local Python environments and BigQuery, which is required to execute local `dbt` runs. If you receive an SSL error upon attempting a run, follow the steps outlined in [this article](https://dbaontap.com/2019/11/11/python-abort-trap-6-fix-after-catalina-update/). Shout out to dbt support for their help here!

#### If you're running: macOS Big Sur, Apple M1 chip
**Updated**: January 2021

If you are using a newer Macbook with an Apple M1 processor and running Big Sur, additional steps should be taken to get `dbt` and your Python environment up and running on your machine. Please check which processor and OS you are using before proceeding. 

The short version of the issue is that the list of acceptable architectures to install Python environments are hardcoded to Intel and PowerPC but not Apple processors. Therefore, `arch` checks on Macs running with Apple Silicon CPUs will fail. 

A Python bug report and patch exists for Python 3.8+ but has yet to be released for this update. For more information and/or to check the status of the bug, see [documentation here](https://bugs.python.org/issue41100).

The intermediate solution for this is to run everything in x86_64 mode. To do this, you can do one of the following:
- Prepend every command with `arch -x86_64`
- **Set up a Terminal to run all commands under Rosetta**

These instructions are based on the second option, to set up your Terminal to run all commands under Rosetta. 
    
   > M1: Set up instructions specific to this process will appear like this below.

### Installation

From your Terminal, run the below commands one line at a time.

1) Install [Homebrew](https://brew.sh/) if you haven't already. 
 
   > M1: Before installing Homebrew, navigate to your Utilities folder via Finder (Finder > Applications > Utilities) and locate your Terminal. Duplicate the Terminal (right click > Duplicate) and name the new Terminal "Terminal_x86_64". Right click on this new Terminal and select Get Info. In the window that appears, under General check the box that says Open using Rosetta. This Terminal is now set to run using Rosetta. If your shell is zsh, type in `chsh -s /bin/bash` to change your default shell to bash.

   > Install the x86_64 version of Homebrew by running the following via your Terminal.
    ```bash
     arch -x86_64 /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/master/install.sh)"
    ```
   > Add Homebrew to your $PATH and then restart your Terminal. 
    ```bash
     echo 'eval $(/opt/homebrew/bin/brew shellenv)' >> $HOME/.bash_profile
    ```
   > Once Homebrew is installed, ensure that you are running the x86_64 version of brew from `/usr/local/bin/brew`.
    ```bash
     $ which brew
     /usr/local/bin/brew
    ```
    
2) Install `pyenv`, `pyenv-virtualenv`, and `zlib`.
   ```bash
    brew update
    brew install pyenv pyenv-virtualenv zlib
   ```
   
   > M1: Install the same build dependencies by running the following: 
    ```bash
     arch -x86_64 brew install pyenv pyenv-virtualenv zlib
    ```  
    
3) Update your `~/.bash_profile` with `zlib` flags.
    ```bash
    echo 'export LDFLAGS="-L/usr/local/opt/zlib/lib"' >> ~/.bash_profile
    echo 'export CPPFLAGS="-I/usr/local/opt/zlib/include"' >> ~/.bash_profile
    source ~/.bash_profile
    ```
    
4) Update your `~/.bash_profile` to auto initialize `pyenv` and `pyenv virtualenv`.
    ```bash
    echo -e 'if command -v pyenv 1>/dev/null 2>&1; then\n  eval "$(pyenv init -)"\nfi' >> ~/.bash_profile
    echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bash_profile
    source ~/.bash_profile
    ```
    
5) Update your `~/.bash_profile` with the relevant environment variable for writing to our projects. (Your local variable should be a sandbox, production will be the actual project name). Replace `"cbh-your-sandbox"` with your sandbox name (example: `"cbh-firstname-lastname"`).
    ```bash
    echo 'export CITYBLOCK_ANALYTICS="cbh-your-sandbox"' >> ~/.bash_profile
    source ~/.bash_profile
    ```
    Please Note: If you do not yet have a sandbox set up in BigQuery, follow the [instructions here](https://github.com/cityblock/mixer/blob/master/terraform/personal-projects/README.md) to create a pull request (PR) and post the PR to the #platform-support Slack channel.  

6) Create your Python environment (we will use Python 3.6.5).
    ```bash
    pyenv install 3.6.5
    pyenv virtualenv 3.6.5 env-dbt
    ```
    
    > M1: To install your Python 3.6.5 environment via `pyenv`, run the following: 
     ```bash
     CFLAGS="-I$(brew --prefix openssl)/include -I$(brew --prefix bzip2)/include -I$(brew --prefix readline)/include -I$(xcrun --show-sdk-path)/usr/include"
     LDFLAGS="-L$(brew --prefix openssl)/lib -L$(brew --prefix readline)/lib -L$(brew --prefix zlib)/lib -L$(brew --prefix bzip2)/lib" \
     pyenv install --patch 3.6.5 < <(curl -sSL https://github.com/python/cpython/commit/8ea6353.patch\?full_index\=1)
      ```
    > If you get an error like "Error: /usr/local/opt/zlib is not a valid keg", this is likely due to an error in installing the libraries by Homebrew. This should be resolved by uninstalling and reinstalling `zlib` then trying to install via `pyenv` again.
     ```bash
     brew uninstall zlib && brew install zlib
     ```
    
7) Activate your Python environment and install `dbt`. Pandas is also required to run the computed fields script [here](./scripts/generate_computed_fields.py).
    ```bash
    pyenv activate env-dbt
    pip install --upgrade pip setuptools
    pip install dbt==0.19.1 pandas==1.0.1
    ```
    
9) Download and install [Google Cloud SDK](https://cloud.google.com/sdk/docs/install) to your home directory. Then run the following code and be sure to select "Y" when prompted "Modify profile to update your $PATH and enable bash completion? (Y/N)" and leave the $PATH as is when prompted to select a $PATH. Then restart your Terminal. 
   ```bash
   ./google-cloud-sdk/install.sh
   ```
    > M1: Use the x86_64 version. 

8) Make sure your `gcloud` command-line installation has the appropriate API scopes for running `dbt`. This will allow you to run models that query files in Google Drive.
    ```bash
    gcloud auth application-default login \
    --scopes=https://www.googleapis.com/auth/userinfo.email,\
    https://www.googleapis.com/auth/cloud-platform,\
    https://www.googleapis.com/auth/drive.readonly
    ```
   Your web browser will open and ask you to authorize the Google Cloud SDK to access several GSuite services.
   
   Please Note: If you are still not authorized, run the below command:
    ```bash
    brew cask install google-cloud-sdk
    ```
    And redo step 7 again 

9) Make sure you have a dev setting with your BigQuery sandbox configured in `~/.dbt/profiles.yml` (use `cmd+shift+.` to see hidden directories/files which start with a `.` or `ls -la` to list them). 
**_You may need to create the file and `~/.dbt` directory manually if the `dbt` installation did not do it for you._** 

   To create the `dbt` directory and the `profiles.yml` file, do the following: 
   - Starting from home, make the dbt directory: `mkdir .dbt`
   - Go into that directory: `cd .dbt`
   - Create the profiles.yml file: `touch profiles.yml`
   - Once the file is created, you'll need to add the dev settings into it. Copy and edit lines 6-16 from [sample-profiles.yml](./sample-profiles.yml) with the relevant information in this file for what our configurations should be. In your Terminal, type `vi profiles.yml` to pull up the contents of the file and type `cc` to allow you to edit it. Once you have finished, hit the `Esc` button and then `:wqa` to save and quit. 

10) Go into BigQuery and create a dataset in your sandbox project named `dbt` (done for saving our audit trail).

11) If you haven't already, clone the `mixer` repo. 

    Please Note: if you have two-factor authentication (as you should) for GitHub, you will want to create a personal access token. Instructions for doing this can be found [here](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token). 

12) Now you should be able to navigate to `/mixer/dbt/` and run the relevant dbt commands.
    ```bash
    dbt deps && dbt seed && dbt run
    ```
    

## Development Workflow

The current workflow would be the following:
1) Ensure your local environment is set up [(see above)](#local-setup-and-installation)
1) Create branch off of `master`
1) Make appropriate changes to `dbt` (SQL changes, models configuration, etc.)
1) On the terminal go to the `mixer/dbt` directory and run the `dbt` commands on
 your project:
    ```
    dbt deps && dbt seed && dbt run
    ```
1) Occasionally you may also need to update the [snapshots](https://docs.getdbt.com/docs/snapshots)
 within our dbt/snapshots folder. This should only be used to log changes in dimensions that
 we are not already capturing in Commons (example- User Role).
 Its important to run our snapshots before we run the dbt run,
 as some files within the dbt run may use snapshots as a source.
 Our nightly job in Airflow already runs snapshots prior to running our nightly tables,
 but if you need to manually create a snapshot, use:
    ```
    dbt snapshot
    ```
1) Once confirming everything looks good (can check manually on BigQuery on the console), open a PR and add the `dbt` label. 
    - Add `&template=dbt_changes.md` to the new pull request URL to populate a dbt-specific template.

For more on the dbt Release Process, check out the playbook [here](https://github.com/cityblock/mixer/blob/master/dbt/playbooks/dbt_release_process.md).

## See dbt Documentation
[dbt docs are generated](https://docs.getdbt.com/docs/running-a-dbt-project/command-line-interface/cmd-docs)
every time the dbt code gets merged to `master` via a Cloud Build Trigger. In order to access these docs, 
open your terminal and go into the `scripts` directory. You can then run:
```
# cd into scripts
./see_docs.sh
```
All the files necessary for the docs generation will be synced to your computer and then served on your default
web browser. 

Run this script to get the latest available docs.

## Data and Seeds

The [`Seeds`](https://docs.getdbt.com/docs/building-a-dbt-project/seeds/) functionality in dbt allows developers to load version-controlled csv files into a data warehouse from the command line. 

We store these files in project folders within `/data/`, and use our `dbt_project.yml` file to determine the destination datasets for these files. When creating a new folder, prefix the destination dataset with `dta_`. For example, the `seeds: panel_mgmt` section of `dbt_project.yml` should direct csv files in `/data/panel_mgmt/` to a dataset named `dta_panel_mgmt`.

To load files from `/dbt/data/` to BigQuery, execute `dbt seed`, supplying the `--select` argument to only load specific files. For example, `dbt seed --select next_step_hierarchy` would only load `/panel_mgmt/next_step_hierarchy.csv` to its destination in BigQuery.

## Tests

dbt [tests](https://docs.getdbt.com/docs/building-a-dbt-project/testing-and-documentation/testing) implement checks on models within a project.

There are two types of dbt tests: schema tests and custom tests. Schema tests validate whether a model meets certain basic expectations. For example, is the table unique at the intended grain? Are all values of a particular column not null? Apply schema tests to a column in the corresponding `schema.yml` file. Custom tests express more sophisticated logic written as query over the model of interest. Create custom tests with a .sql file in `/dbt/tests/`.

Custom tests can get disorganised, and do not have the advantage schema tests have of serving a documentation purpose. It is also generally not preferred to have tests in multiple places. For this reason, if you find yourself wanting to write a custom test, first ask yourself the following question:
_Does the test involve one or multiple tables?_
   * **one**: consider [writing a macro](https://discourse.getdbt.com/t/examples-of-custom-schema-tests/181) for the test instead and using this as a schema test
   * **multiple**: consider whether the query should really be a data model, for which a schema test would then be written

dbt also maintains a set of custom schema tests in [dbt utils](https://github.com/fishtown-analytics/dbt-utils#schema-tests) which can be used for more complex test cases.

dbt offers two test severity levels: `error` and `warn`. The default severity level is `error`, which will result in a failure. Lowering the severity level to `warn` results only in a warning message. Keep this in mind when applying tests to models, as both our [nightly dag](https://github.com/cityblock/mixer/blob/master/cloud_composer/dags/dbt_nightly_v3.py) and [staging cloudbuild](https://github.com/cityblock/mixer/blob/master/dbt/cloudbuild.staging.yaml) now include a `dbt test` step.

### Example schema test
The table `mrt_commons.member_care_team_all` has records for all historical assignments from members to care teams. It has the following columns: `userId`, `patientId`, `firstName`, `lastName`, `careTeamMemberAssignedAt`, `careTeamMemberAssignedUntil`, `userRole`, `userName`, `userEmail`, `isPrimaryContact`.

A set of tests that may make sense to add are:
* Not nullable columns: all except `careTeamMemberAssignedUntil` (which will be `null` for the current assignment)
* `isPrimaryContact` and `userRole` each have a well-defined set of possible values: first is a boolean and the second can only be one of the currently defined roles at Cityblock. The first one should throw an error and the second one just a warning (since a new role could have been added). 
* We could go fancy and demand `userEmail` be a valid email address (not implemented yet)
* `userId` and `patientId` should not contain any values that do not appear in the `user` and `member` tables, respectively
* `careTeamMemberAssignedUntil`, if it isn't `null`, should always come after `careTeamMemberAssignedAt`. _Note that this is a test involving multiple columns._

Here is the implementation of the tests
```
- name: member_care_team_all
    description: "\n
    Author- Marianne Hoogeveen\n
    Purpose- Lists all users that have at any time been part of a member's care team."
    tests:
      - dbt_utils.expression_is_true:
          expression: "careTeamMemberAssignedUntil > careTeamMemberAssignedAt"
          condition: "careTeamMemberAssignedUntil is not null"
    columns:
      - name: userId
        tests:
          - not_null
          - relationships:
              to: ref('user')
              field: userId
      - name: patientId
        tests:
          - not_null
          - relationships:
              to: ref('member')
              field: patientId
      - name: firstName
        tests:
          - not_null
      - name: lastName
        tests:
          - not_null
      - name: careTeamMemberAssignedAt
        tests:
          - not_null
      - name: careTeamMemberAssignedUntil
      - name: userRole
        tests:
          - not_null
          - accepted_values:
              values: ['Care_Guide', 'Community_Health_Partner', 'Hub_RN', 'Physician', 'Back_Office_Admin', 'Nurse_Care_Manager', 'Nurse_Practitioner', 'Community_Connector', 'Outreach_Specialist', 'Hub_Care_Coordinator', 'Hub_Operations_Manager', 'MSC_Nurse_Care_Manager', 'Member_Services_Manager', 'Member_Experience_Advocate', 'Behavioral_Health_Specialist', 'Community_Engagement_Manager', 'Certified_Nurse_Midwife', 'Lead_Nurse_Care_Manager', 'Lead_Community_Health_Partner', 'Community_Health_and_Outreach_Manager', 'Pharmacist', 'Psychiatrist', 'Physician_Assistant', 'Clinical_Operations_Lead', 'MSC_Community_Health_Partner', 'Lead_Outreach_Specialist', 'Member_Services_Advocate']
              severity: warn
      - name: userName
        tests:
          - not_null
      - name: userEmail
        tests:
          - not_null
      - name: isPrimaryContact
        tests:
          - not_null
          - accepted_values:
              values: [true, false]
              quote: false
```

Note that you can also add a description to each column, which for most columns you should do, especially if it is a new field you are defining.

## Exposures

dbt [exposures](https://docs.getdbt.com/docs/building-a-dbt-project/exposures/) provide a way to identify downstream users of our dbt models, such as Looker views, BigQuery sheets, and Python scripts. By listing these dependencies, we'll be able to better protect them as the models they use evolve.

If you create or modify a model that will be used outside of dbt, add the external consumer in an `exposures.yml` file within the same folder. While `name`, `type`, and `owner` are the only required properties, please include a `url` to your exposure, as well as the primary model it `depends_on`.

### Exposure types

dbt currently supports a limited number of `type` values: `dashboard`, `notebook`, `analysis`, `ml`, and `application`. Here is how we will use each.
- `dashboard`: exposures in Looker and Google Sheets
- `notebook`: not currently used; we recommend using an ad hoc query instead
- `analysis`: used sparingly; exposures ingested by consumers not listed here
- `ml`: exposures ingested by the Data Science team 
- `application`: exposures ingested by Commons

### Example exposure

Imagine you're creating a dbt model that will serve as the base view of a Looker Explore. If your model's folder does not already contain an `exposures.yml` file, create one and add an entry like the one below. Otherwise, add a similar entry to the existing file.

Keep in mind that a dbt model may have multiple exposures. For example, one model may support both a Looker view and a machine learning model. Within any `exposures.yml` file, entries should be ordered alphabetically by exposure `name`.

```
  - name: my_new_exposure                    # must be unique among our project's exposures
    type: dashboard
    maturity: low                            # optional; use at your discretion
    url: https://path.to/my.exposure
    description: >
      A description of my exposure

    depends_on:
      - ref('my_new_dbt_model')

    owner:
      name: My Name
      email: my.name@cityblock.com
```
