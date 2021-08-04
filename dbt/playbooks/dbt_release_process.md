# dbt Release Process
A development-to-production workflow for all code relating to dbt

## What is this service, and what does it do?
The [dbt](https://www.getdbt.com/) Release Process includes three elements: the pull request template, the staging run, and the post-merge production run. Together, they ensure that our contributions are in sync with other workflows, free of errors, and quickly available. The goals of each element are described in more detail below.

**[Pull request template](https://github.com/cityblock/mixer/blob/master/.github/PULL_REQUEST_TEMPLATE/dbt_changes.md)**
- Link GitHub changes to the relevant Jira issue, to make it easy to look back on what we did, when, and why from either system.
- Mirror changes in Looker, to provide new content quickly for end users, protect existing content, and remove deprecated content.
- Promote [documentation](https://docs.google.com/document/d/1VJExICS3xIRm08kgQxNI_7aIcJy-gW7KQkD79IZ-nb8/edit#heading=h.7tcr8psbnir7) and [testing](https://docs.google.com/document/d/1fbZuuoB_8SQQFzkSuZsXUa9MPWenKO-QbcGhVXDH49k/edit), to help developers and stakeholders understand our content, and to validate that it produces expected results.

**[Staging run](https://github.com/cityblock/mixer/tree/master/dbt/cloud_build/staging)**
- Confirm that changed and downstream models run successfully and pass tests, to prevent failures in scheduled production runs.

**[Post-merge production run](https://github.com/cityblock/mixer/tree/master/dbt/cloud_build/prod)**
- Apply changes to affected and downstream models immediately, to unblock next steps and prevent failures in scheduled production runs.

## Who is responsible for it?
Katie (analytics engineering) and Tanjin (site reliability engineering) have developed the process and are responsible for maintaining it.

All dbt developers (data analytics, data science, data adjuncts) are responsible for following the process and providing feedback on further improvements.

## What dependencies does it have?
The **pull request template** depends on the author applying it, either by adding `&template=dbt_changes.md` to the end of their pull request URL before creating it, or by copying the Markdown directly at any time.

The **staging run** depends on the developer bringing their branch up-to-date with master before triggering the Cloud Build, by executing [scripts/models_changed.sh](https://github.com/cityblock/mixer/blob/master/dbt/scripts/models_changed.sh) and pushing resulting changes. The Cloud Build, triggered via a `/gcbrun` comment in the Pull Request, must complete within the [100-minute](https://github.com/cityblock/mixer/blob/master/dbt/cloud_build/staging/cloudbuild.yaml#L60) timeout limit.

The **post-merge production run** depends on changed and downstream models completing within [100 minutes](https://github.com/cityblock/mixer/blob/master/dbt/cloud_build/prod/cloudbuild.yaml#L66), and on tests passing with production data. In contrast to the staging run, the production Cloud Build begins automatically upon merging a branch, and is not dependent on a comment trigger.

## What does the infrastructure for it look like?
The pull request template is generated based on the [dbt_changes.md](https://github.com/cityblock/mixer/blob/master/.github/PULL_REQUEST_TEMPLATE/dbt_changes.md) file in the `mixer/.github/PULL_REQUEST_TEMPLATE/` folder

Staging and production Cloud Builds take place according to the contents of their respective `mixer/dbt/cloud_build/` [subfolders](https://github.com/cityblock/mixer/tree/master/dbt/cloud_build).

## What metrics and logs does it emit, and what do they mean?
Both staging and production runs generate logs on a “Build details” page within the [Build History](https://console.cloud.google.com/cloud-build/builds?project=cityblock-data) section of Cloud Build, on Google Cloud Platform. These logs track the step-by-step progress of a build, and list messages for any errors that may occur.

## What alerts are set up for it, and why?
Each production run generates two notifications in the [#data-notifications](https://cityblockhealth.slack.com/archives/C012WE5USF8) Slack channel: one when the run has been queued, and one when the run either succeeds or fails. These notifications inform the developer when their changes are available in BigQuery, or if an error has occurred.

## Tips from the authors and pros
The pull request template works well in tandem with GitHub’s draft feature. In the dropdown menu to the right of “Create pull request”, select “Create draft pull request”.  Work through the template’s checklist at your own pace, then click “Ready for review” and add a reviewer when you’ve completed it.

Both Cloud Builds are blind to changes to dbt macros and variables. The `models_changed.sh` script [identifies .sql files that differ from master](https://github.com/cityblock/mixer/blob/master/dbt/scripts/models_changed.sh#L10) and supplies them to the `dbt run --models` command. However, dbt [model selection syntax](https://docs.getdbt.com/reference/model-selection-syntax/) does not currently provide a way to run models that use a particular macro or variable. In these cases, consider a local run of affected models.

## What to do in case of failure
### Pull request template won’t populate
Make sure the URL follows this pattern:
https://github.com/cityblock/mixer/compare/[branch-name]?expand=1&template=dbt_changes.md

### Staging run fails due to unrelated models
A staging run may have been triggered from a branch behind master, writing old versions of models to the staging environment. Identify the upstream model(s) causing the failure and reset them with a local run from your branch. This might look something like:

`CITYBLOCK_ANALYTICS=cbh-analytics-staging dbt run -m troublesome_model_name+`

### Production run fails
When a production run fails, the Cloud Build Notifier app posts a "Build for dbt-prod-run was FAILURE" message in the [#data-notifications](https://cityblockhealth.slack.com/archives/C012WE5USF8) Slack channel. Click the "See build logs here" link to find more information about the error(s) causing the failure.
