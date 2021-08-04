# Git guide

This document codifies best practices for contributing to the Mixer repository. Its purpose is to promote consistency across a growing team and reduce the number of decisions we make during routine workflows.

The content and format are heavily inspired by the Fishtown Analytics (makers of dbt) [guide](https://github.com/fishtown-analytics/corp/blob/master/git-guide.md).

## Contents
- [Branches](#branches)
- [Commits](#commits)
- [Pull requests](#pull-requests)
- [Post merge](#post-merge)

## Branches
Branch names should be lowercase, and follow a three-part convention: `name/topic/short-description`
* `name`: identifies the primary developer
	* We often use our first and last initials, but first names and nicknames are also fine.
	* Examples: `tp/`, `marianne/`, `dhop/`
* `topic`: identifies the main unit of work
	* This will usually be a Jira issue ID, which connects the branch to the issue.
	* For fixes that arise outside of Jira, use `fix` instead.
	* Examples: `/plat-123/`, `/fix/`
* `short-description`: briefly explains what is being done
	* Descriptions should be relatively short and separated by hyphens.
	* Examples: `/add-dc-quality-measures`, `/unify-phone-number-sources`

Based on these three parts, a full branch name might look like:
* `dp/do-456/delivery-model-outreach`
* `lauren/plat-789/update-insurance-details`
* `kclai/fix/nps-question-types`

## Commits
Commits should:
* Contain small, discrete steps within your larger project.
	* This makes it easier to revert your code, if needed.
* Have descriptive messages.
	* These will help your reviewer understand your process.
	* Ideally, your branch's commit history should read like a checklist of the steps required to accomplish your task.
* Be pushed early and often.
	* This promotes visibility across the team, and makes it easier to collaborate.

Commits may:
* Be squashed locally to combine several similar commits.

## Pull requests
Pull requests should:
* Accomplish a single task.
	* Similar to commits, a pull request should tackle a single piece of work.
	* This makes it easier for reviewers to provide detailed feedback, and makes it easier to revert our code, if needed.
* Include the Jira issue in their title.
	* Pull request titles should begin with the Jira issue in square brackets, then contain a short description of the task.
	* If a pull request implements a fix not captured in Jira, its title should begin with `[FIX]`.
	* Example titles:
		* `[DO-502] Create historical Cotiviti results model`
		* `[FIX] Account for new encounter types in interactions model`
* Tag a single reviewer, with at least 24 hours to review.
	* We take code review seriously, as a way to provide feedback that improves the quality of our code.
	* Giving your reviewer ample time to review a pull request allows them to give you the best possible feedback.
	* Read more about Cityblock's approach to code review in the document [here](https://docs.google.com/document/d/18v7LDkPO4t4FTwYNVlwXBnCBmpIWv-0phiVIqfEMoKc).
* Include the appropriate labels.
	* We use labels to indicate which [sub-projects](https://github.com/cityblock/mixer#sub-projects) within the repository a pull request applies to.
	* A few common labels are:
		* `airflow`: for files changed within `/cloud_composer/`
		* `dbt`: for files changed within `/dbt/`
		* `terraform`: for files changed within `/terraform/`
* Be merged by their author.
	* This gives the author the satisfaction of publishing their work, and allows them to manage any merge-timing dependencies.

Pull requests may:
* Use a template.
	* For `dbt` pull requests, please use the [dbt_changes](https://github.com/cityblock/mixer/blob/master/.github/PULL_REQUEST_TEMPLATE/dbt_changes.md) template. See [instructions](https://github.com/cityblock/mixer/tree/master/dbt#development-workflow) in the dbt README.

## Post merge
[TODO] Outline events that occur after merging code.
