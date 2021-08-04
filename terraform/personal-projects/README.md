# Personal 🍕 Projects

At Cityblock, every person working with data (in BigQuery or other Google Cloud products) is
allocated a personal project that acts as a sandbox. This sandbox provides an isolated space for
private data, where you the user are in control of sharing and can experiment with Google Cloud
services freely. It also allows us to set quotas and measure spending across the organization.

## What you get!

- A Google Cloud project with the name "First Last's Sandbox" and the id `cbh-first-last`
- A Google Cloud storage bucket in your project called `gs://cbh-first-last-scratch`
- The ability to run BigQuery queries in the UI with your personal project selected in the dropdown
  menu.
- The ability to run BigQuery queries from external tools (e.g., command-line,
  [dbt](../../dbt/README.md)) with the project set to your project id (`cbh-first-last`)

## Setup

Like other Google Cloud resources, we provision personal projects with `terraform`. To create your
personal project, you will make new file with name first-last.tf containing
```terraform
   module "first_last_personal_project" {
       source          = "./base"
       person_name     = "First Last"
       person_email    = "first.last@cityblock.com"
       billing_account = var.billing_account
       folder_id       = google_folder.personal_projects.id
   }
```
and submit a pull request, either in command line or using the below instructions.

### Submitting a pull request via the GitHub UI

1. Follow [this link](https://github.com/cityblock/mixer/new/master/terraform/personal-projects) to
   create a personal project terraform file.
1. Name the file like this: `first-last.tf`.
   - Your first and last name should be lowercase.
   - The filename needs to end with `.tf` or else `terraform` won't recognize your changes.
1. Copy the following into the "Edit new file" text box
   ```terraform
   module "first_last_personal_project" {
       source          = "./base"
       person_name     = "First Last"
       person_email    = "first.last@cityblock.com"
       billing_account = var.billing_account
       folder_id       = google_folder.personal_projects.id
   }
   ```
   and replace the three instances of "first last" with your name.
   - Note that `person_email` should be your Cityblock e-mail (which in rare cases is not `first.last@cityblock.com`).
1. Scroll to the bottom of the page where there is a section titled "Commit new file."
1. Select the radio button titled "Create a new branch for this commit and start a pull-request"
1. In the text box, enter `first-last-personal-project` (replacing `first-last` with your name).
1. Click the green button labeled "Propose new file".
1. You should now be on a page called "Open a pull request". Click the green button labeled "Create
   pull request".
1. Post the link to your pull request in the #dmg-and-services-support slack channel. A platform engineer
   will review the request and create your project!.

## Gotchas

### When I run queries in the BigQuery Web UI I get a permissions error!
This is probably because you don't have your personal project selected in the project dropdown. 

If you do have your personal project selected, you may be lacking read or write permissions. Visit
the #dmg-and-services-support slack channel for permissions help!
