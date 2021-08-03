# Terraform

Terraform is a tool for building, changing, and versioning infrastructure safely and efficiently. Configuration files are used to describe to Terraform
the components needed to run a single application or your entire datacenter. 

The infrastructure Terraform can manage includes low-level components such as compute instances, storage, and networking, as well as high-level components
such as DNS entries, SaaS features, etc.

[You can find our terraform folder here](../../terraform/README.md)

## Preface

### Building your own project workspace in GCP

The first step towards building our own mini data platform starts with creating a workspace for us to work in GCP. In GCP, this would be structured in the 
form of a [Project](https://cloud.google.com/storage/docs/projects). Projects can be structured to operate independently or as a group with
intercommunication for separate services. This chapter will teach you how to leverage terraform in order to create a project for yourself where you will
have owner rights and the freedom to test, deploy and experiment to your heart's content (Disclaimer: resources have a charge and although extremely cheap
should still be used responsibly, which means no to Bitcoin mining). Often, we use the term `personal project` interchangeably with `personal sandbox`.

At Cityblock, individuals such as engineers, data scientists and even PMs can request access to their own personal project. This starter project begins, 
with creating your own personal project to work with. This will also be your first commit into the Mixer Repo. In this chapter, we will not perform anything
too difficult in terraform, however, as we progress along the chapters, we will develop more muscles around using terraform.

**Note**: When possible, it is best to avoid storing PHI in your own personal project and to instead work with some form of scrubbed data.

### The state of the Terraform world

State is an important concept in terraform and we're going to take a quick step back to look at what state is, informally of course. When you make changes via
a config file and then apply those said changes, terraform then accordingly makes changes to the GCP infrastructure be it either by adding new permissions,
running new machines or simply updating table views in BigQuery. We will not go deep into the internals but you can for now take it granted that Terraform
manages the infrastructure auto-magically. Once all these changes are done, Terraform needs to preserve the state of the world that it sees (which one could
argue is not necessarily consistent with what is deployed in GCP) so that it may detect future changes (including deletions) and be able to reference resources
correctly. Terraform does this by creating a `terraform.tfstate` file which is a json file and stores all the current changes. Changes to the `.tfstate` file
can only be done by one user at a time, since this would lead to race conditions. This is done by creating a mutex by generating a `lock file` which prevents
more than a single user from making changes to the state. For more info on terraform, feel free to google terraform state, lock state and workspaces.

The `Master` branch on github reflects the state of our Google Cloud Environment and so all terraform requests should be made through a new pull request
and once merged into master, will be applied by a member of the data-engineering@cityblock.com group. There is plans of automating this but until then that
is the current workflow. Since the reader is assumed to be a member of the platform team, we will have an additional step explaining how to
implement changes via terraform. This will be marked as an optional section so that non-platform members may choose to skip over it.


## Steps

### 
To create your personal project, first checkout a new branch on git with the following command. Assuming your name is John Doe, you would want to run the
following command, where the first two letters in the branch name is your initials.

`git checkout -b jd/create-personal-project-mixer-john-doe`

Once done, navigate to `mixer/terraform` where you will create a new file under the `personal-projects` folder with the file name `john-doe.tf` 
Populate the empty file with:

```terraform
module "john_doe_personal_project" {
  source          = "./base"
  person_name     = "John Doe"
  person_email    = "john.doe@cityblock.com"
  billing_account = "${var.billing_account}"
  folder_id       = "${google_folder.personal_projects.id}"
}
```

For now, we will not explain much of what is happening and allow for most of the stuff to pass as hand wavy. If you are part of the
data-engineering@cityblock.com group, run the following command: 

***Note***: make sure you are (1) in the /mixer/terraform directory and (2) that you have authenticated yourself by running the command
`gcloud auth application-default login` mentioned in chapter 1 of this project

```bash
terraform init
terraform plan -target=module.personal-projects.module.john_doe_personal_project
```

***Note***: You will get asked for a few inputs via the command line (ex. var.sendgrid_api_key). For the purpose of this
personal project, it is not necessary, so you can put in fake values (e.g. value_1, value_2). For best practice, 
you'd want to create a terraform.tfvars file to store these values and the values of other secrets 
(which can be given to you by a member of the Platform team). 
As mentioned below in the "Some Notes" section, this file should never be committed.

Let us draw some attention to some of the contents of the output.
- Firstly, the last line of the output specifies how many resources will be created
- Secondly, this is a *plan* phase and so, no action will be taken in the construction, modification or destruction of these
objects.
- Thirdly, with the creation of our personal project other entities were also created. A personal gcs bucket (google cloud storage),
a BigQuery service and an IAM policy. For now, we can disregard how these extra entities were created when we only specified a project
and will re-visit this again in later chapters.

Once verfied that the contents of the file look good, save and commit the file and then send a pull request. A data engineer with review
your request and apply the changes you requested. 

Hooray! With that, you've completed your first terraform interaction. As you continue along the project, we will continue interacting
with terraform 



### Pull Request Process

Only the data-engineering@cityblock.com team should merge and apply terraform pull requests with the command `terraform apply`. Currently this isn't enforced
in GitHub, but we will try to do that soon. In the meantime, we apologize for the inconvenience. We are also revisiting our terraform infrastructure model
and so this portion of the code base will be evolving at a relatively high frequency.


### Some Notes:

- Sensitive info should be in the `.tfvars` file (credentials). `.tfvars` should never be committed 
(as specified in the [`.gitignore`](../../terraform/.gitignore) file).
- The terraform state is stored on GCS, and only data-engineering@cityblock.com has access to it. This provides locking and sharing of state across the org.

## Helpful Links

- [Google Cloud Terraform Provider](https://www.terraform.io/docs/providers/google/index.html)
