# Best Practices for Google Sheet Connected dbt Sources
Best practices for authors and developers

## What are Goole Sheet-connected sources, and how do I create one?

Google Sheet connected sources are those that originate from a Google Sheet. These can be used for a variety of different purposes at Cityblock, including code mappings that are too large for [seed files](https://docs.getdbt.com/docs/building-a-dbt-project/seeds) and sources that originate from non-technical colleagues who need the ability to input information into our data warehouse on an ad-hoc basis.

To implement a Sheet-based source, please first consider what other options may exist to meet your needs. If you have decided that a sheet-based source is the best option, follow the below steps to get started:

1. **Create Your Sheet-Based Source In Google Drive.**
	* Save it in an accessible location to others who may need to access it or run models based off of it.
	* An example of a good place is the [Data Product Drive.](https://drive.google.com/drive/folders/1c3-cKAPjjeqPjtu5idRY5uw_uVmp_fe2)
2. **Google Sheet Permissions**
*This is a data source, after all, and we want to avoid any inadvertent changes from being made to it.*
    * Once your sheet has been created and properly stored to allow others access, consider locking down your sheet so that only the right people can edit it.
    * You can also lock down tabs or ranges within your sheets so that only you or authorized users can edit them.
    * Another alternative is to warn users that editing a cell in a specific range may have consequences and they should proceed with caution.

3. **Share The New Source With Relevant Service Accounts**
		*This step is very important!*
	 * Be sure to share the document with the following accounts:
	     *  bq-data-access@cityblock.com
	     *  dbt-prod@cityblock-analytics.iam.gserviceaccount.com
	      * dbt-run-staging@cbh-analytics-staging.iam.gserviceaccount.com
5. **Data Validation**
    *In order to force a data schema in a Google Sheet, use Data Validation (accessed via Data > Data Validation) to set conditions for what data entered is acceptable in the sheet (e.g., so a date is entered in date format instead of text).*

6. **Add Your Sheet-Based Source to BigQuery**
    * The simplest way to do this is to add your sheet as an external table. To do this, you will begin with defining a schema for the table and linking it to the Google Sheet source.
	    * See [BigQuery’s documentation](https://cloud.google.com/bigquery/external-data-drive) here.

7. **Add your source to the dbt documentation.**
    *It's helpful for other people using models that are built off of or rely on a Google Sheet-connected source to be documented in dbt to help assist in any troubleshooting.*

## Who is responsible for ensuring access to Google Sheet-connected dbt sources?

All dbt developers (Data Analytics, Data Science, Data Adjuncts) are responsible for following the process if they have created a Google Sheet-connected dbt source.

## I'm getting a `BigQuery permission denied` error when trying to run a model that utilizes a sheet-based source. How do I find the connected sheet and troubleshoot?

If you're getting an error while trying to run a model that utilizes a sheet-connected source, take the following troubleshooting measures:

1. Identify the tables referenced in the model. You can do this by utilizing the *"Referenced By"* section or the *"Compiled Code"* of the dbt documentation.

2. Once you've identified the tables, search for them in BigQuery and look at the *"Details"* view under *"Source URI(s)"*. If it contains a link to a spreadsheet, you've found the culprit!

3. If you have access to the spreadsheet, open it and share with the accounts identified above. If you don't have access to the spreadsheet, post in the #data-analytics Slack channel and request that someone with access share the sheet with the accounts listed above.

5. Repeat the above steps for any other Google Sheet-connected sources in the model you are trying to run.
