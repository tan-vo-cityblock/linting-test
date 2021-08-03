# dbt coding conventions

This document codifies best practices for contributing to our dbt project, in order to promote collaboration and consistency across a growing group of developers.

It uses the Fishtown Analytics [conventions](https://github.com/fishtown-analytics/corp/blob/master/dbt_coding_conventions.md) as a starting point.

## Contents
- [Model naming](#model-naming)
- [Model configuration](#model-configuration)
- [Data modeling](#data-modeling)
- [Documentation and testing](#documentation-and-testing)
- [Naming and field conventions](#naming-and-field-conventions)
- [Model composition](#model-composition)
- [SQL style](#sql-style)
- [Example model](#example-model)

## Model naming
Our models fit into the categories described [here](https://github.com/cityblock/mixer/blob/master/dbt/models/README.md).

Within those categories, model names should:
* Begin with the relevant category prefix and subfolder
  * This makes it easy for a developer to know exactly where to find the model.
  * For example, model names within `abstractions/commons/` should begin with `abs_commons`.
* Be plural
  * After the category prefix, the remainder of a model name should fill in the sentence, "This model contains...".
  * For example, a model within `abstractions/commons/` containing events might be named `abs_commons_events`.

## Model configuration
Our project uses two dbt [model configurations](https://docs.getdbt.com/reference/model-configs/).

The first, [materializations](https://docs.getdbt.com/docs/building-a-dbt-project/building-models/materializations/), determines how models are created in the data warehouse (as tables, views, etc). The second, [tags](https://docs.getdbt.com/reference/resource-configs/tags/), determines how often models should be run, in combination with commands in orchestrated Airflow DAGs.

* Default configurations for a directory should be set in `dbt_project.yml`.
    * For example, if all models within `/abstractions/claims/` should be materialized as views, that directory's entry in `dbt_project.yml` would appear as:
```
claims:
  database: "{{ env_var('CITYBLOCK_ANALYTICS') }}"
  schema: abs_claims
  materialized: view
```

* If a model needs to use different configurations, they should be set at the top of the model file.
    * For example, if a new model within `/abstractions/claims/` needs to be materialized as a table, and included in a DAG that uses the `nightly` tag, the model's configurations would appear at the top of the model file as:
```
{{ config(materialized='table', tags=['nightly']) }}
```

* Marts should be materialized as tables.
    * By default, dbt models are materialized as views, which are faster to build, but slower to query.
    * However, because our marts are often queried by Looker, materializing them as tables improves performance.

## Data modeling
* Only `src_` models should select from source tables, using the `source()` function.
    * This helps us manage dependencies on those source tables.
    * If a source table changes, we'll be able to account for the change in a single `src_` model, rather than many models throughout the project.
* All other models should select only from other models, using the `ref()` function.

## Documentation and testing
Testing is discussed more thoroughly in the [tests](https://github.com/cityblock/mixer/blob/master/dbt/README.md#tests) section of the dbt README. The conventions here establish a baseline for how we want stakeholders and teammates to interact with our models, whether in code or through dbt Docs.

* Every subdirectory should contain a `schema.yml` file, in which each model is listed.
* At a minimum, every model entry should include a:
    * Model description
    * Primary key column with `unique` and `not_null` tests

## Naming and field conventions
Naming conventions are discussed more thoroughly in the [Naming Conventions of Columns](https://docs.google.com/document/d/1VJExICS3xIRm08kgQxNI_7aIcJy-gW7KQkD79IZ-nb8/edit#heading=h.35pt2ryn6zqg) section of our SQL Style Guide document.

* Schema (dataset) and model (table) names should be in `snake_case`. Column names should be in `camelCase`.
* Each model should have a primary key.
    * Models with multi-column keys should use the dbt-utils [surrogate_key](https://github.com/fishtown-analytics/dbt-utils#surrogate_key-source) macro to create a hashed `id`.
    * This column will often be created in the `final` CTE, as in the example below:
```
),

final as (

  select
    {{ dbt_utils.surrogate_key(['firstColumn', 'secondColumn', ..., 'nthColumn']) }} as id,
    *

  from earlier_cte

)

select * from final
```
* Names should be based on business terminology, rather than source terminology.
    * Keep in mind the audience for our dbt models, which includes not only developers, but also members of other teams who consume data in BigQuery or Looker.
* Within a model, fields should be ordered in categories, beginning with identifiers and ending with timestamps.
* Field names should reflect their data type.
    * Boolean fields should be prefixed with `is` or `has`.
        * Examples: `isConsented`, `hasPathwaySuggestion`
    * Date fields should be suffixed with `Date`.
        * Examples: `consentDate`, `eventDate`
    * Timestamp fields should be suffixed with `At`.
        * Examples: `createdAt`, `consentedAt`

## Model composition
Model composition is discussed more thoroughly in the [Ingest, Transform, Serve](https://docs.google.com/document/d/1VJExICS3xIRm08kgQxNI_7aIcJy-gW7KQkD79IZ-nb8/edit#heading=h.jxpcl0ilqtr5) section of our SQL Style Guide document.

* A model should select from other dbt models only once, in individual CTEs at the beginning of the model.
    * Any column renaming should happen within these CTEs.
    * The remainder of the model should select only from the initial set of CTEs.
* CTEs should perform a single unit of work.
    * Err on the side of a longer model broken into clear steps, as opposed to a condensed model whose logic is difficult to interpret.
* CTE names should reflect their contents, and be as verbose as needed.
    * Think of each CTE as a miniature model.
    * Name and order CTEs such that a reader could understand the entire model by reading through the CTE names alone.
* In CTEs containing joins, column names should be prefixed with a CTE alias.
    * This makes it easier for a reader to interpret the CTE from which a given field is being used.
    * In addition, it prevents errors due to ambiguous column names.
* A model should end by selecting all results from the final CTE.
    * The last line of code should be: `select * from [final_cte]`.
    * This makes it easier to test and debug a model, without commenting out the last statement.

## SQL style
* Model files should be indented using two spaces.
    * This matches the default setting in BigQuery, and allows for smoother development between environments.
    * Two-space indentation looks like this:
```
select
  firstColumn,
  secondColumn
 ...
```
* Field and function names should be in lowercase.
    * IDEs make lowercase SQL just as readable as uppercase, but with less aggression and fewer keystrokes.

## Example model
```

with event_names as (

  select
    id as eventId,
    name as eventName

  from {{ ref('events') }}

),

member_events as (

  select
    id as memberEventId,
    memberId,
    eventId,
    eventAt

  from {{ ref('member_events') }}

),

named_member_events as (

  select
    me.memberEventId,
    me.memberId,
    me.eventId,
    en.eventName,
    me.eventAt

  from member_events me

  left join event_names en
  using (eventId)

)

select * from named_member_events

```
