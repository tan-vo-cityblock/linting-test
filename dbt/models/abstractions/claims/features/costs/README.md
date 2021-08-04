## Cost & Use Transformation

THIS IS OLD AND NOT UPDATED

## Why?
These abstractions provide sets of flags, groupings, and metrics core to
claims analysis.

## Code sructure
For each domain there will exist a series for models that encapsulate all of the
necessary data and bussines logic. At minimum each domain will have `%_base`
and `%_final` models.

- The `%_base` model will create the data structure that is to be used by all
  downstream models, including all references to upstream data sources and
  merging in any needed secondary sources.
- The `%_final` model will create the dataset that all downstream domains should
  reference (specifically other domains `%_base` models). This model should
  combine the base and all of the intermediate transformations into one
  final structure.

Between the base and final model there can be n number of intermediate
transfromations. Currently these types of transformations are

- `%_flags` which includes the creation of domain specific flags
- `%_buckets` which includes the create of domain specific cost buckets
- `%_stays` whcih includes the creation of grouped admissions called stays

### `%_base` models
The end result of a base model is a normalized data structure that can be used by
all downstream models for the given set of transformations. The `%_base` will
include defining the raw sources, merging and then transforming
(renaming, flattening, casting, ...) the data to fit the defined contract for the
process. An example base model would look like the below

```sql
select
    base.id as claimId,
    base.header.date.from as fromDate,
    base.header.date.to as toDate,
    drg_map.drg_mdc as mdc

from source('gold_claims', 'Facility') as base

left join source('ref', 'drg_codes') as drg_map
  on base.drg = drg_codes.drg_code
```

### Logic models (`%_flags`, `%_buckets`, ... )
Logic models contain all of the busines logic clustered around a certain type of
logic. All logic models have the name output api, a `claimId` column (which
is used to merge the data back onto the `%_base` model in the `%_final` model),
and a set of columns that contain the results of the busines logic. The end
result of a logic model would look like

```sql
select
    claimId,
    icuServiceFlag,
    edFlag,
    ...Flag
```

When merging the logic model onto the base dataset it should be a `left join`
on `claimId`.

### `%_final` models
Final models are the end result of a series for transformations. In this model
the base model should be joined with all of the results of the logic models. An
example final model would like the below.

```sql
select
    base.*,
    flags.* except (claimId),
    buckets.* except (claimId)

from base

left join flags
  on base.claimdId = flags.claimId

left join buckets
  on base.claimdId = buckets.claimId
```

## Business logic
Business logic is seporated by domain, with the domain containing all of the
logic (data source, bussines rules) specific to that domain. Domans have a
hierarchy, where a downstream domain can build off the logic of a previous
domain. Any logic that is to be shared by two domains should be stored at the
upstream domain both downstream domains share.

For example, the `snf` and `outpatient` domain both need the logic for flagging
a claim if it has a dialysis revenue code. As both domains need this (and it is
applicable to all facility), the flag in created in the `facility_flags` model.

Cost and use contains the following domains
  - `facility`
    - `inpatient`
    - `snf`
    - `outpatient`
  - `professional`

### Facility
Facility contains all of the logic specific to faciliy claims. The main data
source is the `gold_claims.Facility` source. This domain contains the following
models

  - `facility_base`
    - `facility_flags`
    - `facility_buckets`
  - `facility_final`

#### Inpatient
Facility contains all of the logic specific to inpatient faciliy claims. The main
data source is the `facility_final` model. This domain contains the following
models

  - `inpatient_base`
    - `inpatient_flags`
    - `inpatient_buckets`
    - `inpatient_stays`
  - `inpatient_final`

#### Skilled Nursing (snf)
Facility contains all of the logic specific to skilled nursing faciliy claims.
The main data source is the `facility_final` model. This domain contains the
following models

  - `snf_base`
    - `snf_buckets`
    - `snf_stays`
  - `snf_final`

#### Outpatient
Facility contains all of the logic specific to outpatient faciliy claims.
The main data source is the `facility_final` model. This domain contains the
following models

  - `outpatient_base`
    - `outpatient_buckets`
  - `outpatient_final`

### Professional
Professional contains all logic specific to professional claims.
This domain contains the following models:

  - `professional_base`


## Notes on the R version

Notes for the cost and use transformations in SQL

load the reference files
```
./bq_load_csvs 'gs://sc-ref/*.csv' ',' 'cbh-spencer-carrucciu' 'ref'
```

files reviewed:
1a
1b
2
3

separate into own modules:
- adding descriptions for code sets (icd, proc, rev, type of bill)
- removing duplicate claims
- removing denied claims
- identify transfers
- combine transfer info

steps:

inpatient claims (3)
- filter facility by type of bill (011%)
- merge drg codes map
- merge rev code map
- if mdc = "14", "15" then make inpt cat 2 (maternity)
- if inpt cat = "2", and mdc not in "14", "15" then 6 (acute)
- make any missing ones 7
- take the lowest ranked category as the claim category

acute (3a)
- filter for inpt cat = "1" or "6"
- create the admit from ed flag
- combine transfers (admission dates)
- assign a "cost bucket" category and sub-category based on icu flag
- calculate readmissions

maternity (3b)
- facility where inpt cat = "2" and mdc = "14"
- combine transfers (admission dates)
- combine transfer info
- assign a "cost bucket" category and sub-category

newborn (3c)
- facility where inpt cat = "2" and mdc = "15"
- combine transfers (admission dates)
- combine transfer info
- assign a "cost bucket" category and sub-category based on rev code

psych (3b)
- facility where inpt cat = "4"
- combine transfers (from dates) with transfer code 65 not 02
- combine transfer info
- assign a "cost bucket" category and sub-category based on icd diag code

rehab (3e)
- facility where inpt cat = "5"
- combine transfers (from dates) (with disch code 62 not 02)
- combine transfer info
- assign a "cost bucket" category and sub-category based on icd diag code

other (3f)
- facility where inpt cat = "7"
- assign a "cost bucket" category and sub-category

snf
- filter facility by type of bill (021%)
- remove snf dialysis
- flag subacute by reve code 019
- combine transfer (from date) (with disch code 03)
- combine transfer info
- assign a "cost bucket" category and sub-category based on rev code

snf dialysis
- filter facility by type of bill (021%)
- fitler by rev code 082|083

outpatient
- filter facility by type of bill (not in 011|021)
- add rev code hierarchy
- add cpt code hierarchy
- add obs flag
- take min of the two heirarchy
- get the lower est heirarchy, if there is none set to 8
- flag high part b drug spend
- rename the category for high cost drugs to 7

ed
- outpatient where cat = "1" and rev code is not 072%

outpatient surgery
- filter outpatient where cat = "2"
- create outpatient location column using rev code
- create cost bucket and sub bucket based on location

outpatient observation
- filter outpatietn where cat = "3"
- add cost bucket

outpatiet dialysis
- filter outpatient where cat = "4" and it contains the snf dialysis
- create cost bucket and sub bucket based on ub type of bill

outpatient home health
- filter outpatient where cat = "5" and ub tob in 031|032|033|034 and not equal to 013
- create cost bucket and sub bucket

outpatient bh
- filter outpatiet where cat = "6"
- create flags for Substance Abuse, Psych/BH, & Other BH using the primary diag code
- create methedone flag using procedure code
- create cost bucket and sub buckets based on the above flags

outpatient infusion
- filter outpatient where cat = "7"
- create cost bucket and sub buckets based on ub tob

outpatient other
- filter outpatient where cat = "8" or cat = "5" and ub tob 013 and ub tob not in 031|032|033|034
- flag outpatient clinic visits by procedure code
- flag physicial therapy based on rev code
- create cost bucket and sub buckets based on the above flags

outpatient lab
- filter outpatient where cat = "10"
- assign cost bucket and sub bucket

outpatient ambulance
- filter outpatient where cat = "11"
- flag emergent by procedure code
- create cost bucket and sub bucket based on the above flags

outpatient dme
- filter outpatient where cat = "12"
- assign a cost bucket

outpatient hospice
- fitler outpatient where cat = "13"
- create cost bucket and sub bucket based on ub tob

professional (5a)
- replace missing cpt categories with 8
- find minimum at claim, pos, fromDate level
