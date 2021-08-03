# Adding a New Partner

## Introduction

This document describes the mixer changes necessary to import a new
partner's claims data into BigQuery and then push that data to mixer
consumers. For the purposes of this guide, we'll call our fictional
partner "AppleCare".

### Disclaimer

Ideally most of this document would either exist as scaladoc or be
rendered unnecessary by more descriptive traits. We've decided that we
should document the process as it currently exists, hoping that
writing this documentation deeply informs a future refactor.

Scaladoc is referenced multiple times throughout this document. You
can view this in your browser by running `sbt doc` and opening the
resulting `index.html` file. Alternatively, just navigate to the
appropriate files in your editor.

### Assumptions

This guide makes a few assumptions about the current state of our
"AppleCare" integration:

- there exists an `applecare-data` GCP project with BigQuery and
  billing enabled
- `gold_claims` and `silver_claims` datasets exist within
  `project-data` and are tracked within `terraform`
- AppleCare files are getting delivered via `sftp` to the appropriate
  subdirectory of `gs://cbh-sftp-drop`
- AppleCare has supplied a data dictionary for their raw claims files
- members in Cityblock's first AppleCare cohort have been added to the
  Member Index

### Terms

**Bronze** data refers to the rawest form of partner data we
receive. For claims, this is the raw text files received via
sftp. These files are typically CSV's that represent database tables
from the partner's data warehouse.

**Silver** data retains the same shape as bronze data, but is enriched
with a Cityblock-generated surrogate id and information from our
member index. This is the `cityblock.importers.common.Patient`
struct. For claims, silver data lives in the `silver_claims` dataset
of the partner's project.

**Gold** data adheres to the [Cityblock gold
schema](https://docs.google.com/spreadsheets/d/1AZTM1Thj3Av1dfJxiNerZO6HLIGk5a0n4GYsjyXYgSo). This
is a subset of the data in silver, but it is normalized across
partners. Gold data lives in the `gold_claims` dataset of the
partner's project.

## Goals

We need to create two batch jobs:

- `LoadAppleCareDataToSilver`, which loads raw CSV's from GCS, converts the data
  to the `silver` schema, and persists the results to BigQuery
- `PolishAppleCareData`, which transforms the `silver` data to
  Cityblock's `gold` schema and persists the results to BigQuery
  
The jobs that hydrate mixer consumers all pull from `gold_claims`.
  
## Components

### Partner Configuration

Add a new entry to `cityblock.utilities.PartnerConfiguration`. For
AppleCare, this would look like

```scala
val applecare: PartnerConfiguration = PartnerConfiguration(
    Partner("AppleCare"),
    "applecare-data",
    "staging-applecare-data",
    "applecare")
```

### Parsers

We need to parse AppleCare's raw CSV's into scala case classes that
mirror the CSV schema.

- create a new package `cityblock.parsers.applecare`
- create a package object including a `Parseable` trait that
  + contains implicit `StringConverter`s for any field types that
    require special parsing logic
  + defines any other common logic for parsing AppleCare's CSV's (such
    as the CSV delimeter)
- for each CSV
  + create a case class with the CSV fields in order
  + create a companion object that extends `Parseable` and defines
    `fromCsv` as a `List[Try[T]]`
    
Scio restricts the field types of case classes used for type-safe
BigQuery. See the [list of supported
types](https://spotify.github.io/scio/api/com/spotify/scio/bigquery/types/BigQueryType$.html).

We'll most likely need to create a
`StringConverter[org.joda.time.LocalDate]`, since this type is
required by Scio for BigQuery `DATE` fields. Each partner typically
has their own idiosyncratic date format.

See `cityblock.parsers.connecticare` for a good example.

### Bronze to Silver Transform

Before we persist the parsed AppleCare data to BigQuery, we need to
transform it to the silver schema. For this, we use `Model` to
represent the Bronze and Silver models, and `ModelTransformer` to
perform the transformer.

- create two new files in `cityblock.models`
  + AppleCareBronzeClaims.scala
  + AppleCareSilverClaims.scala
- in AppleCareBronzeClaims, create an AppleCareBronzeClaimsModel case
  class that extends `Model`
  + its members should be `SCollection`s of the `Parsed*` case classes
    we defined in the last step
- in AppleCareSilverClaims, create an AppleCareSilverClaimsModel case
  class that extends `Model` *and* `Persistable` (see scaladoc in
  `cityblock.transforms.Transform`)
  + the model's members should be `SCollection`s in the following
    format (for AppleCare's [fictional] PharmacyClaim file)
    
    ```scala
        case class PharmacyClaim(
            identifier: SilverIdentifier,
            patient: Patient,
            claim: ParsedPharmacyClaim
        )
    ```
    
    For files that aren't associated with a patient, the `patient`
    field is omitted.
- create a new package `cityblock.transforms.applecare` 
  + within this package, create a `BronzeToSilverClaimsTransformer`
    case class that extends `ModelTransformer`
  + this `ModelTransformer` should contain an `SCollection[(String,
    Patient)]` where `String` is AppleCare's patient identifier
  + for each case class in the silver model, add a `Transformer` that
    maps the corresponding bronze case class to the silver one
    - use `Transform.generateUUID()` to generate surrogate ids
    - join against the indexed `Patient` `SCollection` to get the
      appropriate `Patient` struct
      
For good examples, see `cityblock.models.ConnecticareBronzeClaims`,
`cityblock.models.ConnecticareSilverClaims`, and
`cityblock.transforms.connecticare.BronzeToSilverClaimsTransformer`.

### `LoadAppleCareDataToSilver` Job

To actually run the bronze-to-silver transforms, we need a small job
to fetch the data from GCS, parse them into the bronze claims model,
and the persist the silver data to BigQuery.

- create a new package `cityblock.importers.applecare`
- with that package create `LoadAppleCareDataToSilver.scala`
- in that file's object, include 
  + `val shard: LocalDate` as a global variable (set to the most recent
  delivery date of AppleCare data in GCS)
  + `val dataDeliveryDate: String` as a representation of `shard` that
  matches the date format in the GCS filenames
  + GCS paths for each file we're loading
    - if AppleCare splits up a table across multiple files, we can
      use globbing to grab all files with a single GCS path
      
      ```scala
      val bucket = "cbh_sftp_drop/applecare_production/drop"
      val pharmacyClaimPath = s"gs://$bucket/PharmacyClaim_*_$dataDeliveryDate.txt
      ```

The job should use `PartnerConfiguration` to set the source
project. For debugging, accept a single program argument
`destinationProject` that can be set to something other than
`applecare-data`.

The job needs to extend `ImportableData` and `ExternalDatasource` so
it can fetch the member index with `patientIndex`.

See `cityblock.importers.connecticare.LoadCCIData` for a good
example (but ignore the `dataProject` argument).

### Silver to Gold Transform

The silver-to-gold transform is significantly more complicated than
the bronze-to-silver transform. It requires a lot of domain knowledge
to write the business logic, so we'll frequently need to consult with
the data team. The associated job also spawns multiple dataflow jobs
and requires several joins amongst silver tables.

Within a new package `cityblock.transforms.applecare.gold`, we need to create
- a package object
- an `Indices` case class
- a `Transformer[T]` for each gold table
- a `TransformAppleCareSilverToGold` object (with a main method)

### `cityblock/transforms/applecare/gold/package.scala`

This file primarily contains `cityblock.transforms.Transform.Key` case
classes used for joins throughout the `gold` package. Check the
scaladoc on `Key` for an in-depth explanation.

### Indices

Aside from Provider, every gold table references contains a foreign
key to at least one other gold table. Right now, the primary keys for
each gold table are randomly generated at transform time, so they
cannot be constructed from the corresponding silver object or
objects. As such, for each table referenced by a foreign key, we need
to build an index mapping the corresponding silver id to the gold id
generated in the transform.

We encapsulate index generation in a single `Indices` case class via
an `apply` method.

Index generation is typically expensive, `Indices`
should extend `cityblock.transforms.Transform.Chainable` so its
contents can be made available to other Scio contexts. See the
scaladoc for `Chainable` for detailed instructions.

Most likely, we'll need an index for providers, members, diagnoses,
and procedures. Each index should be of type `SCollection[(K <: Key,
V)]`, where `V` is the type of the gold table. 

For each index type `(K <: Key, V)` with a corresponding gold table `Value`,
create a type alias `type IndexedValue = (K, V)` in the package
object. This enforces typesafe joins and makes it easier for
`Transformer`s to declare which indexes they need.

Since building an index generally requires constructing the gold
`SCollection` itself, the functions to build an index often live in
the associated `Transformer` file instead of in `Indices`.

Diagnoses and procedures are typically delivered in separate
files from their associated claims and are indexed by claim id. Our
convention is to build the diagnosis index in a file named
`DiagnosisTransformer` (same for procedures), even though those files
don't need a class that extends `Transformer`.

### `Transformer`s

Transformers are case classes whose properties are the silver tables
and gold indices necessary to construct a single gold table. Most of
their usage is covered in the scaladoc for
`cityblock.transforms.Transform.Transformer`.

Writing transformers requires consultation with claims domain experts
(i.e., the data team) for certain mappings. Here's an example
workflow:

- make an initial pass through mappings, marking any ambiguous ones
  with a `TODO`
- walk through the list of mappings with @jordan or @gerardo.sierra

Sometimes, getting the mapping "right" requires more substantial data
analysis that is out of scope for the initial Transformer
implementation. In this case, work with the data team to find an
intermediate solution. If no clear intermediate solution exists, emit
`None` for the field.

Reach out to @ben if you are unsure about a field mapping, or if you
want support negotiating which business logic to include in your
initial implementation.

#### `Insurance` and `InsuranceMapping`

One such mapping maps AppleCare's own "Line of Business" values to
Cityblock's standard enums for `lineOfBusiness` and
`subLineOfBusiness`. You'll need to consult with @lesli and
@jac.joubert to determine the appropriate mapping. See
`cityblock.utilities.Insurance` for implementation details.

### "Polish" Job

`TransformAppleCareSilverToGold` links separate data flow jobs for `Indices`,
`Transformer`s, persisting the newly mapped gold data to BigQuery.

The job should accept a single application argument:
`--destinationProject`. This should be set to `applecare-data` for
production runs, and to the developers personal project for debug
runs. Use `cityblock.utilities.PartnerConfiguration` to determine the
source project (which should also be `applecare-data`).

Use `Transform.fetchFromBigQuery` to fetch silver tables from
BigQuery. Pass `LoadAppleCareDataToSilver.shard` to this function so that you
are transforming data for the correct month.

By convention, `TransformAppleCareSilverToGold` contains a function
```scala
def polishAppleCareClaims(
	  sourceProject: String,
	  destinationProject: String,
	  args: Array[String]
    ): (ScioResult, GoldClaimsModel.Futures) = ...
```
that constructs the pipelines for each dataflow job. Use
`cityblock.utilities.ScioUtils.runJob` to encapsulate each
pipeline and `cityblock.utilities.ScioUtils.waitAndOpen` to open
`Future[Tap[T]]`s in subsequent pipelines (read the those functions'
scaladoc for more details). The return type allows `main` to wait on
the result of the final pipeline, and it also allows chaining this job
with other pipelines using `runJob`.

See `cityblock.transforms.connecticare.PolishCCIClaims` for a good
example.

### Testing

We want to create an integration test for `LoadAppleCareDataToSilver` and
`TransformAppleCareSilverToGold`. We'll use `com.spotify.scio.testing.JobTest`
for each test.

For the integration test, we'll need a bronze, silver, and gold
representation of a simple dataset. The final gold dataset
should include

- a `Member` with at least two eligibility and attribution entries
- a `Professional` claim with at least two lines
- at least one of `LabResult`, `Facility`, and `Pharmacy`
- enough `Provider`s to have distinct providers for each claim and for
  the `Member`'s PCP
- diagnoses and procedures for the relevant claims 

This will involve a lot of typing since we need to create mock CSV's for
each AppleCare file and mock case classes for the silver and gold
models. Luckily, the silver output of the `LoadAppleCareDataToSilver` test can
be used as the input of the `TransformAppleCareSilverToGold` test.

#### Equality Assertion
p
The easiest and most concise way to write both tests is to use
`containsInAnyOrder` on each output `SCollection`. However, since
surrogate and gold id generation is random, a naive comparison will
fail.

When testing the silver output of `LoadAppleCareDataToSilver`, it's easy to
use the copy constructor to set all surrogate ids to a dummy value
before using `containsInAnyOrder`.

Testing the gold output of `TransformAppleCareSilverToGold` is more
complicated. We need to ensure that joins against the provider index
worked. Ideally we would implement deterministic id generation when
the job is running in test mode. For now, the suggested workaround is
to
- set all provider references to `None` in the test output and
  expected values before comparing with `containsInAnyOrder`
- assert that the relevant provider references are empty or non-empty
  (depending on the test input)

_We are considering fully embedding provider information in the claims
tables rather than using foreign keys to the `Provider`
table. Verifying joins would be easier if we go that route._
