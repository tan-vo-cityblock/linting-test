## Computed fields in dbt

Files from the `data`, `macros`, `models`, and `scripts` subfolders collectively power the next version of [computed fields](https://docs.google.com/document/d/1U7OYUKjdaQadM0pq6ni5atnKFjbwFId-DPT6z2hYQpw). 

Each section is described in turn below, with headings indicating the relevant path within the `dbt` folder.

### data/computed_reference/

`computed_codes.csv` contains codes referenced by each computed field. Note that codes may be listed with a percentage sign indicating “like” syntax (for example, `I21%`). The `parameter` field indicates the type of code listed (diagnosis, procedure, lab, etc.), while the `operator` field indicates whether the code is included or excluded.

`computed_jobs.json` contains the configuration for each computed field, with the `type` value indicating its parent macro. Each key: value pair represents an argument supplied by the computed field to a parameter in its parent macro, where that argument is required or different from the default value.

### macros/computed/

**Utility** macros perform basic operations that we use in constructing queries, such as creating long “like” statements from a list of codes. Utility macros specific to computed field are located in the `computed_fields_utils` subfolder.

Two **schema** macros, `computed field` and `computed event`, select the results of each job as standardized fields, which include a UUID for the result and the timestamp at which it was created.

**Primary** macros define logic for common patterns followed by computed fields. By centralizing this logic, we avoid repeating ourselves, and are able to apply changes to many computed fields more efficiently. Individual computed fields can still be finetuned by adjusting arguments to the desired values, rather than relying on the default settings. Examples of primary macros for computed fields include `adherence`, `diagnoses_and_procedures`, and `hedis_encounter`.

Each macro’s **parameters** are defined at the top of the respective file. Default arguments are also supplied here, which may be strings, integers, or boolean values. Optional parameters are set with a default value of `None`.

The **body** of a macro is a list of CTEs making up the SQL query. You’ll spot parameters that are optional or excluded by default by the presence of an `{% if [parameter_name] %}` line above the CTE. These CTEs are only included in the query for a computed field if its configuration supplies an argument for the parameter (in the case of optional parameters), or a value of `True` (in the case of a boolean parameter with a default value of `False`).

### models/abstractions/computed/

**Individual** computed fields and events live in their respective subfolders, `computed_fields` and `computed_events`. They are fairly slim, consisting only of codes and configurations specified in the respective csv files. A Python script, located in the `scripts` folder, creates models automatically based on these specifications.

Two **parent** models, `all_computed_fields` and `all_computed_events`, union results from individual jobs.

### scripts/

The `generate_computed_fields` Python script does just that, creating individual models based on codes and configurations listed in the respective csv files.

### Development

Follow the steps below to create, modify, or delete a computed field.

1. Determine which macro the computed field should use. Macros are named roughly according to the type of evidence they use to determine member values. For example, macros prefixed with `bp_` use blood pressure readings, macros prefixed with `lab_` use lab results, and macros prefixed with `medication_` use pharmacy data.

2. Specify required inputs for the macro you've chosen, and determine whether any default settings should be overwritten.

3. If necessary, add codes for the field to `computed_codes.csv`, and add a dictionary entry for the field to `computed_jobs.json`. When adding codes, follow conventions for `parameter` values (`lab_codes`, `icd_diagnoses`, etc), as these correspond to parameters in associated macros. If you do add codes, you must also include a `"codes": true` key: value pair in the field's dictionary. For `diagnoses_and_procedures` fields, note that you must supply either codes or a value set name.

4. Set your working directory to `mixer/dbt/`. Print your working directory and note the path to mixer, which may be something like `/Users/katieclaiborne/mixer/`. Then execute `python scripts/generate_computed_fields.py [mixer path]` to apply your changes.

5. Test your field by executing `dbt run -m cf_[field_slug]`.
