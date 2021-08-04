# Scripts

This is a collection of ruby scripts we have used to solve operational
issues. Maintenance here is best-effort, but is not up to the standard
of other parts of Mixer.

## Update MRN Index from Bigquery

TODO

## Verify Patient Demographics

This tool pulls all of the available ACPNY CCD's for patients in the
patient index specified by `CITYBLOCK_DATA_PROJECT_ID` and searches
for DOB's that don't match the DOB from Emblem claims data.

```
bundle exec verify_patient_demographics.rb
```

The script asks the user to define any missing environment variables
(e.g., Redox credentials). It write all mismatched patients to the file
`mismatches.json` in the CWD.

## Fetch CCD For a Given Member

First, set the following environment variables. Contact @ben,
@preston, or @logan for these credentials.

- `REDOX_API_KEY`
- `REDOX_CLIENT_SECRET`
- `REDOX_DESTINATION_NAME`
- `REDOX_DESTINATION_ID`

Once these are set, run

```
bundle exec fetch_ccd.rb <MRN>
```

The script will print out the JSON response from Redox. If the CCD is
unavailable for this member, the JSON response will include an error
message indicating why.

## Geocoding

To geocode member addresses, supply a valid Geocodio API key,
as well as source and destination tables. The script will create
the destination table if it does not exist, otherwise it inserts
geocodes for new addresses in the source table. Note that the script
assumes the schema of the source tables conforms with that of CBH
Gold claims. It also assumes the source and destination tables are
in the same GCP project.

Example usage:

```
ruby geocode_members.rb -k <Geocodio API Key> -d geocoding.member_locations -s gold_claims.Member -p emblem-data
```

## `bq_load_csvs`

Loads CSV files that match the specified partner drop directory and
filename pattern into the designated bigquery project and dataset, but
manually specify the schema as all strings.

Run `./bq_load_csvs` with no arguments for usage information.

## Data Analytics Config
All Data Analytics configurations are located in the [`data_analytics`](./data_analytics)
directory. `cd` into their to use the appropriate utilities outlined below.

### On-call information
To use the Data Analytics on-call information, run the script in the 
following manner:
```commandline
cd data_analytics/
./analytics-config -h  # this pulls the usage docs from the script, -h denotes help
```
This would give further details on how to use the script with available commands including
how to view and alter the on-call rotation.

The `names_map.json` file is a key component of this script, it contains all possible team members
that can be put into the on-call rotation. To quickly view it run:
```commandline
cd data_analytics/
cat names_map.json
```
This should reflect the same information that is given when running `/oncall data` in Slack.
If there is a mismatch, there is a bug in the backend and Platform team should investigate it.

To edit it, use your favorite text editor and save it, please ensure the JSON syntax
is valid. [Paste the JSON into this webpage to confirm it is valid](https://jsonlint.com/).
```commandline
cd data_analytics/
open -a textedit names_map.json  # opens in Mac TextEdit program
```

## Best Practices - Jupyter Notebooks
### Develop with Jupyter notebooks but do not commit them to git
Notebooks should not be staged or committed to git. Notebook output cells can contain data; data should be kept out of version control.

### Convert `.py` between `.ipynb` with `jupytext`
Before committing, convert all `.ipynb` files in `notebooks/` to `.py` files using `jupytext`.

Use the command line utility to sync files or explicitly perform conversions:

```bash
jupytext --to py notebook.ipynb                 # convert notebook.ipynb to a .py file
jupytext --to notebook notebook.py              # convert notebook.py to an .ipynb file with no outputs
jupytext --to notebook --execute notebook.md    # convert notebook.md to an .ipynb file and run it
jupytext --update --to notebook notebook.py     # update the input cells in the .ipynb file and preserve outputs and metadata
jupytext --set-formats ipynb,py notebook.ipynb  # Turn notebook.ipynb into a paired ipynb/py notebook
jupytext --sync notebook.ipynb                  # Update all paired representations of notebook.ipynb
```

Alternatively, you can use `jupytext` as a jupyter lab extension. 
See the [jupytext project](https://github.com/mwouts/jupytext) for details on setting this up.
