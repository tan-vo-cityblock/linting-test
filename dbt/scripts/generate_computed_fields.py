import pandas as pd
import sys
import json

def create_models(mixer_path):

    config_path = mixer_path + '/dbt/data/computed_reference'

    union_path = mixer_path + '/dbt/models/abstractions/computed'
    model_path = mixer_path + '/dbt/models/abstractions/computed/computed_fields/generated'

    with open(config_path + '/computed_jobs.json', 'r') as infile:
        jobs = json.load(infile)

    # load in the detailed codes of the jobs
    # this included all manually configured codesets as well as hedis codesets
    codes = pd.read_csv(config_path + '/computed_codes.csv', dtype='str')

    # if the operator is excluded, then add it as a prefix to the paratemer name
    codes.loc[codes['operator'] == 'excluded', 'parameter'] = 'excluded_' + codes['parameter']

    # for the sdoh codes append the operator as a prefix as well
    codes.loc[codes['operator'] == 'required', 'parameter'] = 'required_' + codes['parameter']
    codes.loc[codes['operator'] == 'optional', 'parameter'] = 'optional_' + codes['parameter']

    # transform the codes into a dicts of {slug: {parameter1: codes, parameter2: codes2}}
    codes = codes.groupby(['slug', 'parameter']).agg({'codes': list})
    codes = codes.reset_index(level=1).groupby('slug').apply(lambda x: x.to_dict(orient='records'))
    codes = codes.apply(lambda x: [{y['parameter']: y['codes']} for y in x])
    codes = codes.apply(lambda x: {k: v for d in x for k, v in d.items()})
    codes = codes.to_dict()

    # merge the codes parameters into the overall job configuration
    jobs = {k: {**v, **codes[v['slug']]} if 'codes' in v else v for k, v in jobs.items()}

    # remove the codes paramter from the job dictionary
    jobs = {k: {x: y for x, y in v.items() if x != 'codes'} for k, v in jobs.items()}

    # fucntion to quote strings before creating the dbt function strings
    def quote_strings(obj):
        if isinstance(obj, str):
             return f'"{obj}"'
        else:
            return obj

    # iterate through the jobs to create the function calls and files
    all_jobs_refs = []
    for key, job in jobs.items():

        # remove the type from the parameter dict to use as the function name
        function = job.pop('type')

        # create the dbt model name using the pattern of 'cf_{slug}'
        filename = key.replace('-', '_')
        ref_name = 'cf_' + filename
        all_jobs_refs.append(ref_name)

        # ensure the wrapped strings
        job = {k: quote_strings(v) for k, v in job.items()}

        # transform the parameter dict into parameter string with the 'keyword=value' pattern
        keywords = ', '.join('{}={}'.format(key, value) for key, value in job.items())

        # create the function call
        function = f'{function}({keywords})'

        # wrap the function in the jinja context
        function = "{{ " + function + " }}"
        print(function)

        # save the individual models
        output_filename = model_path + '/' + ref_name + '.sql'
        with open(output_filename, "w") as file:
            file.write("\n")
            file.write(function)

    # create a master all cf model that unions them all together
    union_cf_file = union_path + '/' + 'all_computed_fields.sql'
    with open(union_cf_file, "w") as file:
        for index, ref in enumerate(all_jobs_refs):
            if index == 0:
                file.write("\n")
                file.write("select * from {{ ref('" + ref + "') }}")
                file.write("\n")
            else:
                file.write("union all")
                file.write("\n")
                file.write("select * from {{ ref('" + ref + "') }}")
                file.write("\n")

if __name__ == "__main__":

    mixer_path = sys.argv[1]

    create_models(mixer_path)
    #python generate_computed_fields.py '/Users/spencercarrucciu/projects/mixer'
