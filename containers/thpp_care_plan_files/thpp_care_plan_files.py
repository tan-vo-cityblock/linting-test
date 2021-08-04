
"""
THPP Care Plan Files: pull appropriate data sources, assemble data, output to csv, and zip directory

The output of this script is intended to be a part of our long-term solution with Tufts
to make Cityblock's care plan data ingestible and available by their systems.
"""

import os
import shutil

from os.path import expanduser
from datetime import datetime
from unidecode import unidecode
from google.cloud import bigquery


file_dict = [
    {
        'code': 'code',
        'file': 'NWH_CM_CODE_CBH'
    },
    {
        'code': 'care_manager',
        'file': 'NWH_CM_CARE_MANAGER_CBH'
    },
    {
        'code': 'case',
        'file': 'NWH_CM_CASE_CBH'
    },
    {
        'code': 'goal',
        'file': 'NWH_CM_GOAL_CBH'
    },
    {
        'code': 'member_goal',
        'file': 'NWH_CM_MEMBER_GOAL_CBH'
    },
    {
        'code': 'intervention',
        'file': 'NWH_CM_INTERVENTION_CBH'
    },
    {
        'code': 'member_intervention',
        'file': 'NWH_CM_MEMBER_INTERVENTION_CBH'
    },
    {
        'code': 'problem',
        'file': 'NWH_CM_PROBLEM_CBH'
    },
    {
        'code': 'member_problem',
        'file': 'NWH_CM_MEMBER_PROBLEM_CBH'
    }
]

def read_sql(filename):
    with open(f'sql/{filename}.sql') as file:
        query = file.read()
    return query

def execute_query(query):
    client = bigquery.Client()
    return client.query(query).to_dataframe()

def try_unidecode(text):
    try:
        return unidecode(text)
    except:
        return text


if __name__ == '__main__':

    home = expanduser("~")

    now = datetime.now()
    dt = now.strftime('%m%d%Y')

    tables_dir = home+'/thpp_care_plan_files_output/'
    if not os.path.exists(tables_dir):
        os.mkdir(tables_dir)

    for d in file_dict:
        query = read_sql(d['code'])
        df = execute_query(query)
        df = df.dropna(axis=0, how='all')
        df = df.applymap(try_unidecode)
        csv_loc = tables_dir+'{}.csv'.format(d['file'])
        df.to_csv(csv_loc, sep='~',index=False, encoding='ascii', header=None, line_terminator='\r\n')

    os.chdir(home)
    zip_name = 'CBH_CarePlans_{}'.format(dt)
    shutil.make_archive(zip_name, 'zip', tables_dir)
