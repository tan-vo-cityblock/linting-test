
"""
Health Home Emblem Billing File: Assemble data incorporating billing rates file provided by Market Ops, output as txt

The output of this script is to be provided upon request and update of the billing rates file
"""

import pandas as pd
import csv

from os.path import expanduser
from datetime import datetime
from google.cloud import bigquery


def read_sql(filename):
    with open(f'sql/{filename}.sql') as file:
        query = file.read()
    return query

def execute_query(query):
    client = bigquery.Client()
    return client.query(query).to_dataframe()


if __name__ == '__main__':

    home = expanduser("~")

    now = datetime.now()
    dt = now.strftime('%m%d%Y%H%M%S')

    # Header
    q1 = read_sql('header')
    df1 = execute_query(q1)
    df1 = df1.assign(data_all=lambda df: df1[:].applymap(str).agg("|".join, axis=1))
    df1 = df1['data_all']

    # Claim record
    q2 = read_sql('claim_record')
    df2 = execute_query(q2)
    df2 = df2.assign(data_all=lambda df: df2.drop(['id'], axis=1).applymap(str).agg("|".join, axis=1))
    df2 = df2[['id','recordType','data_all']]

    # Detail lines record
    q3 = read_sql('detail_lines_record')
    df3 = execute_query(q3)
    df3 = df3.assign(data_all=lambda df: df3.drop(['id'], axis=1).applymap(str).agg("|".join, axis=1))
    df3 = df3[['id','recordType','data_all']]

    # Select cols, concat
    keep_cols = ['id', 'recordType', 'data_all']
    df2and3 = pd.concat([df2[keep_cols],df3[keep_cols]], sort=False).sort_values(by=['id','recordType'])['data_all']
    df = pd.concat([df1,df2and3], sort=False).reset_index(drop=True)

    # Output
    output_path = home + '/EH_CITYBLOCK_IN_HMCAID_INSTCLM_EX_XB01_PROD_{}.txt'.format(dt)
    df.to_csv(output_path, sep='|', index=False, header=False)
    pd.read_csv(output_path).to_csv(output_path, quoting=csv.QUOTE_NONE, index=False, escapechar='\\', line_terminator='\r\n')
