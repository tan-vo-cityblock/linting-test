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

    q1 = read_sql('ecds')
    df = execute_query(q1)
    # df1 = df1.assign(data_all=lambda df: df1[:].applymap(str).agg("|".join, axis=1))

    output_path = home + '/CF_CBH_ECDS_{}.txt'.format(dt)
    df.to_csv(output_path, sep='|', index=False, header=True)
    pd.read_csv(output_path).to_csv(output_path, quoting=csv.QUOTE_NONE, index=False, escapechar='\\', line_terminator='\r\n')
