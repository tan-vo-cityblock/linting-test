# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:light
#     text_representation:
#       extension: .py
#       format_name: light
#       format_version: '1.4'
#       jupytext_version: 1.1.1
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

import pandas_gbq as gbq
from tableone import TableOne

project = 'cbh-katie-claiborne'

def tryBQ(file):
    try:
        query = open(file, 'r')
        y = gbq.read_gbq(query.read(), project_id = project, dialect = "standard")
        print('Data pull complete')
        return(y)
    except:
        print('Error reading dataset')

df_panel_capacity = tryBQ('panel_capacity.sql')

# %whos DataFrame

columns = ['ratioTendsToAttempts']
categorical = ['isOnPanelLargerThan30']
groupby = ['isOnPausedPanel']
nonnormal = ['ratioTendsToAttempts']

mytable = TableOne(df_panel_capacity, columns, categorical,
                   groupby, nonnormal, pval = True)
