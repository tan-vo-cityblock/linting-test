
import pandas as pd
import csv
import numpy as np
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

    dt = datetime.now().strftime('%m%d%Y%H%M%S')
    now = datetime.now()
    df1 = pd.DataFrame({"PhpId": ["BLU"], "PhpName": ["Healthy Blue"], "load": ["F"],
                        "fileName": [f'NCMT_CareQualityManagement_AMH_PatientListRiskScore_BCBS_CB_{datetime.strftime(now,"%Y%m%d")}_{datetime.strftime(now,"%H%M%S")}.txt'],
                        "fileType": ["D"], "version": ["1.0"], "createDate": [datetime.strftime(now,"%Y%m%d")], "createTime": [datetime.strftime(now,"%H:%M:%S")]})

    query = read_sql('patientRiskHealthyBlue')
    df = execute_query(query)
    df1["numberOfRecords"]=str(df.shape[0])
    output_path = home + '/healthyblue_drop_NCMT_CareQualityManagement_AMH_PatientListRiskScore_BCBS_CB_{}.txt'.format(
        dt)
    # with open("test.txt", 'w') as f:
    #     dfAsString = df1.to_string(header=False, index=False,)
    #     f.write(dfAsString)
    test=[]
    with open(output_path, 'w') as f:
        for rec_index, rec in df1.iterrows():
            test.extend(rec.values)
        test[0]='"'+ test[0]
            # test.insert(0,'"')
        f.write(('"|"').join(test)+ '"\n')


    # output_path = home + '/healthyblue_drop_NCMT_CareQualityManagement_AMH_PatientListRiskScore_BCBS_CB_{}.txt'.format(dt)
    # df.to_csv(output_path, sep='|', index=False,header=False,quoting=csv.QUOTE_ALL, quotechar='"',doublequote=True)
    df=df.fillna('').astype(str)
    with open(output_path, 'a') as f:
        for rec_index, rec in df.iterrows():
            test1 = []
            test1.extend(rec.values)
            # test1[:-1][0] + '"'
            test1[len(test1)-1] = test1[len(test1)-1] + '"'
            # test1.insert(0, '"')
            f.write( '\n"' + ('"|"').join(test1))


