
"""
CM Patient Record Export:
PDF version: pull appropriate data sources, assemble data into dataframes, convert to html files, output as pdfs, and merge
Excel version: pull appropriate data sources, assemble data into dataframes, output as excel

The output of this script is member-specific information that is to be run and shared upon request.

"""

import pandas as pd
import re
import os
import pdfkit
import argparse

from datetime import datetime
from os.path import expanduser
from google.cloud import bigquery
from pandas.api.types import is_datetime64_any_dtype as is_datetime
from PyPDF2 import PdfFileMerger


file_dict = [
    {
        'file': 'demographics',
        'tab': 'Member Info'
    },
    {
        'file': 'team',
        'tab': 'Team'
    },
    {
        'file': 'answer',
        'tab': 'Assessment History'
    },
    {
        'file': 'medication',
        'tab': 'Medications'
    },
    {
        'file': 'summary',
        'tab': '360 Summary'
    },
    {
        'file': 'map',
        'tab': 'MAP'
    },
    {
        'file': 'outreach',
        'tab': 'Outreach Notes'
    },
    {
        'file': 'progress',
        'tab': 'Progress Notes'
    },
    {
        'file': 'admissions',
        'tab': 'Admissions & Alerts'
    },
    {
        'file': 'docs',
        'tab': 'Documents'
    }
]


def convert(col):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1 \2', col)
    return re.sub('([a-z0-9])([A-Z])', r'\1 \2', s1).title()

def filter_by_member(df, member_id):
    return df[df['Member Id'] == member_id]

def read_sql(filename):
    with open(f'sql/{filename}.sql') as file:
        query = file.read()
    return query

def execute_query(query):
    client = bigquery.Client()
    return client.query(query).to_dataframe()

def convert_datetime(df):
    df = df.copy()
    dt_cols = [column for column in df.columns if is_datetime(df[column])]
    for col in dt_cols:
        df[col] = df[col].astype(str)
    return df

def setup_argparser():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--memberIds",
        type=str,
        nargs="+",
        required=True,
        help="Input memberId(s) of the member(s) you would like to export a patient record for"
    )
    parser.add_argument(
        "--excel",
        required=False,
        action='store_true',
        help="Will generate an Excel file if specified"
    )
    parser.add_argument(
        "--pdf",
        required=False,
        action='store_true',
        help="Will generate an PDF file if specified"
    )

    return parser


if __name__ == '__main__':

    home = expanduser("~")

    args = setup_argparser().parse_args()

    member_export_list = (args.memberIds)

    dt = datetime.now().strftime("%m%d%Y")

    df_dict = {}

    pd.set_option('display.max_colwidth', 7000)

    for d in file_dict:
        query = read_sql(d['file'])
        df = execute_query(query)
        df.columns = [convert(x) for x in df.columns]
        df = convert_datetime(df)
        df_dict[d['tab']] = df


    # ordered_df_dict={}
    # for key in ['Member Info', 'Team', 'Documents', 'Outreach Notes', 'Assessment History', 'Medications',
    #             '360 Summary', 'MAP', 'Progress Notes', 'Admissions & Alerts']:
    #         ordered_df_dict[key] = df_dict[key]
    # df_dict=ordered_df_dict


    ## PDF output version
    if args.pdf:
        for member in member_export_list:
            for section, df in df_dict.items():
                filtered_df = filter_by_member(df, member)
                filtered_df = filtered_df.loc[:, filtered_df.columns != 'Member Id']

                filtered_html = filtered_df.to_html(classes='table', index=False, escape=False)

                base_dir = home + f'/cm_patient_record_export_files_{member}_{dt}/'
                if not os.path.exists(base_dir):
                    os.mkdir(base_dir)
                os.chdir(base_dir)

                html_filename = f"{section}_{member}.html"
                pdf_filename = f"{section}_{member}.pdf"

                with open(html_filename, "w") as file:
                    file.write(f'{section}')
                    file.write(filtered_html)

                path_wkthmltopdf = '/usr/local/bin/wkhtmltopdf'
                config = pdfkit.configuration(wkhtmltopdf=path_wkthmltopdf)

                pdfkit.from_file(html_filename, pdf_filename, configuration=config)
                os.remove(html_filename)

            merger = PdfFileMerger()

            for key in ['Member Info', 'Team', 'Documents', 'Outreach Notes', 'Assessment History', 'Medications',
                '360 Summary', 'MAP', 'Progress Notes', 'Admissions & Alerts']:
                item = '{}_{}.pdf'.format(key, member)
                merger.append(item)

            merger.write(base_dir + (f'COMBINED_cm_patient_record_export_files_{member}_{dt}.pdf'))
            merger.close()


    ## Excel output version
    if args.excel:
        for member in member_export_list:
            excel_loc = home+f'/cm_patient_record_export_{member}_{dt}.xlsx'
            with pd.ExcelWriter(excel_loc, engine='xlsxwriter', options={'remove_timezone': True}) as writer:
                for tab_name, df in df_dict.items():
                    filtered_df = filter_by_member(df, member)
                    if tab_name in ('Member Info', '360 Summary' ):
    ## The data for Member Info and 360 Summary needs to be transposed as requested by Rosely and Kristen M. for tUfts
                        filtered_df = filtered_df.T
                        filtered_df.to_excel(writer, sheet_name=tab_name,header=False,index=True)
                    else:
                        filtered_df.to_excel(writer, sheet_name=tab_name, index=False)
                    worksheet = writer.sheets[tab_name]
                for idx, col in enumerate(filtered_df):
                    series = filtered_df[col]
                    max_len = max((series.astype(str).map(len).max(), len(str(series.name)))) + 1
                    worksheet.set_column(idx, idx, max_len)
            writer.save()
