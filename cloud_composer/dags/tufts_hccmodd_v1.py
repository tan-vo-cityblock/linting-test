import datetime

from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import DAG

import airflow_utils

dag = DAG(
    dag_id="tufts_hccmodd_v1",
    start_date=datetime.datetime(2021, 1, 27),  # Data is usually delivered by the 24th.
    schedule_interval="0 0 27 * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

# Adding 7 days here so that the timestamp reflects *next* month. (We get next month's data this month)
year_month = (datetime.datetime.now() + datetime.timedelta(days=7)).strftime("%Y%m")

load_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id="load_raw_to_bq",
    bucket="cbh_sftp_drop",
    source_objects=[
        "tufts_production/drop/CBH.P.RH7419.HCCMODD.D" + year_month[2:] + "*"
    ],
    destination_project_dataset_table="cityblock-orchestration.ephemeral_airflow.{{ dag.dag_id }}_{{ task.task_id }}",
    schema_fields=[{"name": "data", "type": "STRING", "mode": "NULLABLE"},],
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

extract_human_readable = bigquery_operator.BigQueryOperator(
    task_id="transform_to_human_readable",
    sql=r"""
            WITH table1 AS (SELECT
            * FROM (SELECT
                * FROM (SELECT
                    SUBSTR(data,1,1) as Record_Type_Code
                ,       SUBSTR(data,2,11) as Medicare_Beneficiary_Identifier_MBI
                ,       SUBSTR(data,13,1) as Filler
                ,       SUBSTR(data,14,12) as Beneficiary_LastName
                ,       SUBSTR(data,26,7) as Beneficiary_First_Name
                ,       SUBSTR(data,33,1) as Beneficiary_Initial
                ,       SUBSTR(data,34,8) as Date_of_Birth
                ,       SUBSTR(data,42,1) as Sex
                ,       ' ' as filler1
                ,       SUBSTR(data,52,1) as Age_Group_Female0_34
                ,       SUBSTR(data,53,1) as Age_Group_Female35_44
                ,       SUBSTR(data,54,1) as Age_Group_Female45_54
                ,       SUBSTR(data,55,1) as Age_Group_Female55_59
                ,       SUBSTR(data,56,1) as Age_Group_Female60_64
                ,       SUBSTR(data,57,1) as Age_Group_Female65_69
                ,       SUBSTR(data,58,1) as Age_Group_Female70_74
                ,       SUBSTR(data,59,1) as Age_Group_Female75_79
                ,       SUBSTR(data,60,1) as Age_Group_Female80_84
                ,       SUBSTR(data,61,1) as Age_Group_Female85_89
                ,       SUBSTR(data,62,1) as Age_Group_Female90_94
                ,       SUBSTR(data,63,1) as Age_Group_Female95_GT
                ,       SUBSTR(data,64,1) as Age_Group_Male0_34
                ,       SUBSTR(data,65,1) as Age_Group_Male35_44
                ,       SUBSTR(data,66,1) as Age_Group_Male45_54
                ,       SUBSTR(data,67,1) as Age_Group_Male55_59
                ,       SUBSTR(data,68,1) as Age_Group_Male60_64
                ,       SUBSTR(data,69,1) as Age_Group_Male65_69
                ,       SUBSTR(data,70,1) as Age_Group_Male70_74
                ,       SUBSTR(data,71,1) as Age_Group_Male75_79
                ,       SUBSTR(data,72,1) as Age_Group_Male80_84
                ,       SUBSTR(data,73,1) as Age_Group_Male85_89
                ,       SUBSTR(data,74,1) as Age_Group_Male90_94
                ,       SUBSTR(data,75,1) as Age_Group_Male95_GT

                ,       SUBSTR(data,168,1) as Medicaid
                ,       case when SUBSTR(data,42,1)  = '2' and  SUBSTR(data,169,1)  = '1' then '1' else '0' end as Originally_Disabled_Female
                ,       case when SUBSTR(data,42,1)  = '1' and  SUBSTR(data,169,1)  = '1' then '1' else '0' end as Originally_Disabled_Male

                ,       SUBSTR(data,78,1) as HCC001
                ,       SUBSTR(data,79,1) as HCC002
                ,       SUBSTR(data,80,1) as HCC006
                ,       SUBSTR(data,81,1) as HCC008
                ,       SUBSTR(data,82,1) as HCC009
                ,       SUBSTR(data,83,1) as HCC010
                ,       SUBSTR(data,84,1) as HCC011
                ,       SUBSTR(data,85,1) as HCC012
                ,       SUBSTR(data,86,1) as HCC017
                ,       SUBSTR(data,87,1) as HCC018
                ,       SUBSTR(data,88,1) as HCC019
                ,       SUBSTR(data,89,1) as HCC021
                ,       SUBSTR(data,90,1) as HCC022
                ,       SUBSTR(data,91,1) as HCC023
                ,       SUBSTR(data,92,1) as HCC027
                ,       SUBSTR(data,93,1) as HCC028
                ,       SUBSTR(data,94,1) as HCC029
                ,       SUBSTR(data,95,1) as HCC033
                ,       SUBSTR(data,96,1) as HCC034
                ,       SUBSTR(data,97,1) as HCC035
                ,       SUBSTR(data,98,1) as HCC039
                ,       SUBSTR(data,99,1) as HCC040
                ,       SUBSTR(data,100,1) as HCC046
                ,       SUBSTR(data,101,1) as HCC047
                ,       SUBSTR(data,102,1) as HCC048
                ,       ' ' as HCC051
                ,       ' ' as HCC052
                ,       SUBSTR(data,103,1) as HCC054
                ,       SUBSTR(data,104,1) as HCC055
                ,       ' ' as HCC056
                ,       SUBSTR(data,105,1) as HCC057
                ,       SUBSTR(data,106,1) as HCC058
                ,       ' ' as HCC059
                ,       ' ' as HCC060
                ,       SUBSTR(data,107,1) as HCC070
                ,       SUBSTR(data,108,1) as HCC071
                ,       SUBSTR(data,109,1) as HCC072
                ,       SUBSTR(data,110,1) as HCC073
                ,       SUBSTR(data,111,1) as HCC074
                ,       SUBSTR(data,112,1) as HCC075
                ,       SUBSTR(data,113,1) as HCC076
                ,       SUBSTR(data,114,1) as HCC077
                ,       SUBSTR(data,115,1) as HCC078
                ,       SUBSTR(data,116,1) as HCC079
                ,       SUBSTR(data,117,1) as HCC080
                ,       SUBSTR(data,118,1) as HCC082
                ,       SUBSTR(data,119,1) as HCC083
                ,       SUBSTR(data,120,1) as HCC084
                ,       SUBSTR(data,121,1) as HCC085
                ,       SUBSTR(data,122,1) as HCC086
                ,       SUBSTR(data,123,1) as HCC087
                ,       SUBSTR(data,124,1) as HCC088
                ,       SUBSTR(data,125,1) as HCC096
                ,       SUBSTR(data,126,1) as HCC099
                ,       SUBSTR(data,127,1) as HCC100
                ,       SUBSTR(data,128,1) as HCC103
                ,       SUBSTR(data,129,1) as HCC104
                ,       SUBSTR(data,130,1) as HCC106
                ,       SUBSTR(data,131,1) as HCC107
                ,       SUBSTR(data,132,1) as HCC108
                ,       SUBSTR(data,133,1) as HCC110
                ,       SUBSTR(data,134,1) as HCC111
                ,       SUBSTR(data,135,1) as HCC112
                ,       SUBSTR(data,136,1) as HCC114
                ,       SUBSTR(data,137,1) as HCC115
                ,       SUBSTR(data,138,1) as HCC122
                ,       SUBSTR(data,139,1) as HCC124
                ,       SUBSTR(data,140,1) as HCC134
                ,       SUBSTR(data,141,1) as HCC135
                ,       SUBSTR(data,142,1) as HCC136
                ,       SUBSTR(data,143,1) as HCC137
                ,       ' ' as HCC138
                ,       ' ' as HCC139
                ,       ' ' as HCC140
                ,       ' ' as HCC141
                ,       SUBSTR(data,144,1) as HCC157
                ,       SUBSTR(data,145,1) as HCC158
                ,       ' ' as HCC159
                ,       ' ' as HCC160
                ,       SUBSTR(data,146,1) as HCC161
                ,       SUBSTR(data,147,1) as HCC162
                ,       SUBSTR(data,148,1) as HCC166
                ,       SUBSTR(data,149,1) as HCC167
                ,       SUBSTR(data,150,1) as HCC169

                ,       SUBSTR(data,151,1) as HCC170
                ,       SUBSTR(data,152,1) as HCC173
                ,       SUBSTR(data,153,1) as HCC176
                ,       SUBSTR(data,154,1) as HCC186
                ,       SUBSTR(data,155,1) as HCC188
                ,       SUBSTR(data,156,1) as HCC189

                ,       SUBSTR(data,157,1) as Disabled_Disease_HCC006
                ,       ' ' as filler3
                ,       ' ' as filler4
                ,       ' ' as filler5
                ,       ' ' as Disabled_Disease_HCC034
                ,       ' ' as Disabled_Disease_HCC046
                ,       ' ' as Disabled_Disease_HCC054
                ,       ' ' as Disabled_Disease_HCC055
                ,       ' ' as Disabled_Disease_HCC110
                ,       ' ' as Disabled_Disease_HCC176
                ,       SUBSTR(data,161,1) as CANCER__IMMUNE
                ,       SUBSTR(data,163,1) as CHF_COPD
                ,       SUBSTR(data,164,1) as CHF_RENAL
                ,       SUBSTR(data,166,1) as COPD_CARD_RESP_FAIL
                ,       SUBSTR(data,165,1)  as HCC85_HCC96
                ,       SUBSTR(data,167,1)  as gSubstanceUseDisorder_gPsych
                ,       SUBSTR(data,162,1) as DIABETES_CHF
                ,       ' ' as SEPSIS_CARD_RESP_FAIL
                ,       SUBSTR(data,169,1) as Originally_Disabled
                ,       SUBSTR(data,170,1) as Disabled_Disease_HCC039
                ,       SUBSTR(data,171,1) as Disabled_Disease_HCC077
                ,       SUBSTR(data,172,1) as Disabled_Disease_HCC085
                ,       SUBSTR(data,173,1) as Disabled_Disease_HCC161
                ,       SUBSTR(data,175,1) as RT_OPENINGS_PRESSURE_ULCER
                ,       SUBSTR(data,176,1) as SP_SPEC_BACT_PNEUM_PRES_ULC
                ,       SUBSTR(data,177,1) as COPD_ASP_SPEC_BACT_PNEUM
                ,       SUBSTR(data,174,1) as DISABLED_PRESSURE_ULCER
                ,       SUBSTR(data,178,1) as SCHIZO_PHRENIA_CHF
                ,       SUBSTR(data,179,1) as SCHIZO_PHRENIA_COPD
                ,       SUBSTR(data,180,1) as SCHIZO_PHRENIA_SEIZURES
                ,       SUBSTR(data,181,1) as SEPSIS_ARTIF_OPENINGS
                ,       SUBSTR(data,182,1) as SEPSIS_ASP_SPEC_BACT_PNEUM
                ,       SUBSTR(data,183,1) as SEPSIS_PRESSURE_ULCER
                ,       ' ' as patient_HCC_count
                ,       SUBSTR(data,184,17) as Filler6
                    FROM
                        `TEMPLATE_GOES_HERE`
                    WHERE SUBSTR(data,1,1)  = 'D')

                UNION All (SELECT DISTINCT
                    SUBSTR(data,1,1) as Record_Type_Code,
                        SUBSTR(data,2,11) as Medicare_Beneficiary_Identifier_MBI
                ,       ' ' as Filler
                ,       SUBSTR(data,13,12) as Beneficiary_LastName
                ,       SUBSTR(data,25,7) as Beneficiary_First_Name
                ,       SUBSTR(data,32,1) as Beneficiary_Initial
                ,       SUBSTR(data,33,8) as Date_of_Birth
                ,       SUBSTR(data,41,1) as Sex
                ,       ' ' as Filler2
                ,       SUBSTR(data,42,1) as Age_Group_Female0_34
                ,       SUBSTR(data,43,1) as Age_Group_Female35_44
                ,       SUBSTR(data,44,1) as Age_Group_Female45_54
                ,       SUBSTR(data,45,1) as Age_Group_Female55_59
                ,       SUBSTR(data,46,1) as Age_Group_Female60_64
                ,       SUBSTR(data,47,1) as Age_Group_Female65_69
                ,       SUBSTR(data,48,1) as Age_Group_Female70_74
                ,       SUBSTR(data,49,1) as Age_Group_Female75_79
                ,       SUBSTR(data,50,1) as Age_Group_Female80_84
                ,       SUBSTR(data,51,1) as Age_Group_Female85_89
                ,       SUBSTR(data,52,1) as Age_Group_Female90_94
                ,       SUBSTR(data,53,1) as Age_Group_Female95_GT
                ,       SUBSTR(data,54,1) as Age_Group_Male0_34
                ,       SUBSTR(data,55,1) as Age_Group_Male35_44
                ,       SUBSTR(data,56,1) as Age_Group_Male45_54
                ,       SUBSTR(data,57,1) as Age_Group_Male55_59
                ,       SUBSTR(data,58,1) as Age_Group_Male60_64
                ,       SUBSTR(data,59,1) as Age_Group_Male65_69
                ,       SUBSTR(data,60,1) as Age_Group_Male70_74
                ,       SUBSTR(data,61,1) as Age_Group_Male75_79
                ,       SUBSTR(data,62,1) as Age_Group_Male80_84
                ,       SUBSTR(data,63,1) as Age_Group_Male85_89
                ,       SUBSTR(data,64,1) as Age_Group_Male90_94
                ,       SUBSTR(data,65,1) as Age_Group_Male95_GT

                ,       SUBSTR(data,66,1) as Medicaid
                ,       SUBSTR(data,67,1) as Originally_Disabled_Female
                ,       SUBSTR(data,68,1) as Originally_Disabled_Male

                ,       SUBSTR(data,69,1) as HCC001
                ,       SUBSTR(data,70,1) as HCC002
                ,       SUBSTR(data,71,1) as HCC006
                ,       SUBSTR(data,72,1) as HCC008
                ,       SUBSTR(data,73,1) as HCC009
                ,       SUBSTR(data,74,1) as HCC010
                ,       SUBSTR(data,75,1) as HCC011
                ,       SUBSTR(data,76,1) as HCC012
                ,       SUBSTR(data,77,1) as HCC017
                ,       SUBSTR(data,78,1) as HCC018
                ,       SUBSTR(data,79,1) as HCC019
                ,       SUBSTR(data,80,1) as HCC021
                ,       SUBSTR(data,81,1) as HCC022
                ,       SUBSTR(data,82,1) as HCC023
                ,       SUBSTR(data,83,1) as HCC027
                ,       SUBSTR(data,84,1) as HCC028
                ,       SUBSTR(data,85,1) as HCC029
                ,       SUBSTR(data,86,1) as HCC033
                ,       SUBSTR(data,87,1) as HCC034
                ,       SUBSTR(data,88,1) as HCC035
                ,       SUBSTR(data,89,1) as HCC039
                ,       SUBSTR(data,90,1) as HCC040
                ,       SUBSTR(data,91,1) as HCC046
                ,       SUBSTR(data,92,1) as HCC047
                ,       SUBSTR(data,93,1) as HCC048
                ,       SUBSTR(data,94,1) as HCC051
                ,       SUBSTR(data,95,1) as HCC052
                ,       SUBSTR(data,96,1) as HCC054
                ,       SUBSTR(data,97,1) as HCC055
                ,       SUBSTR(data,98,1) as HCC056
                ,       SUBSTR(data,99,1) as HCC057
                ,       SUBSTR(data,100,1) as HCC058
                ,       SUBSTR(data,101,1) as HCC059
                ,       SUBSTR(data,102,1) as HCC060
                ,       SUBSTR(data,103,1) as HCC070
                ,       SUBSTR(data,104,1) as HCC071
                ,       SUBSTR(data,105,1) as HCC072
                ,       SUBSTR(data,106,1) as HCC073
                ,       SUBSTR(data,107,1) as HCC074
                ,       SUBSTR(data,108,1) as HCC075
                ,       SUBSTR(data,109,1) as HCC076
                ,       SUBSTR(data,110,1) as HCC077
                ,       SUBSTR(data,111,1) as HCC078
                ,       SUBSTR(data,112,1) as HCC079
                ,       SUBSTR(data,113,1) as HCC080
                ,       SUBSTR(data,114,1) as HCC082
                ,       SUBSTR(data,115,1) as HCC083
                ,       SUBSTR(data,116,1) as HCC084
                ,       SUBSTR(data,117,1) as HCC085
                ,       SUBSTR(data,118,1) as HCC086
                ,       SUBSTR(data,119,1) as HCC087
                ,       SUBSTR(data,120,1) as HCC088
                ,       SUBSTR(data,121,1) as HCC096
                ,       SUBSTR(data,122,1) as HCC099
                ,       SUBSTR(data,123,1) as HCC100
                ,       SUBSTR(data,124,1) as HCC103
                ,       SUBSTR(data,125,1) as HCC104
                ,       SUBSTR(data,126,1) as HCC106
                ,       SUBSTR(data,127,1) as HCC107
                ,       SUBSTR(data,128,1) as HCC108
                ,       SUBSTR(data,129,1) as HCC110
                ,       SUBSTR(data,130,1) as HCC111
                ,       SUBSTR(data,131,1) as HCC112
                ,       SUBSTR(data,132,1) as HCC114
                ,       SUBSTR(data,133,1) as HCC115
                ,       SUBSTR(data,134,1) as HCC122
                ,       SUBSTR(data,135,1) as HCC124
                ,       SUBSTR(data,136,1) as HCC134
                ,       SUBSTR(data,137,1) as HCC135
                ,       SUBSTR(data,138,1) as HCC136
                ,       SUBSTR(data,139,1) as HCC137
                ,       SUBSTR(data,140,1) as HCC138
                ,       ' ' as HCC139
                ,       ' ' as HCC140
                ,       ' ' as HCC141
                ,       SUBSTR(data,141,1) as HCC157
                ,       SUBSTR(data,142,1) as HCC158
                ,       SUBSTR(data,143,1) as HCC159
                ,       ' ' as HCC160
                ,       SUBSTR(data,144,1) as HCC161
                ,       SUBSTR(data,145,1) as HCC162
                ,       SUBSTR(data,146,1) as HCC166
                ,       SUBSTR(data,147,1) as HCC167
                ,       SUBSTR(data,148,1) as HCC169
                ,       SUBSTR(data,149,1) as HCC170
                ,       SUBSTR(data,150,1) as HCC173
                ,       SUBSTR(data,151,1) as HCC176
                ,       SUBSTR(data,152,1) as HCC186
                ,       SUBSTR(data,153,1) as HCC188
                ,       SUBSTR(data,154,1) as HCC189
                ,       SUBSTR(data,176,1) as Disabled_Disease_HCC006
                ,       ' ' as filler3
                ,       ' ' as filler4
                ,       ' ' as filler5

                ,       ' ' as Disabled_Disease_HCC034
                ,       ' ' as Disabled_Disease_HCC046
                ,       ' ' as Disabled_Disease_HCC054
                ,       ' ' as Disabled_Disease_HCC055
                ,       ' ' as Disabled_Disease_HCC110
                ,       ' ' as Disabled_Disease_HCC176
                ,       SUBSTR(data,155,1) as CANCER__IMMUNE
                ,       SUBSTR(data,157,1) as CHF_COPD
                ,       SUBSTR(data,158,1) as CHF_RENAL
                ,       SUBSTR(data,159,1) as COPD_CARD_RESP_FAIL
                ,       SUBSTR(data,160,1) as HCC85_HCC96
                ,       SUBSTR(data,161,1) as gSubstanceUseDisorder_gPsych
                ,       SUBSTR(data,156,1) as DIABETES_CHF
                ,       ' ' as SEPSIS_CARD_RESP_FAIL
                ,       case when SUBSTR(data,67,1) = '1' or SUBSTR(data,68,1) = '1' then '1' else '0' end as Originally_Disabled
                ,       SUBSTR(data,174,1) as Disabled_Disease_HCC039
                ,       SUBSTR(data,175,1) as Disabled_Disease_HCC077
                ,       SUBSTR(data,171,1) as Disabled_Disease_HCC085
                ,       SUBSTR(data,173,1) as Disabled_Disease_HCC161
                ,       SUBSTR(data,164,1) as RT_OPENINGS_PRESSURE_ULCER
                ,       SUBSTR(data,166,1) as SP_SPEC_BACT_PNEUM_PRES_ULC
                ,       SUBSTR(data,165,1) as COPD_ASP_SPEC_BACT_PNEUM
                ,       SUBSTR(data,172,1) as DISABLED_PRESSURE_ULCER
                ,       SUBSTR(data,169,1) as SCHIZO_PHRENIA_CHF
                ,       SUBSTR(data,168,1) as SCHIZO_PHRENIA_COPD
                ,       SUBSTR(data,170,1) as SCHIZO_PHRENIA_SEIZURES
                ,       SUBSTR(data,163,1) as SEPSIS_ARTIF_OPENINGS
                ,       SUBSTR(data,167,1) as SEPSIS_ASP_SPEC_BACT_PNEUM
                ,       SUBSTR(data,162,1) as SEPSIS_PRESSURE_ULCER

                ,       SUBSTR(data,177,2) as patient_HCC_count
                ,       SUBSTR(data,179,22) as Filler6
                    FROM
                        `TEMPLATE_GOES_HERE`
                    WHERE
                        SUBSTR(data,1,1)  = 'J')
                )
            )
            SELECT * FROM table1""".replace(
        "TEMPLATE_GOES_HERE",
        "cityblock-orchestration.ephemeral_airflow.{{ dag.dag_id }}_{{ ti.task.upstream_task_ids | first }}",
    ),
    destination_dataset_table=f"tufts-data.cms_revenue.mor_hccmodd_7419_{year_month}01",
    priority="BATCH",
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

load_to_bigquery >> extract_human_readable
