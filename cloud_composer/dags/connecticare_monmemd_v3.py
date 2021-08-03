import datetime

from airflow.contrib.operators import bigquery_operator
from airflow.contrib.operators.gcs_to_bq import GoogleCloudStorageToBigQueryOperator
from airflow.models import DAG

import airflow_utils

dag = DAG(
    dag_id="connecticare_monmemd_v3",
    start_date=datetime.datetime(
        2020, 12, 27
    ),  # Data is usually delivered by the 24th.
    schedule_interval="0 0 27 * *",
    default_args=airflow_utils.default_args,
    catchup=False,
)

# Adding 7 days here so that the timestamp reflects *next* month. (We get next month's data this month)
year_month = (datetime.datetime.now() + datetime.timedelta(days=7)).strftime("%Y%m")

load_medicare_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id="load_medicare_to_bigquery",
    bucket="cbh_sftp_drop",
    source_objects=[
        "connecticare_production/drop/EFTO.FH3528.MONMEMD.D" + year_month[2:] + "*"
    ],
    destination_project_dataset_table="cityblock-orchestration.ephemeral_airflow.{{ dag.dag_id }}_{{ task.task_id }}",
    schema_fields=[{"name": "data", "type": "STRING", "mode": "NULLABLE"},],
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

load_dsnp_to_bigquery = GoogleCloudStorageToBigQueryOperator(
    task_id="load_dsnp_to_bigquery",
    bucket="cbh_sftp_drop",
    source_objects=[
        "connecticare_production/drop/EFTO.FH3276.MONMEMD.D" + year_month[2:] + "*"
    ],
    destination_project_dataset_table="cityblock-orchestration.ephemeral_airflow.{{ dag.dag_id }}_{{ task.task_id }}",
    schema_fields=[{"name": "data", "type": "STRING", "mode": "NULLABLE"},],
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

extract_medicare_human_readable = bigquery_operator.BigQueryOperator(
    task_id="extract_medicare_human_readable",
    sql=r"""
            WITH table1 AS (Select
                LENGTH(data) AS Fixed_Width_File_Length
            ,   SUBSTR(data,1,5) As `Plan_Contract_Number`
            ,		PARSE_DATE("%Y%m%d", SUBSTR(data,6,8)) AS `Run_Date`
            ,		PARSE_DATE("%Y%m%d", CONCAT(SUBSTR(data,14,6), "01")) AS `Payment_Date`
            ,		SUBSTR(data,20,12) AS `Beneficiary_Id`
            ,		SUBSTR(data,32,7) AS `Surname`
            ,		SUBSTR(data,39,1) AS `First_Initial`
            ,		SUBSTR(data,40,1) AS `Gender`
            ,		PARSE_DATE("%Y%m%d", SUBSTR(data,41,8)) AS `Date_of_Birth`
            ,		REGEXP_REPLACE(SUBSTR(data,49,4), r"(\d{2})(\d{2})", "\\1-\\2") AS `Age_Group`
            ,		SUBSTR(data,53,5) AS `State_County_Code` -- Cross reference https://www.nber.org/data/ssa-fips-state-county-crosswalk.html
            ,		IF(SUBSTR(data,58,1) IS NULL, " ", SUBSTR(data,58,1)) AS `Out_of_Area_Indicator`
            ,		IF(SUBSTR(data,59,1) IS NULL, " ", SUBSTR(data,59,1)) AS `Part_A_Entitlement`
            ,		IF(SUBSTR(data,60,1) IS NULL, " ", SUBSTR(data,60,1)) AS `Part_B_Entitlement`
            ,		IF(SUBSTR(data,61,1) IS NULL, " ", SUBSTR(data,61,1)) AS `Hospice`
            ,		IF(SUBSTR(data,62,1) IS NULL, " ", SUBSTR(data,62,1)) AS `ESRD`
            ,		SUBSTR(data,63,1) AS `Aged_Disabled_MSP`
            ,		IF(SUBSTR(data,64,1) IS NULL, " ", SUBSTR(data,64,1)) AS `Institutional`
            ,		IF(SUBSTR(data,65,1) IS NULL, " ", SUBSTR(data,65,1)) AS `NHC`
            ,		IF(SUBSTR(data,66,1) IS NULL, " ", SUBSTR(data,66,1)) AS `New_Medicare_Beneficiary_Medicaid_Status_Flag`
            ,		IF(SUBSTR(data,67,1) IS NULL, " ", SUBSTR(data,67,1)) AS `LTI_Flag`
            ,		IF(SUBSTR(data,68,1) IS NULL, " ", SUBSTR(data,68,1)) AS `Medicaid_Indicator`
            ,		SUBSTR(data,69,2) AS `PIP_DCG`
            ,   IF(SUBSTR(data,71,1) IS NULL, " ", SUBSTR(data,71,1)) AS `Default_Risk_Factor_Code`
            ,		SAFE_CAST(SUBSTR(data,72,7) AS NUMERIC) AS `Risk_Adjustment_Factor_A`
            ,		SAFE_CAST(SUBSTR(data,79,7) AS NUMERIC) AS `Risk_Adjustment_Factor_B`
            ,		SAFE_CAST(SUBSTR(data,86,2) AS INT64) AS `Number_of_Payment_Adjustment_Months_Part_A`
            ,		SAFE_CAST(SUBSTR(data,88,2) AS INT64) AS `Number_of_Payment_Adjustment_Months_Part_B`
            ,		SUBSTR(data,90,2) AS `MMR_Adjustment_Reason_Code`
            ,		PARSE_DATE("%Y%m%d", SUBSTR(data,92,8)) AS `Payment_Adjustment_Start_Date`
            ,		PARSE_DATE("%Y%m%d", SUBSTR(data,100,8)) AS `Payment_Adjustment_End_Date`
            ,		SAFE_CAST(SUBSTR(data,108,9) AS NUMERIC) AS `Demographic_Payment_Adjustment_Rate_A`
            ,		SAFE_CAST(SUBSTR(data,117,9) AS NUMERIC) AS `Demographic_Payment_Adjustment_Rate_B`
            ,		SAFE_CAST(SUBSTR(data,126,9) AS NUMERIC) AS `Monthly_Payment_Adjustment_Amount_Rate_A`
            ,		SAFE_CAST(SUBSTR(data,135,9) AS NUMERIC) AS `Monthly_Payment_Adjustment_Amount_Rate_B`
            ,		SAFE_CAST(SUBSTR(data,144,8) AS NUMERIC) AS `LIS_Premium_Subsidy`
            ,		IF(SUBSTR(data,152,1) IS NULL, " ", SUBSTR(data,152,1)) AS `ESRD_MSP_Flag`
            ,		SAFE_CAST(SUBSTR(data,153,10) AS NUMERIC) AS `Medication_Therapy_Management_Add_On`
            ,   SUBSTR(data,163,8) AS `Filler`
            ,		IF(SUBSTR(data,171,1) IS NULL, " ", SUBSTR(data,171,1)) AS `Medicaid_Status`
            ,   REGEXP_REPLACE(SUBSTR(data,172,4), r"(\d{2})(\d{2})", "\\1-\\2") AS `Risk_Adjustment_Age_Group_RAAG`
            ,		SAFE_CAST(SUBSTR(data,176,7) as NUMERIC) As `Previous_Disable_Ratio_PRDIB`
            ,		SUBSTR(data,183,1) AS `De_Minimis`
            ,		SUBSTR(data,184,1) AS `Beneficiary_Dual_and_Part_D_Enrollment_Status_Flag`
            ,		SUBSTR(data,185,3) AS `Plan_Benefit_Package_Id`
            ,		SUBSTR(data,188,1) AS `Race_Code`
            ,		SUBSTR(data,189,2) AS `Risk_Adjustment_Factor_Type_Code`
            ,		SUBSTR(data,191,1) AS `Frailty_Indicator`
            ,		SUBSTR(data,192,1) AS `Original_Reason_for_Entitlement_Code_OREC`
            ,		SUBSTR(data,193,1) AS `Lag_Indicator`
            ,		SUBSTR(data,194,3) AS `Segment_Number`
            ,		SUBSTR(data,197,1) AS `Enrollment_Source`
            ,		SUBSTR(data,198,1) AS `Employer_Group_Health_Plan_EGHP_Flag`
            ,		SAFE_CAST(SUBSTR(data,199,8) AS NUMERIC) AS `Part_C_Basic_Premium_Part_A_Amount`
            ,		SAFE_CAST(SUBSTR(data,207,8) AS NUMERIC) AS `Part_C_Basic_Premium_Part_B_Amount`
            ,		SAFE_CAST(SUBSTR(data,215,8) AS NUMERIC) AS `Rebate_for_Part_A_Cost_Sharing_Reduction`
            ,		SAFE_CAST(SUBSTR(data,223,8) AS NUMERIC) AS `Rebate_for_Part_B_Cost_Sharing_Reduction`
            ,		SAFE_CAST(SUBSTR(data,231,8) AS NUMERIC) AS `Rebate_for_Other_Part_A_Mandatory_Supplemental_Benefits`
            ,		SAFE_CAST(SUBSTR(data,239,8) AS NUMERIC) AS `Rebate_for_Other_Part_B_Mandatory_Supplemental_Benefits`
            ,		SAFE_CAST(SUBSTR(data,247,8) AS NUMERIC) AS `Rebate_for_Part_B_Premium_Reduction_Part_A_Amount`
            ,		SAFE_CAST(SUBSTR(data,255,8) AS NUMERIC) AS `Rebate_for_Part_B_Premium_Reduction_Part_B_Amount`
            ,		SAFE_CAST(SUBSTR(data,263,8) AS NUMERIC) AS `Rebate_for_Part_D_Supplemental_Benefits_Part_A_Amount`
            ,		SAFE_CAST(SUBSTR(data,271,8) AS NUMERIC) AS `Rebate_for_Part_D_Supplemental_Benefits_Part_B_Amount`
            ,		SAFE_CAST(SUBSTR(data,279,10) AS NUMERIC) AS `Total_Part_A_MA_Payment`
            ,		SAFE_CAST(SUBSTR(data,289,10) AS NUMERIC) AS `Total_Part_B_MA_Payment`
            ,		SAFE_CAST(SUBSTR(data,299,11) AS NUMERIC) AS `Total_MA_Payment_Amount`
            ,		SAFE_CAST(SUBSTR(data,310,7) AS NUMERIC) AS `Part_D_RA_Factor`
            ,		IF(SUBSTR(data,317,1) IS NULL, " ", SUBSTR(data,317,1)) AS `Part_D_Low_Income_Indicator`
            ,		SAFE_CAST(SUBSTR(data,318,7) AS NUMERIC) AS `Part_D_Low_Income_Multiplier`
            ,		IF(SUBSTR(data,325,1) IS NULL, " ", SUBSTR(data,325,1)) AS `Part_D_Long_Term_Institutional_Indicator`
            ,		SAFE_CAST(SUBSTR(data,326,7) AS NUMERIC) AS `Part_D_Long_Term_Institutional_Multiplier`
            ,		SAFE_CAST(SUBSTR(data,333,8) AS NUMERIC) AS `Rebate_for_Part_D_Basic_Premium_Reduction`
            ,		SAFE_CAST(SUBSTR(data,341,8) AS NUMERIC) AS `Part_D_Basic_Premium_Amount`
            ,		SAFE_CAST(SUBSTR(data,349,10) AS NUMERIC) AS `Part_D_Direct_Subsidy_Monthly_Payment_Amount`
            ,		SAFE_CAST(SUBSTR(data,359,10) AS NUMERIC) AS `Reinsurance_Subsidy_Amount`
            ,		SAFE_CAST(SUBSTR(data,369,10) AS NUMERIC) AS `Low_Income_Subsidy_Cost_Sharing_Amount`
            ,		SAFE_CAST(SUBSTR(data,379,11) AS NUMERIC) AS `Total_Part_D_Payment`
            ,		SAFE_CAST(SUBSTR(data,390,2) AS INT64) AS `Number_of_Payment_Adjustment_Months_Part_D`
            ,		SAFE_CAST(SUBSTR(data,392,10) AS NUMERIC) AS `PACE_Premium_Add_On`
            ,		SAFE_CAST(SUBSTR(data,402,10) AS NUMERIC) AS `PACE_Cost_Sharing_Add_on`
            ,		SAFE_CAST(SUBSTR(data,412,7) AS NUMERIC) AS `Part_C_Frailty_Score_Factor`
            ,		SAFE_CAST(SUBSTR(data,419,7) AS NUMERIC) AS `MSP_Factor`
            ,		SAFE_CAST(SUBSTR(data,426,10) AS NUMERIC) AS `MSP_Reduction_Reduction_Adjustment_Amount_Part_A`
            ,		SAFE_CAST(SUBSTR(data,436,10) AS NUMERIC) AS `MSP_Reduction_Reduction_Adjustment_Amount_Part_B`
            ,		SUBSTR(data,446,2) AS `Medicaid_Dual_Status_Code`
            ,		SAFE_CAST(SUBSTR(data,448,8) AS NUMERIC) AS `Part_D_Coverage_Gap_Discount_Amount`
            ,		SUBSTR(data,456,2) AS `Part_D_Risk_Adjustment_Factor_Type`
            ,		SUBSTR(data,458,1) AS `Default_Part_D_Risk_Adjustment_Factor_Code`
            ,		SAFE_CAST(SUBSTR(data,459,9) AS NUMERIC) AS `Part_A_Risk_Adjusted_Monthly_Rate_Amount_for_Payment_Adjustment`
            ,		SAFE_CAST(SUBSTR(data,468,9) AS NUMERIC) AS `Part_B_Risk_Adjusted_Monthly_Rate_Amount_for_Payment_Adjustment`
            ,		SAFE_CAST(SUBSTR(data,477,9) AS NUMERIC) AS `Part_D_Direct_Subsidy_Monthly_Rate_Amount_for_Payment_Adjustment`
            ,		SUBSTR(data,486,10) AS `Cleanup_Id`
            FROM `TEMPLATE_GOES_HERE`)
            SELECT * FROM table1""".replace(
        "TEMPLATE_GOES_HERE",
        "cityblock-orchestration.ephemeral_airflow.{{ dag.dag_id }}_{{ ti.task.upstream_task_ids | first }}",
    ),
    destination_dataset_table=f"connecticare-data.cms_revenue.mmr_monmemd_3528_{year_month}01",
    priority="BATCH",
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

extract_dsnp_human_readable = bigquery_operator.BigQueryOperator(
    task_id="extract_dsnp_human_readable",
    sql=r"""
            WITH table1 AS (Select
                LENGTH(data) AS Fixed_Width_File_Length
            ,   SUBSTR(data,1,5) As `Plan_Contract_Number`
            ,		PARSE_DATE("%Y%m%d", SUBSTR(data,6,8)) AS `Run_Date`
            ,		PARSE_DATE("%Y%m%d", CONCAT(SUBSTR(data,14,6), "01")) AS `Payment_Date`
            ,		SUBSTR(data,20,12) AS `Beneficiary_Id`
            ,		SUBSTR(data,32,7) AS `Surname`
            ,		SUBSTR(data,39,1) AS `First_Initial`
            ,		SUBSTR(data,40,1) AS `Gender`
            ,		PARSE_DATE("%Y%m%d", SUBSTR(data,41,8)) AS `Date_of_Birth`
            ,		REGEXP_REPLACE(SUBSTR(data,49,4), r"(\d{2})(\d{2})", "\\1-\\2") AS `Age_Group`
            ,		SUBSTR(data,53,5) AS `State_County_Code` -- Cross reference https://www.nber.org/data/ssa-fips-state-county-crosswalk.html
            ,		IF(SUBSTR(data,58,1) IS NULL, " ", SUBSTR(data,58,1)) AS `Out_of_Area_Indicator`
            ,		IF(SUBSTR(data,59,1) IS NULL, " ", SUBSTR(data,59,1)) AS `Part_A_Entitlement`
            ,		IF(SUBSTR(data,60,1) IS NULL, " ", SUBSTR(data,60,1)) AS `Part_B_Entitlement`
            ,		IF(SUBSTR(data,61,1) IS NULL, " ", SUBSTR(data,61,1)) AS `Hospice`
            ,		IF(SUBSTR(data,62,1) IS NULL, " ", SUBSTR(data,62,1)) AS `ESRD`
            ,		SUBSTR(data,63,1) AS `Aged_Disabled_MSP`
            ,		IF(SUBSTR(data,64,1) IS NULL, " ", SUBSTR(data,64,1)) AS `Institutional`
            ,		IF(SUBSTR(data,65,1) IS NULL, " ", SUBSTR(data,65,1)) AS `NHC`
            ,		IF(SUBSTR(data,66,1) IS NULL, " ", SUBSTR(data,66,1)) AS `New_Medicare_Beneficiary_Medicaid_Status_Flag`
            ,		IF(SUBSTR(data,67,1) IS NULL, " ", SUBSTR(data,67,1)) AS `LTI_Flag`
            ,		IF(SUBSTR(data,68,1) IS NULL, " ", SUBSTR(data,68,1)) AS `Medicaid_Indicator`
            ,		SUBSTR(data,69,2) AS `PIP_DCG`
            ,   IF(SUBSTR(data,71,1) IS NULL, " ", SUBSTR(data,71,1)) AS `Default_Risk_Factor_Code`
            ,		SAFE_CAST(SUBSTR(data,72,7) AS NUMERIC) AS `Risk_Adjustment_Factor_A`
            ,		SAFE_CAST(SUBSTR(data,79,7) AS NUMERIC) AS `Risk_Adjustment_Factor_B`
            ,		SAFE_CAST(SUBSTR(data,86,2) AS INT64) AS `Number_of_Payment_Adjustment_Months_Part_A`
            ,		SAFE_CAST(SUBSTR(data,88,2) AS INT64) AS `Number_of_Payment_Adjustment_Months_Part_B`
            ,		SUBSTR(data,90,2) AS `MMR_Adjustment_Reason_Code`
            ,		PARSE_DATE("%Y%m%d", SUBSTR(data,92,8)) AS `Payment_Adjustment_Start_Date`
            ,		PARSE_DATE("%Y%m%d", SUBSTR(data,100,8)) AS `Payment_Adjustment_End_Date`
            ,		SAFE_CAST(SUBSTR(data,108,9) AS NUMERIC) AS `Demographic_Payment_Adjustment_Rate_A`
            ,		SAFE_CAST(SUBSTR(data,117,9) AS NUMERIC) AS `Demographic_Payment_Adjustment_Rate_B`
            ,		SAFE_CAST(SUBSTR(data,126,9) AS NUMERIC) AS `Monthly_Payment_Adjustment_Amount_Rate_A`
            ,		SAFE_CAST(SUBSTR(data,135,9) AS NUMERIC) AS `Monthly_Payment_Adjustment_Amount_Rate_B`
            ,		SAFE_CAST(SUBSTR(data,144,8) AS NUMERIC) AS `LIS_Premium_Subsidy`
            ,		IF(SUBSTR(data,152,1) IS NULL, " ", SUBSTR(data,152,1)) AS `ESRD_MSP_Flag`
            ,		SAFE_CAST(SUBSTR(data,153,10) AS NUMERIC) AS `Medication_Therapy_Management_Add_On`
            ,   SUBSTR(data,163,8) AS `Filler`
            ,		IF(SUBSTR(data,171,1) IS NULL, " ", SUBSTR(data,171,1)) AS `Medicaid_Status`
            ,   REGEXP_REPLACE(SUBSTR(data,172,4), r"(\d{2})(\d{2})", "\\1-\\2") AS `Risk_Adjustment_Age_Group_RAAG`
            ,		SAFE_CAST(SUBSTR(data,176,7) as NUMERIC) As `Previous_Disable_Ratio_PRDIB`
            ,		SUBSTR(data,183,1) AS `De_Minimis`
            ,		SUBSTR(data,184,1) AS `Beneficiary_Dual_and_Part_D_Enrollment_Status_Flag`
            ,		SUBSTR(data,185,3) AS `Plan_Benefit_Package_Id`
            ,		SUBSTR(data,188,1) AS `Race_Code`
            ,		SUBSTR(data,189,2) AS `Risk_Adjustment_Factor_Type_Code`
            ,		SUBSTR(data,191,1) AS `Frailty_Indicator`
            ,		SUBSTR(data,192,1) AS `Original_Reason_for_Entitlement_Code_OREC`
            ,		SUBSTR(data,193,1) AS `Lag_Indicator`
            ,		SUBSTR(data,194,3) AS `Segment_Number`
            ,		SUBSTR(data,197,1) AS `Enrollment_Source`
            ,		SUBSTR(data,198,1) AS `Employer_Group_Health_Plan_EGHP_Flag`
            ,		SAFE_CAST(SUBSTR(data,199,8) AS NUMERIC) AS `Part_C_Basic_Premium_Part_A_Amount`
            ,		SAFE_CAST(SUBSTR(data,207,8) AS NUMERIC) AS `Part_C_Basic_Premium_Part_B_Amount`
            ,		SAFE_CAST(SUBSTR(data,215,8) AS NUMERIC) AS `Rebate_for_Part_A_Cost_Sharing_Reduction`
            ,		SAFE_CAST(SUBSTR(data,223,8) AS NUMERIC) AS `Rebate_for_Part_B_Cost_Sharing_Reduction`
            ,		SAFE_CAST(SUBSTR(data,231,8) AS NUMERIC) AS `Rebate_for_Other_Part_A_Mandatory_Supplemental_Benefits`
            ,		SAFE_CAST(SUBSTR(data,239,8) AS NUMERIC) AS `Rebate_for_Other_Part_B_Mandatory_Supplemental_Benefits`
            ,		SAFE_CAST(SUBSTR(data,247,8) AS NUMERIC) AS `Rebate_for_Part_B_Premium_Reduction_Part_A_Amount`
            ,		SAFE_CAST(SUBSTR(data,255,8) AS NUMERIC) AS `Rebate_for_Part_B_Premium_Reduction_Part_B_Amount`
            ,		SAFE_CAST(SUBSTR(data,263,8) AS NUMERIC) AS `Rebate_for_Part_D_Supplemental_Benefits_Part_A_Amount`
            ,		SAFE_CAST(SUBSTR(data,271,8) AS NUMERIC) AS `Rebate_for_Part_D_Supplemental_Benefits_Part_B_Amount`
            ,		SAFE_CAST(SUBSTR(data,279,10) AS NUMERIC) AS `Total_Part_A_MA_Payment`
            ,		SAFE_CAST(SUBSTR(data,289,10) AS NUMERIC) AS `Total_Part_B_MA_Payment`
            ,		SAFE_CAST(SUBSTR(data,299,11) AS NUMERIC) AS `Total_MA_Payment_Amount`
            ,		SAFE_CAST(SUBSTR(data,310,7) AS NUMERIC) AS `Part_D_RA_Factor`
            ,		IF(SUBSTR(data,317,1) IS NULL, " ", SUBSTR(data,317,1)) AS `Part_D_Low_Income_Indicator`
            ,		SAFE_CAST(SUBSTR(data,318,7) AS NUMERIC) AS `Part_D_Low_Income_Multiplier`
            ,		IF(SUBSTR(data,325,1) IS NULL, " ", SUBSTR(data,325,1)) AS `Part_D_Long_Term_Institutional_Indicator`
            ,		SAFE_CAST(SUBSTR(data,326,7) AS NUMERIC) AS `Part_D_Long_Term_Institutional_Multiplier`
            ,		SAFE_CAST(SUBSTR(data,333,8) AS NUMERIC) AS `Rebate_for_Part_D_Basic_Premium_Reduction`
            ,		SAFE_CAST(SUBSTR(data,341,8) AS NUMERIC) AS `Part_D_Basic_Premium_Amount`
            ,		SAFE_CAST(SUBSTR(data,349,10) AS NUMERIC) AS `Part_D_Direct_Subsidy_Monthly_Payment_Amount`
            ,		SAFE_CAST(SUBSTR(data,359,10) AS NUMERIC) AS `Reinsurance_Subsidy_Amount`
            ,		SAFE_CAST(SUBSTR(data,369,10) AS NUMERIC) AS `Low_Income_Subsidy_Cost_Sharing_Amount`
            ,		SAFE_CAST(SUBSTR(data,379,11) AS NUMERIC) AS `Total_Part_D_Payment`
            ,		SAFE_CAST(SUBSTR(data,390,2) AS INT64) AS `Number_of_Payment_Adjustment_Months_Part_D`
            ,		SAFE_CAST(SUBSTR(data,392,10) AS NUMERIC) AS `PACE_Premium_Add_On`
            ,		SAFE_CAST(SUBSTR(data,402,10) AS NUMERIC) AS `PACE_Cost_Sharing_Add_on`
            ,		SAFE_CAST(SUBSTR(data,412,7) AS NUMERIC) AS `Part_C_Frailty_Score_Factor`
            ,		SAFE_CAST(SUBSTR(data,419,7) AS NUMERIC) AS `MSP_Factor`
            ,		SAFE_CAST(SUBSTR(data,426,10) AS NUMERIC) AS `MSP_Reduction_Reduction_Adjustment_Amount_Part_A`
            ,		SAFE_CAST(SUBSTR(data,436,10) AS NUMERIC) AS `MSP_Reduction_Reduction_Adjustment_Amount_Part_B`
            ,		SUBSTR(data,446,2) AS `Medicaid_Dual_Status_Code`
            ,		SAFE_CAST(SUBSTR(data,448,8) AS NUMERIC) AS `Part_D_Coverage_Gap_Discount_Amount`
            ,		SUBSTR(data,456,2) AS `Part_D_Risk_Adjustment_Factor_Type`
            ,		SUBSTR(data,458,1) AS `Default_Part_D_Risk_Adjustment_Factor_Code`
            ,		SAFE_CAST(SUBSTR(data,459,9) AS NUMERIC) AS `Part_A_Risk_Adjusted_Monthly_Rate_Amount_for_Payment_Adjustment`
            ,		SAFE_CAST(SUBSTR(data,468,9) AS NUMERIC) AS `Part_B_Risk_Adjusted_Monthly_Rate_Amount_for_Payment_Adjustment`
            ,		SAFE_CAST(SUBSTR(data,477,9) AS NUMERIC) AS `Part_D_Direct_Subsidy_Monthly_Rate_Amount_for_Payment_Adjustment`
            ,		SUBSTR(data,486,10) AS `Cleanup_Id`
            FROM `TEMPLATE_GOES_HERE`)
            SELECT * FROM table1""".replace(
        "TEMPLATE_GOES_HERE",
        "cityblock-orchestration.ephemeral_airflow.{{ dag.dag_id }}_{{ ti.task.upstream_task_ids | first }}",
    ),
    destination_dataset_table=f"connecticare-data.cms_revenue.mmr_monmemd_3276_{year_month}01",
    priority="BATCH",
    use_legacy_sql=False,
    write_disposition="WRITE_TRUNCATE",
    dag=dag,
)

load_medicare_to_bigquery >> extract_medicare_human_readable
load_dsnp_to_bigquery >> extract_dsnp_human_readable
