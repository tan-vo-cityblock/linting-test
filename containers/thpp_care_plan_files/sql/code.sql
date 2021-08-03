with code_case_goal as (
    select 'NWHD01' as codeId, 'case_type' as codeType, 'Unify Case Management' as codeDescription union all
    select '02' as codeId, 'case_status' as codeType, 'Pending' as codeDescription union all
    select '03' as codeId, 'case_status' as codeType, 'Closed' as codeDescription union all
    select '04' as codeId, 'case_status' as codeType, 'Re-Opened' as codeDescription union all
    select '05' as codeId, 'case_status' as codeType, 'Open' as codeDescription union all
    select 'HH' as codeId, 'case_status' as codeType, 'Health Trio History' as codeDescription union all
    select 'Z2' as codeId, 'case_status' as codeType, 'Deactivated' as codeDescription union all
    select 'ZV' as codeId, 'case_status' as codeType, 'Voided' as codeDescription union all
    select '01' as codeId, 'goal_status' as  codeType, 'Unable to complete' as codeDescription union all
    select '02' as codeId, 'goal_status' as codeType, 'On Hold' as codeDescription union all
    select '03' as codeId, 'goal_status' as codeType, 'Met/Completed' as codeDescription union all
    select 'P1' as codeId, 'goal_status' as codeType, 'Priority Goal 1' as codeDescription union all
    select 'P2' as codeId, 'goal_status' as codeType, 'Priority Goal 2' as codeDescription union all
    select 'P3' as codeId, 'goal_status' as codeType, 'Priority Goal 3' as codeDescription union all
    select 'P4' as codeId, 'goal_status' as codeType, 'Priority Goal 4' as codeDescription union all
    select 'P5' as codeId, 'goal_status' as codeType, 'Priority Goal 5' as codeDescription union all
    select 'P6' as codeId, 'goal_status' as codeType, 'Priority Goal 6' as codeDescription union all
    select 'P7' as codeId, 'goal_status' as codeType, 'Priority Goal 7' as codeDescription union all
    select 'Z2' as codeId, 'goal_status' as codeType, 'Deactivated' as code_description union all
    select '01' as codeId, 'intervention_status' as codeType, 'health coaching ext HD' as codeDescription union all
    select '02' as codeId, 'intervention_status' as codeType, 'case management' as codeDescription union all
    select '03' as codeId, 'intervention_status' as codeType, 'health promotion RN phone' as codeDescription union all
    select '04' as codeId, 'intervention_status' as codeType, 'aftercare' as codeDescription union all
    select '05' as codeId, 'intervention_status' as codeType, 'health promotion matrl mailed' as codeDescription union all
    select '06' as codeId, 'intervention_status' as codeType, 'Outreaching' as codeDescription union all
    select '07' as codeId, 'intervention_status' as codeType, 'Reached provider' as codeDescription union all
    select '08' as codeId, 'intervention_status' as codeType, 'Engaged' as codeDescription union all
    select '09' as codeId, 'intervention_status' as codeType, 'Reached member' as codeDescription union all
    select '10' as codeId, 'intervention_status' as codeType, 'Follow-up' as codeDescription union all
    select '11' as codeId, 'intervention_status' as codeType, 'Transportation Outreach - Prov' as codeDescription union all
    select '12' as codeId, 'intervention_status' as codeType, 'Transportation Outreach -Phone' as codeDescription
 ),

code as (
    select
       'Y' as codeActive,
        CAST(NULL AS STRING)  AS codeTransType ,
        CAST(null AS STRING) AS codeCreateDate,
        CAST(null AS STRING) AS codeCreateBy,
        CAST(null AS STRING) AS codeUpdateDate,
        CAST(null AS STRING) AS codeUpdateBy
)

select  codeTransType,
        codeId,
        codeType,
        codeActive,
        codeDescription,
        codeCreateDate,
        codeCreateBy,
        codeUpdateDate,
        codeUpdateBy
from code_case_goal
join code on 1=1

