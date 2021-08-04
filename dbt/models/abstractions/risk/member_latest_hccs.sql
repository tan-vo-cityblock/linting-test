with risk_claims_patients as (
    select * 
    from {{ ref('risk_claims_patients') }}
    where clmyear = extract(year from date_sub(current_date(), interval 9 month)) -- using the calendar year for which we have 9mo runout
        and hcc is not null
),

hcc_desc as (
   select distinct
        cast(hcc as int64) as hcc,
        HCCDescription,
    from risk_claims_patients 
),

ranked_hcc as (
    select distinct
        patientId,
        cast(hcc as int64) as hcc,
        cast(hccRollup as int64) as hccRollup,
        DENSE_RANK () OVER (PARTITION BY patientID, hccRollup ORDER BY hcc) as ranked
    from risk_claims_patients
),

latest_mem_hccs as (
    select 
        patientId,
        cast(hcc as string) as hcc,
        HCCDescription
    from ranked_hcc 
    left join hcc_desc 
        using(hcc)
    where ranked = 1
)
select * from latest_mem_hccs
