with reporting_month as (
  select
  /* 
  Pulls the first and last date of the previous month for the rest of the SLA report
  To change replace date statements with desired start and end dates in YYYY-MM-DD format
  For example: 20202-03-01 as tuftsSlaReportStartDate 
  */
  date_sub(date_trunc(current_date(), month), interval 1 month) as tuftsKpiReportStartDate,
  date_sub(date_trunc(current_date(), month), interval 1 day) as tuftsKpiReportEndDate
  
),

kpi_pharmacy as (
  select 
    patientId, 
    riskAreaSlug, 
    date(createdAt) as interventionDate,
    completedAt as interventionCompletionDate
  
  from {{ source('commons','goal') }}

  where riskAreaSlug = 'medication-management' and
    date(createdAt) >= '2020-03-02'  

),

kpi_num_den as (
  select
  
  -- Reporting Months
  tuftsKpiReportStartDate,
  tuftsKpiReportEndDate,
  
  -- KPI 1: Percent of Members Who Have A Pharmacy Related Intervention On Care Plan During Current Month
  ( select count(distinct m.patientId)
    from {{ ref('thpp_sla_member_info') }} m
    inner join kpi_pharmacy p
    on m.patientId = p.patientId and
      interventionDate between tuftsKpiReportStartDate and tuftsKpiReportEndDate
    where cbHasCarePlan is true 
  ) as kpi1Num,

  ( select count(*) 
    from {{ ref('thpp_sla_member_info') }}
    where cbHasCarePlan is true
  ) as kpi1Den,

 -- KPI 2: Percent of Pharmacy Related Interventions Completed On Care Plan During Current Month
  ( select count(distinct m.patientId)
    from {{ ref('thpp_sla_member_info') }} m
    inner join kpi_pharmacy p
    on m.patientId = p.patientId and
      interventionDate between tuftsKpiReportStartDate and tuftsKpiReportEndDate
    where cbHasCarePlan is true and
    interventionCompletionDate is not null
  ) as kpi2Num,
  
  ( select count(distinct m.patientId)
    from {{ ref('thpp_sla_member_info') }} m
    inner join kpi_pharmacy p
    on m.patientId = p.patientId and
      interventionDate between tuftsKpiReportStartDate and tuftsKpiReportEndDate
    where cbHasCarePlan is true
  ) as kpi2Den,

-- KPI 3: Percentage of Members Who Have Been Identified As Having LTSS Needs Within First 90 Days
  ( select count(distinct patientId)
    from {{ ref('thpp_sla_member_info') }}
    where kpiLtssFlag is true and
    date_add(tuftsEffectiveDate, interval 90 day) between tuftsKpiReportStartDate and tuftsKpiReportEndDate
  ) as kpi3Num,

  ( select count(distinct patientId)
    from {{ ref('thpp_sla_member_info') }}
    where date_add(tuftsEffectiveDate, interval 90 day) between tuftsKpiReportStartDate and tuftsKpiReportEndDate
  ) as kpi3Den,
  
  from reporting_month

),

calculations as (
  select

    -- KPI Report Month
    concat(cast(tuftsKpiReportStartDate as string),' to ',cast(tuftsKpiReportEndDate as string)) as tuftsKpiReportMonth,

    concat(kpi1Num,'/',kpi1Den) as kpi1Calculation,
    concat(kpi2Num,'/',kpi2Den) as kpi2Calculation,
    concat(kpi3Num,'/',kpi3Den) as kpi3Calculation,
  
    case
      when kpi1Den = 0 then 'N/A'
      else cast(kpi1Num/kpi1Den as string)
    end as kpi1MembersWithPharmacyRelatedInterventionsInCurrentMonth,

    case
      when kpi2Den = 0 then 'N/A'
      else cast(kpi2Num/kpi2Den as string)
    end as kpi2PharmacyRelatedInterventionsCompletedInCurrentMonth,

    case
      when kpi3Den = 0 then 'N/A'
      else cast(kpi3Num/kpi3Den as string)
    end as kpi3MembersWithLtssNeedsIn90Days
    
  from kpi_num_den

),

final as (
    select
        tuftsKpiReportMonth,

        kpi1Calculation,
        kpi1MembersWithPharmacyRelatedInterventionsInCurrentMonth,

        kpi2Calculation,
        kpi2PharmacyRelatedInterventionsCompletedInCurrentMonth,

        kpi3Calculation,
        kpi3MembersWithLtssNeedsIn90Days,
        
    from calculations

)

select * from final