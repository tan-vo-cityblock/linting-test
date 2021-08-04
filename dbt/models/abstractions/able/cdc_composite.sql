with A1cControl as (
  
  select 
    externalId, 
    createdAt, 
    performanceYear, 
    measureId, 
    numeratorPerformanceNotMet,
    denominatorExclusion
  
  from {{ source ('able','measure_results') }}
  
  where createdAt = (
    select max(createdAt) from {{ source ('able','measure_results') }}) 
    and measureId = "HEDIS CDC3"
    
),
  
nephropathy as (
  
  select
    externalId,
    measureId,
    numeratorPerformanceNotMet,
    denominatorExclusion    
  
  from {{ source ('able','measure_results') }} 
  
  where createdAt = (
    select  max(createdAt) from {{ source ('able','measure_results') }} )
    and measureId = "HEDIS CDC6"),
  
eye_exam as (
  select
    externalId,
    measureId,
    numeratorPerformanceNotMet,
    denominatorExclusion    
  from {{ source ('able','measure_results') }} 
  where createdAt = (
    select max(createdAt) from {{ source ('able','measure_results') }} )
    and measureId = "HEDIS CDC5"),
  
bp_control as (
  select 
    externalId,
    measureId,
    numeratorPerformanceNotMet,
    denominatorExclusion    
  from {{ source ('able','measure_results') }} 
  where createdAt = (
    select max(createdAt) from {{ source ('able','measure_results') }} )
    and measureId = "HEDIS CDC7"),
  
final as (
  
  select 
    a.externalId as patientId,
    case
      when a.denominatorExclusion = 1 or b.denominatorExclusion = 1 or c.denominatorExclusion = 1 or d.denominatorExclusion = 1 then 'EXCLUSION'
      when a.numeratorPerformanceNotMet = 1 or b.numeratorPerformanceNotMet = 1 or c.numeratorPerformanceNotMet = 1 or d.numeratorPerformanceNotMet = 1 then 'OPEN'
      else 'CLOSED'
    end as opportunityStatus,
    'Internal CDC' as measureId,
    'Comprehensive Diabetes Care Composite' as measureIdName,
    a.createdAt
  
  from A1cControl  as a
  
  inner join nephropathy as b 
    on a.externalId = b.externalId
  
  inner join eye_exam as c 
    on b.externalId = c.externalId
  
  inner join bp_control as d 
    on c.externalId = d.externalId)

select * from final
