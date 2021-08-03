{{
  config(
   tags=["nightly", "payer_list"]
  )
}}


{%- set payer_list = var('payer_list') -%}


-----------------------------------
---Section One: Reference tables---
-----------------------------------


with risk_cpt_hcpcs_yr as (

select distinct
hcpcs_cpt_code as hcpcsCptCode ,
year

from 
{{ source('codesets', 'risk_cpt_hcpcs_yr') }} 
),



-------------------------------------
---Section Two: Claims Inclusion---
-------------------------------------
{% for source_name in payer_list %}

{{ source_name }}_claims as (

    select distinct * 
    from
         (select distinct 
         cast( EXTRACT(YEAR FROM  header.date.from ) as string) as clmYear,
         claimId, 
         memberIdentifier.partner,
         'facility' as clmType,
         un_lines.procedure.code as procCode,
        case when (un_lines.procedure.code is not null and cpt.year is not null) 
                or un_lines.procedure.code is null 
          then true
            end as hccEligibleCpt,
            header.typeOfBill as billType,
         case when SUBSTR(header.typeOfBill,2, 2) in ('12', '13', '43', '71', '73', '76', '77', '85')
          then true
            end as hccEligibleBilltype,
          header.date.paid as paidDate,
          un_lines.claimLineStatus as paidStatus

          from 
          {{ source( source_name, 'Facility') }} clm

          left join 
          unnest(header.diagnoses) as un_head_dx  

          left join 
          unnest(lines) as un_lines

          left join 
          risk_cpt_hcpcs_yr cpt
          on un_lines.procedure.code = cpt.hcpcsCptCode
          and CAST(EXTRACT(YEAR FROM header.date.from ) as STRING) = cpt.year 

          where  
          EXTRACT(YEAR FROM  header.date.from )  >= extract(Year from current_date()) - 3
          )

    UNION all

        (select distinct   
        cast( EXTRACT(YEAR FROM  un_lines.date.from ) as string) as clmYear,
        claimId, 
        memberIdentifier.partner,
        'professional' as clmType,
        un_lines.procedure.code as procCode,
        case when (un_lines.procedure.code is not null and cpt.year is not null) 
                or un_lines.procedure.code is null 
          then true
            end as hccEligibleCpt,
        cast(null as string) as billType,
        false as hccEligibleBilltype,
        un_lines.date.paid as paidDate,
        un_lines.claimLineStatus as paidStatus

        from 
        {{ source( source_name, 'Professional') }} clm 

        left join 
        unnest(header.diagnoses) as un_head

        left join 
        unnest(lines) as un_lines

        left join 
        risk_cpt_hcpcs_yr cpt
        on un_lines.procedure.code = cpt.HcpcsCptCode
        and  CAST(EXTRACT(YEAR FROM un_lines.date.from ) as STRING) = cpt.year 

        where 
        extract(Year from un_lines.date.from) >= extract(Year from current_date() )- 3
)
)
{% if not loop.last -%} , {%- endif %}
{% endfor %}
,



----------------------------------------------------
---Section Four: Pulling claims and elig together---
----------------------------------------------------

included_claims as (

select
included.* 

from        
({% for source_name in payer_list -%}

select distinct * from {{ source_name }}_claims
{% if not loop.last -%} union all {%- endif %}
{% endfor %}) included
),

minPaid as (

select distinct 
claimId, 
min(paidDate) as minDate,
paidStatus
from 
included_claims 

where 
paidDate is not null

group by
claimId,
paidStatus
),
----Final Table

final AS (

select distinct
clm.clmYear,
clm.claimId, 
partner,
clmType,
procCode,  
hccEligibleCpt,  
billType,
hccEligibleBilltype,
case when (hccEligibleCpt = true and clmtype = "professional")
       or (hccEligibleBilltype = true and hccEligibleCpt = true and clmtype = "facility") 
then true end as validClaim,
min.minDate as paidDate,
min.paidStatus

from
included_claims clm      

left join
minPaid min 
on clm.claimID = min.ClaimID
) 


select * from final
