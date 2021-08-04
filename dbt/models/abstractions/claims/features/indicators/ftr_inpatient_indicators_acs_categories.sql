


with acs_long as (


    {{ dbt_utils.unpivot(ref('ftr_inpatient_indicators_acs_flags'),
                         cast_to='boolean',
                         exclude=['claimId'],
                         field_name='acsName',
                         value_name='acsValue') }}

),

acs_grouped as (

    select
        claimId,
        true as acsInpatientFlag,
        ARRAY_TO_STRING(array_agg(acsName), ', ') as acsInpatientTypes

    from acs_long

    where acsValue = true

    group by claimId, acsInpatientFlag

)

select * from acs_grouped
