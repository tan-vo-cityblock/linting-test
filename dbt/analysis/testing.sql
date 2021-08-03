

with data as (

    select

        claimId,
        partnerMemberId as memberId,
        principalDiagnosisCode as diagnosis

    from {{ ref('abs_facility_flat') }}

),

mapped as (

    {{ map_cdps_categories(
      diagnosis_column='diagnosis',
      table_name='data',
      index_columns=['memberId'],
      group_by=True
    ) }}

),

heir as (

    {{ apply_cdps_hierarchies('mapped') }}

),

weighted as (

    {{ apply_cdps_weights(table_name='heir',
              model='CDPS',
              model_type='PROSPECTIVE',
              population='TANF ADULTS',
              score_type='acute')
    }}

)

select * from weighted
