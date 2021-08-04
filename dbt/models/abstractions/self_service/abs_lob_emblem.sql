
with lob_mapping_emblem as (

    SELECT *
  FROM UNNEST([
      STRUCT('PGHMO1' as BEN_PKG_ID, 'Commercial' as lineOfBusiness1 , 'Fully Insured' as lineOfBusiness2 ),
      STRUCT('PHCAT1', 'Commercial', 'Exchange'),
      STRUCT('PHBVS1', 'Commercial', 'Exchange'),
      STRUCT('PHGLD1', 'Commercial', 'Exchange'),
      STRUCT('PHSTDB', 'Commercial', 'Fully Insured'),
      STRUCT('PHSVS1', 'Commercial', 'Exchange'),
      STRUCT('PHSPL1', 'Commercial', 'Exchange'),
      STRUCT('PHAXS2', 'Commercial', 'Fully Insured'),
      STRUCT('PHGLDC', 'Commercial', 'Fully Insured'),
      STRUCT('PHBRZC', 'Commercial', 'Fully Insured'),
      STRUCT('PHSVSA', 'Commercial', 'Exchange'),
      STRUCT('PHPLTA', 'Commercial', 'Exchange'),
      STRUCT('PHSLVA', 'Commercial', 'Exchange'),
      STRUCT('PHBRZA', 'Commercial', 'Exchange'),
      STRUCT('P1CHP1', 'Medicaid',   'Medicaid'),
      STRUCT('PHGLDB', 'Commercial', 'Fully Insured'),
      STRUCT('PHSLVB', 'Commercial', 'Fully Insured'),
      STRUCT('PHSVD1', 'Commercial', 'Exchange'),
      STRUCT('PHBVD1', 'Commercial', 'Exchange'),
      STRUCT('PEVEP1', 'Medicare',   'Medicare Advantage'),
      STRUCT('PEVXC1', 'Medicare',   'Medicare Advantage'),
      STRUCT('PEVSD2', 'Medicare',   'DSNP'),
      STRUCT('PEVHP1', 'Medicare',   'Medicare Advantage'),
      STRUCT('P1EPPA', 'Medicaid',   'HARP'),
      STRUCT('PEVSP3', 'Medicare',   'Medicare Advantage'),
      STRUCT('P1KIDS', 'Medicaid',   'HARP'),
      STRUCT('PHSTDC', 'Commercial', 'Fully Insured'),
      STRUCT('PHARP1', 'Medicaid',   'HARP'),
      STRUCT('P1EPPB', 'Medicaid',   'HARP'),
      STRUCT('PPSTD5', 'Commercial', 'Fully Insured'),
      STRUCT('PHTBP1', 'Commercial', 'Fully Insured'),
      STRUCT('PHSTD8', 'Commercial', 'Fully Insured'),
      STRUCT('P1HFED', 'Commercial', 'Fully Insured'),
      STRUCT('PHSTDA', 'Commercial', 'Fully Insured'),
      STRUCT('PHSTD9', 'Commercial', 'Fully Insured'),
      STRUCT('PH0005', 'Commercial', 'Fully Insured'),
      STRUCT('PEVVP1', 'Medicare',   'Medicare Advantage'),
      STRUCT('P1WEST', 'Medicaid',   'Medicaid'),
      STRUCT('PHNYG1', 'Commercial', 'Exchange'),
      STRUCT('PHSLV1', 'Commercial', 'Exchange'),
      STRUCT('PE0002', 'Commercial', 'Fully Insured'),
      STRUCT('PH0006', 'Commercial', 'Fully Insured'),
      STRUCT('PEVSP2', 'Medicare',   'Medicare Advantage'),
      STRUCT('PECSAB', 'Medicare',   'Medicare Advantage'),
      STRUCT('PHGLDA', 'Commercial', 'Exchange'),
      STRUCT('PHBSVA', 'Commercial', 'Exchange'),
      STRUCT('PEFVIP', 'Medicare',   'Medicare Advantage'),
      STRUCT('PHBRZB', 'Commercial', 'Exchange'),
      STRUCT('PPSTD2', 'Commercial', 'Fully Insured'),
      STRUCT('PHSBZA', 'Commercial', 'Exchange'),
      STRUCT('PHSGLA', 'Commercial', 'Exchange'),
      STRUCT('PHSPLA', 'Commercial', 'Exchange'),
      STRUCT('PHGVAA', 'Commercial', 'Exchange'),
      STRUCT('PHSSP1', 'Commercial', 'Fully Insured'),
      STRUCT('PE0003', 'Commercial', 'Fully Insured'),
      STRUCT('PHPLTB', 'Commercial', 'Exchange'),
      STRUCT('PPAXS5', 'Commercial', 'Fully Insured'),
      STRUCT('PHBRZ1', 'Commercial', 'Exchange'),
      STRUCT('PHCATA', 'Commercial', 'Exchange'),
      STRUCT('PHSGL1', 'Commercial', 'Exchange'),
      STRUCT('PHSSLA', 'Commercial', 'Exchange'),
      STRUCT('PHSSL1', 'Commercial', 'Exchange'),
      STRUCT('PGSSP1', 'Commercial', 'Fully Insured'),
      STRUCT('PHSPC1', 'Commercial', 'Exchange'),
      STRUCT('PHSGPR', 'Commercial', 'Fully Insured'),
      STRUCT('PHSGP1', 'Commercial', 'Fully Insured'),
      STRUCT('PHSSPR', 'Commercial', 'Fully Insured'),
      STRUCT('PH0004', 'Commercial', 'Fully Insured'),
      STRUCT('P1HVIP', 'Commercial', 'Fully Insured'),
      STRUCT('PHSGC1', 'Commercial', 'Exchange'),
      STRUCT('PHSSPA', 'Commercial', 'Fully Insured'),
      STRUCT('P1HMO1', 'Commercial', 'Fully Insured'),
      STRUCT('PE0001', 'Commercial', 'Fully Insured'),
      STRUCT('PHSBZ1', 'Commercial', 'Exchange'),
      STRUCT('PHGVDA', 'Commercial', 'Exchange'),
      STRUCT('PECSBO', 'Medicare',   'Medicare Advantage'),
      STRUCT('PPAXS2', 'Commercial', 'Fully Insured'),
      STRUCT('PPAXS1', 'Commercial', 'Fully Insured'),
      STRUCT('PHSSPS', 'Commercial', 'Fully Insured'),
      STRUCT('PEMFED', 'Medicare',   'Medicare Advantage'),
      STRUCT('P1VSP1', 'Medicare',   'Medicare Advantage'),
      STRUCT('PHSSC1', 'Commercial', 'Exchange'),
      STRUCT('PEDAP3', 'Medicare',   'Medicare Advantage'),
      STRUCT('PPSTD6', 'Commercial', 'Fully Insured'),
      STRUCT('PHSGV1', 'Commercial', 'Exchange'),
      STRUCT('PFPPOE', 'Commercial', 'Fully Insured'),
      STRUCT('PFPPO5', 'Commercial', 'Fully Insured'),
      STRUCT('PECCE1', 'Commercial', 'Fully Insured'),
      STRUCT('PESLT3', 'Commercial', 'Fully Insured'),
      STRUCT('PEVSP1', 'Medicare',   'Medicare Advantage'),
      STRUCT('P1VIP1', 'Medicare',   'Medicare Advantage'),
      STRUCT('P1MPAO', 'Medicaid',   'Medicaid'),
      STRUCT('P1MPAU', 'Medicaid',   'Medicaid')
  ])

),

lob_emblem_temp as (

  {% set emblem_list = ["emblem_silver", "emblem_silver_virtual"] %}

  {% for emblem in emblem_list %}

    select
        mm.patient.patientId,
        mm.month.SPANFROMDATE as spanFromDate,
        map.lineOfBusiness1,
        map.lineOfBusiness2

    from lob_mapping_emblem map

    join {{ source(emblem, 'member_month') }} as mm
      on map.BEN_PKG_ID = mm.month.BENEFITPLANID

    where mm.patient.patientId is not null

    {% if not loop.last %} union distinct {% endif %}

  {% endfor %}

),

-- arbitrarily choosing 1 lob per member per month to reduce fanout, using alphabetical by BENEFITPLANID
-- eventually replace with a real hierarchy, and/or switch to using LOB from the member data not claims data

lob_emblem as (

    select
        patientId,
        spanFromDate,
        lineOfBusiness1,
        lineOfBusiness2,
        row_number() over(partition by patientId, spanFromDate order by lineOfBusiness1, lineOfBusiness2 asc) as rowNum

    from lob_emblem_temp

),

final as (

  select *
  from lob_emblem
  where rowNum = 1

)

select * from final
