
{% macro apply_hcc_hierarchies(table_name) %}

with base as (

    select *

    from {{ table_name }}

),

interactions as (

    select
        base.*,

        case when HCC047=1 and (HCC008+HCC009+HCC010+HCC011+HCC012) >0 then 1 else 0 end as hcc47_gcancer,
        case when HCC085=1 and (HCC017+HCC018+HCC019) >0 then 1 else 0 end as hcc85_gdiabetesmellit,
        case when HCC085=1 and (HCC110+HCC111+HCC112) >0 then 1 else 0 end as hcc85_gcopdcf,
        case when HCC085=1 and (HCC134+HCC135+HCC136+HCC137+HCC138) >0 then 1 else 0 end as HCC85_gRenal_V23,
        case when (HCC082+HCC083+HCC084) >0 and (HCC110+HCC111+HCC112) >0 then 1 else 0 end as grespdepandarre_gcopdcf,
        case when HCC085=1 and HCC096=1 then 1 else 0 end  as hcc85_hcc96,
        case when HCC002=1 and (HCC157+HCC158) > 0 then 1 else 0 end as sepsis_pressure_ulcer,
        case when HCC002=1 and HCC188=1 then 1 else 0 end as sepsis_artif_openings,
        case when HCC188=1 and (HCC157+HCC158) > 0 then 1 else 0 end as art_openings_press_ulcer,
        case when HCC114=1 and (HCC110+HCC111+HCC112) >0 then 1 else 0 end as gCopdCF_ASP_SPEC_B_PNEUM,
        case when HCC114=1 and (HCC157+HCC158) > 0 then 1 else 0 end as ASP_SPEC_B_PNEUM_PRES_ULC,
        case when HCC002=1 and HCC114=1 then 1 else 0 end as SEPSIS_ASP_SPEC_BACT_PNEUM,
        case when HCC057=1 and (HCC110+HCC111+HCC112) >0 then 1 else 0 end as SCHIZOPHRENIA_gCopdCF,
        case when HCC057=1 and HCC085=1 then 1 else 0 end SCHIZOPHRENIA_CHF,
        case when HCC057=1 and HCC079=1 then 1 else 0 end as SCHIZOPHRENIA_SEZ,
        case when HCC085=1 and (HCC110+HCC111+HCC112) >0 then 1 else 0 end as chf_gcopdcf,
        case when (HCC082+HCC083+HCC084) >0 and (HCC110+HCC111+HCC112) >0 then 1 else 0 end as gcopdcf_card_resp_fail,
        case when HCC085=1 and (HCC017+HCC018+HCC019) >0 then 1 else 0 end as diabetes_chf

       
  
    from base

),


ranked as (

    select
        interactions.*,


        {% set trumping=[
        ('HCC009', 'CC_08'),
        ('HCC010', 'CC_08'),
        ('HCC011', 'CC_08'),
        ('HCC012', 'CC_08'),
        ('HCC010', 'CC_09'),
        ('HCC011', 'CC_09'),
        ('HCC012', 'CC_09'),
        ('HCC011', 'CC_10'),
        ('HCC012', 'CC_10'),
        ('HCC012', 'CC_11'),
        ('HCC018', 'CC_17'),
        ('HCC019', 'CC_17'),
        ('HCC019', 'CC_18'),
        ('HCC028', 'CC_27'),
        ('HCC029', 'CC_27'),
        ('HCC080', 'CC_27'),
        ('HCC029', 'CC_28'),
        ('HCC048', 'CC_46'),
        ('HCC055', 'CC_54'),
        ('HCC056', 'CC_54'),
        ('HCC056', 'CC_55'),
        ('HCC058', 'CC_57'),
        ('HCC059', 'CC_57'),
        ('HCC060', 'CC_57'),
        ('HCC059', 'CC_58'),
        ('HCC060', 'CC_58'),
        ('HCC060', 'CC_59'),
        ('HCC071', 'CC_70'),
        ('HCC072', 'CC_70'),
        ('HCC103', 'CC_70'),
        ('HCC104', 'CC_70'),
        ('HCC169', 'CC_70'),
        ('HCC072', 'CC_71'),
        ('HCC104', 'CC_71'),
        ('HCC169', 'CC_71'),
        ('HCC169', 'CC_72'),
        ('HCC083', 'CC_82'),
        ('HCC084', 'CC_82'),
        ('HCC084', 'CC_83'),
        ('HCC087', 'CC_86'),
        ('HCC088', 'CC_86'),
        ('HCC088', 'CC_87'),
        ('HCC100', 'CC_99'),
        ('HCC104', 'CC_103'),
        ('HCC107', 'CC_106'),
        ('HCC108', 'CC_106'),
        ('HCC161', 'CC_106'),
        ('HCC189', 'CC_106'),
        ('HCC108', 'CC_107'),
        ('HCC111', 'CC_110'),
        ('HCC112', 'CC_110'),
        ('HCC112', 'CC_111'),
        ('HCC115', 'CC_114'),
        ('HCC135', 'CC_134'),
        ('HCC136', 'CC_134'),
        ('HCC137', 'CC_134'),
        ('HCC138', 'CC_134'),
        ('HCC136', 'CC_135'),
        ('HCC137', 'CC_135'),
        ('HCC138', 'CC_135'),
        ('HCC137', 'CC_136'),
        ('HCC138', 'CC_136'),
        ('HCC138', 'CC_137'),
        ('HCC158', 'CC_157'),
        ('HCC161', 'CC_157'),
        ('HCC161', 'CC_158'),
        ('HCC080', 'CC_166'),
        ('HCC167', 'CC_166')
        ] %}

        {% for group in trumping %}
          case when {{ group[1] }} = '1' then '0' else {{ group[0] }} end as {{ group[0] }} 
          {%- if not loop.last %},{% endif -%}
        {% endfor %}

    from interactions
)


select * from ranked

{% endmacro %}

