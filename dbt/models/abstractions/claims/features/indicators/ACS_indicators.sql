
{{ config(tags = ['payer_list']) }}

{%- set payer_list = var('payer_list') -%}


with pcs_chf_icd10 as (
    select distinct icd10 
    from {{source('code_maps','icd9ToIcd10Pcs')}}
    where (substr(icd9,1,3) in ("361", "375", "377")
          or (substr(icd9,1,4) in ("3601", "3602", "3605")))
    ),

pcs_ent_icd10 as (
    select distinct icd10 
    from {{source('code_maps','icd9ToIcd10Pcs')}} 
    where (substr(icd9,1,4) in ('2001'))
    ),

pcs_cellulitis_icd10 as (
    select distinct icd10 
    from {{source('code_maps','icd9ToIcd10Pcs')}} 
    where (substr(icd9,1,3) in ('860'))
    ),

pcs_pelvic_icd10 as (
    select distinct icd10 
    from {{source('code_maps','icd9ToIcd10Pcs')}} 
    where (substr(icd9,1,3) in ('683','684','685','686','687','688'))
    ),




member_date_of_birth as (

  select 
    memberId as patientId, 
    dateOfBirth as dob

  from {{ ref('src_member_demographics') }}
  
),

inpatient_claims as (

  
    select  memberIdentifier.patientId as patientId,header.DRG , 
            cats.*,  
            header.diagnoses, header.procedures,
            date_diff(header.date.From,dob,year) as age

    from   {{ref('ftr_facility_costs_categories')}} cats 
    join   {{ref('facility_gold_all_partners')}}  as em

    on cats.claimId = em.claimId
    left join member_date_of_birth 
    on member_date_of_birth.patientId = em.memberIdentifier.patientId
    where costCategory ="inpatient"
    and (costSubCategory in ('acute','psych') or costCategory = 'inpatient' )
    
    
    ),
 
 chf_diags as (
    select  * from inpatient_claims 
    left join unnest(diagnoses ) as undx
    where undx.tier = 'principal'
          and (
            (substr(undx.code,1,3) in ('428','I50','J81'))
             or (substr(undx.code,1,4) in ('5184'))
             or (substr(undx.code,1,5) in ('40201','40211','40291'))
              )
    ),

 congenital_syphilis_diags as (
    select * from inpatient_claims 
    left join unnest(diagnoses ) as undx
    where undx.tier = 'principal'
         and ((substr(undx.code,1,3) in ('090','A50')))
      ),
 
 immunization_related_preventable_diags as (
    select * from inpatient_claims 
    left join unnest(diagnoses ) as undx
    where undx.tier = 'principal'
          and (
            substr(undx.code,1,3) in ('033','037','045','390','391','A35','A37','A80','I00','I01')
            or ((substr(undx.code,1,4) in ('3220','G000'))) 
              )
      ),

  epilepsy_diags as (
 select * from inpatient_claims 
  left join unnest(diagnoses ) as undx
 where
 undx.tier = 'principal'
 and (substr(undx.code,1,3) in ('345','G40','G41')
 )
 ),
  convulsion_diags as (
 select * from inpatient_claims 
  left join unnest(diagnoses ) as undx
 where
 undx.tier = 'principal'
 and (undx.code like 'R56%' or undx.code like '7803%')
 ),

  ENT_diags as (
 select * from inpatient_claims 
  left join unnest(diagnoses ) as undx
 where
 undx.tier = 'principal'
 and (substr(undx.code,1,3) in ('462','463','465','J02','J03','J06')
 or  (substr(undx.code,1,4) in ('4721','J312')))
 ),

  ENT_diags_v2 as (
 select * from inpatient_claims 
          left join unnest(diagnoses ) as undx 
 where undx.tier = 'principal'
       and (substr(undx.code,1,3) in ('382','H66')
       )
 ),

   tuberculosis_diags as (
      select * from inpatient_claims 
               left join unnest(diagnoses ) as undx
      where undx.tier = 'principal'
            and (substr(undx.code,1,3) in ('011','012','013','014','015','016','017','018','A17','A18','A19')
                or  (substr(undx.code,1,4) in ('A150','A151','A152','A153','A157','A159','A160','A161','A162','A167','A169','A154','A155','A156','A158','A163','A164','A165','A168')))
 ) 
 ,
 copd_diags as (
     select * from inpatient_claims 
               left join unnest(diagnoses ) as undx
      where undx.tier = 'principal'
            and (substr(undx.code,1,3) in ('491','492','494','496','J41','J42','J43','J44','J47')
            )
            )
  
,
 copd_sec as (
     select * from inpatient_claims 
               left join unnest(diagnoses ) as undx
      where undx.tier != 'principal'
            and (substr(undx.code,1,3) in ('491','492','494','496','J41','J42','J43','J44','J47')
            )
            )
,
acute_br as (
     select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               join copd_sec on copd_sec.claimId = inpatient_claims.claimId
      where undx.tier = 'principal'
         and (undx.code like 'J20%' 
              or undx.code like '4660%'))
              
,
BacterialPneumExc as (
      select *,True as excluded from inpatient_claims 
               left join unnest(diagnoses ) as undx
      where undx.tier != 'principal'
            and (substr(undx.code,1,4) in ('2826','D570,D571,D572,D578')))
,  
           

BacterialPneum as (
      select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               left join BacterialPneumExc on inpatient_claims.claimId = BacterialPneumExc.claimId
      where undx.tier = 'principal'
            and excluded is null
            and (substr(undx.code,1,3) in ('481','483','485','486','J13','J14','J16','J18')
                or  (substr(undx.code,1,4) in ('4822','4823','4829','J153','J154','J157','J159')))
                
 ) 
,
 
asthma as (
      select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               
      where undx.tier = 'principal'
            
            and ((substr(undx.code,1,3) in ('493','J45')
               
                )))
,

hypert as (
      select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               
      where undx.tier = 'principal'
            
            and ((substr(undx.code,1,3) in ('I11')
               or substr(undx.code,1,4) in ('4010','4019','I100','I101')
               or substr(undx.code,1,5) in ('40200','40210','40290'))
                ))
 
,

angina as (
      select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               
      where undx.tier = 'principal'
            
            and ((substr(undx.code,1,3) in ('413','I20')
               or substr(undx.code,1,4) in ('4111','4118','I240','I248','I249')
               or substr(undx.code,1,5) in ('I2382'))
                ))

,

cellul as (
      select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               
      where undx.tier = 'principal'
            
            
            and ((substr(undx.code,1,3) in ('681','682','683','686','L03','L04','L08','L88')
               or substr(undx.code,1,4) in ('L444','L922','L980','L983')
               
                )))
                
                
 ,
 
 Diabetes as ( 
     select inpatient_claims.* from inpatient_claims 
                   left join unnest(diagnoses ) as undx

          where undx.tier = 'principal'

                and ((substr(undx.code,1,4) in ('2500','2501','2502','2503','2508','2509','E101','E106','E107','E109','E110','E111','E116','E117','E119','E130','E131','E136','E137','E139','E140','E141','E146','E147','E149'))))

,

hypog as (
     select inpatient_claims.* from inpatient_claims 
                   left join unnest(diagnoses ) as undx

          where undx.tier = 'principal'

                and ((substr(undx.code,1,4) in ('2512','E160','E161','E162'))))
,

gastro as (
     select inpatient_claims.* from inpatient_claims 
                   left join unnest(diagnoses ) as undx

          where undx.tier = 'principal'

                and ((substr(undx.code,1,4) in ('K522','K528','K529','5589'))))
,


 kidney as (
 
 select inpatient_claims.* from inpatient_claims 
           left join unnest(diagnoses ) as undx

  where undx.tier = 'principal'

        and ((substr(undx.code,1,3) in ('590','N10','N11','N12')
           or substr(undx.code,1,4) in ('5990','5999','N136','N151','N158','N159','N160','N161','N162','N163','N164','N165','N369','N390','N399')
           or substr(undx.code,1,5) in ('N2883','N2884','N2885'))
            )),
            
            
  dehydra as (
        select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               
      where undx.tier = 'principal'
            
            and ((substr(undx.code,1,3) in ('E86')
               or substr(undx.code,1,4) in ('2765'))
                )),
                
  anemia as (
      select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               
      where undx.tier = 'principal'
            
            and ( substr(undx.code,1,4) in ('2801','2808','2809','D501','D508','D509'))),
            
   nutri as (
        select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               
      where undx.tier = 'principal'
            
            and ((substr(undx.code,1,3) in ('260','261','262','E40','E41','E42','E43')
               or substr(undx.code,1,4) in ('2680','2681','E550','E643'))
                )),
                
                
    thrive as (
        select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               
      where undx.tier = 'principal'
            
            and ((substr(undx.code,1,3) in ('R62')
               or substr(undx.code,1,4) in ('7834'))
                )),
                
   pelvic as (
        select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               
      where undx.tier = 'principal'
            
            and ((substr(undx.code,1,3) in ('614','N70','N73')
               or substr(undx.code,1,4) in ('N994'))
                )),
                
                
     dental as (
        select inpatient_claims.* from inpatient_claims 
               left join unnest(diagnoses ) as undx
               
      where undx.tier = 'principal'
            
            and ((substr(undx.code,1,3) in ('521','522','523','525','528','K02','K03','K04','K05','K06','K08','K12','K13')
               or substr(undx.code,1,4) in ('K098','K099'))
                )),

alz as (
select inpatient_claims.* from inpatient_claims 
  left join unnest(diagnoses) as undx 
  where undx.tier = 'principal'
  and DRG.code in ('056','057')
  and (substr(undx.code,1,4) in ('G300','G301', 'G308', 'G309','G3101','G3109','G311','G312','G3181',       
'G3182','G3183','G3184','G3189','G319') 
or substr(undx.code,1,5) in ('G300','G301', 'G308', 'G309','G3101','G3109','G311','G312','G3181',       
'G3182','G3183','G3184','G3189','G319') 
)),


hipknee as (
select inpatient_claims.* from inpatient_claims 
  left join unnest(procedures) as unpx 
  where unpx.tier = 'principal' 
  #and DRG.code in ('056','057')
  and unpx.code in ('0SRC0J9','0SRD0J9','0SRB04A','0SR904A','0SRS0J9','0SRB02A','8E0Y0CZ','0SR90JZ','0SR903Z',
    '0SR902A','0SR90JA','0SRB03A','0SRR0J9','0SRB0JZ','0SRB049','0SR902Z','0SPS0JZ','0SRB04Z','0SR904Z','0SRB0JA',
    '0QU70JZ','0SRT0J9','0SR90J9','0SRC069','0SUA09Z','0SRA019','0SRD0JA','0SR9049','0SWC0JZ','0SR903A','0SRR0JZ',
    '0SRB02Z','0SR906A','0SRS01Z','0SRS03A','0SRB01Z','0SRV0J9','0SRC0L9','0SR901A','0SPR0JZ','0SUD09C','0SRW0J9',
    '0SWB0JZ','0SWS0JZ','0SR906Z','0SRD0JZ','0SBD0ZX','0SRE0JA')
),

hysterectom as (
select inpatient_claims.* from inpatient_claims 
  left join unnest(procedures) as unpx 
  where unpx.tier = 'principal' 
  #and DRG.code in ('056','057')
  and unpx.code in ('0UT90ZZ','0UTC0ZZ','0UT90ZL','0UTC4ZZ','0UB90ZZ','0UT94ZZ','0UTC7ZZ','0UT97ZZ','8E0W4CZ','0UT9FZZ',
                    '0UB98ZZ','0UBC7ZX','0UBC0ZZ')
),

spine as (
select inpatient_claims.* from inpatient_claims 
  left join unnest(procedures) as unpx 
  where unpx.tier = 'principal' 
  #and DRG.code in ('056','057')
  and unpx.code in ('0RB30ZZ','0RT30ZZ','0RG20A0','0RG2071','0RG2070','00NW0ZZ','0RG20K0','0RG4071','0PB30ZZ','01N10ZZ','0RG10AJ','0RG207J',
  '0RG1071','0RT50ZZ','0RB90ZZ','0RG40K1','0RG20K1','00NX0ZZ')),

 
 px_chf as (
 select DRG , cats.* , diagnoses, procedures ,icd10, True as excluded
from   {{ref('ftr_facility_costs_categories')}} cats 

 join   chf_diags em on cats.claimId = em.claimId
 left join unnest(procedures) as unpx
 
 join pcs_chf_icd10 on unpx.code = pcs_chf_icd10.icd10
       )
 ,
 
  px_ent as (
 select DRG , cats.* , diagnoses, procedures ,icd10, True as excluded
from   {{ref('ftr_facility_costs_categories')}} cats 

 join   ENT_diags em on cats.claimId = em.claimId
 left join unnest(procedures) as unpx
 
 join pcs_ent_icd10 on unpx.code = pcs_ent_icd10.icd10
       )
 ,      
 px_cell as (
 select DRG , cats.* , diagnoses, procedures ,icd10, True as included
from   {{ref('ftr_facility_costs_categories')}} cats 

 join   cellul em on cats.claimId = em.claimId
 left join unnest(procedures) as unpx
 
 join pcs_cellulitis_icd10 on unpx.code = pcs_cellulitis_icd10.icd10
       ),
       
  px_pel as (
 select DRG , cats.* , diagnoses, procedures ,icd10, True as excluded
from   {{ref('ftr_facility_costs_categories')}} cats 

 join   pelvic em on cats.claimId = em.claimId
 left join unnest(procedures) as unpx
 
 join pcs_pelvic_icd10 on unpx.code = pcs_pelvic_icd10.icd10
       )
       
       
       
,chf as (
select  chf_diags.* ,'CHFACS' as ACS 
from chf_diags 
left join px_chf on chf_diags.claimId = px_chf.claimId 
where excluded is null
)

,

cong_syphil as (
select congenital_syphilis_diags.*, 'CongenitalSyphilisACS' as ACS 
from congenital_syphilis_diags
where age =0),

imunirel as (
select immunization_related_preventable_diags.*, 'ImmunizationRelatedPreventableACS' as ACS from immunization_related_preventable_diags
where age in (1,2,3,4,5))

,

epilepsy as (
select epilepsy_diags.*, 'EpilepsyACS'as ACS 
from epilepsy_diags)
,
convulsions as (
select convulsion_diags.*, 'ConvulsionsACS' as ACS from convulsion_diags)
,
severe_ent as (
select ENT_diags.* ,'SevereEntACS' as ACS from ENT_diags
union all
(select ENT_diags_v2.*, 'SevereEntACS' as ACS from ENT_diags_v2
left join px_ent on ENT_diags_v2.claimId = px_ent.claimId 
where excluded is null))
,
tuberculosis as ( 
select tuberculosis_diags.*, 'TuberculosisACS' as ACS from tuberculosis_diags)
,
copdACS as (
select copd_diags.*, 'COPDACS' as ACS from copd_diags)
,

acute_brACS as (
select acute_br.*, 'acuteBronchitisACS' as ACS from acute_br),

bacpneACS as (
select BacterialPneum.*, 'bacterialPneumoniaACS'as ACS from BacterialPneum  where age >0)
,
asthmaACS as (
select asthma.*, 'Asthma' as ACS from asthma)


,hypertensionACS as (
select  hypert.* ,'hypertensionACS' as ACS 
from hypert 
left join px_chf on hypert.claimId = px_chf.claimId 
where excluded is null
)

,anginaACS as (
select angina.*, 'AnginaACS' as ACS from angina 
where array_length(angina.procedures) =0) #where unnest(procedures) is null

, cellulACS as (
select cellul.* , 'CellulitisACS' as ACS from cellul 
left join px_cell on px_cell.claimId = cellul.claimId
#left join unnest(procedures) as unpx
where included = True or array_length(cellul.procedures) =0)


,
diabetesACS as (
select Diabetes.* , 'DiabetesACS' as ACS from Diabetes )

,

hypoglycemiaACS as (
select hypog.*, 'hypoglycemiaACS' as ACS  from hypog ),

gastroenteritis as (
select gastro.*, 'gastroenteritisACS' as ACS  from gastro ),

kidneyACS as (
select kidney.*, 'kidneyACS' as ACS  from kidney  ),

dehydraACS as (
select dehydra.*, 'dehydrationACS' as ACS  from dehydra   )
,

anemiaACS as ( 
select anemia.* , 'anemiaACS'  as ACS from anemia where age <=5),

nutritionACS as ( 
select nutri.*, 'nutritionalACS' as ACS from nutri ),


thriveACS as (
select thrive.*, 'failToThriveACS' as ACS from thrive where age <1),

pelvicACS as (
select pelvic.*, 'pelvicInflaACS' as ACS from pelvic 
left join px_pel on px_pel.claimId = pelvic.claimId
where excluded is null)

,
dentalACS as (
select dental.*, 'dentalACS' as ACS from dental),

alzACS as (
select alz.*, 'AlzheimerACS' as ACS from alz)
,
psy as (
select inpatient_claims.*, 'psychACS' as ACS from inpatient_claims
where inpatient_claims.costSubCategory ='psych'),

spineACS as (
select spine.*, 'spineACS' as ACS from spine),

hipkneeACS as (
select hipknee.*, 'hipkneeACS' as ACS from hipknee),

hystACS as (
select hysterectom.*, 'hysterectomyACS' as ACS from hysterectom),



allACS as (
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from chf 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from severe_ent 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from imunirel 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from cong_syphil 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from tuberculosis 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from copdACS  
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from epilepsy 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from convulsions
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from acute_brACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from asthmaACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from bacpneACS  
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from hypertensionACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from anginaACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from cellulACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from diabetesACS  
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from hypoglycemiaACS  
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age  from gastroenteritis
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from kidneyACS  
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from dehydraACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from anemiaACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from nutritionACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from thriveACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from pelvicACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from alzACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age	 from psy 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age  from spineACS 
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age  from hystACS
union all
select patientId, claimId,costCategory, costSubCategory, ACS, age  from hipkneeACS
)

select distinct 
dxc.patientId,stays.stayGroup, allACS.ACS, dxc.claimId,dxc.costCategory, dxc.costSubCategory,dxc.age 

from inpatient_claims as dxc 
left join {{ref('ftr_facility_stays')}} as stays 
on dxc.claimId = stays.claimId

left join allACS  
on dxc.claimId = allACS.claimId


