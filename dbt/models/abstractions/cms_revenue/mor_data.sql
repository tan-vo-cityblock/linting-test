-------------------------------------
--- Section One: Reference Tables ---
-------------------------------------

--members with required RA related columns
with member as (

select distinct
patientId ,
memberId as id,
firstname,
lastname,
dateOfBirth


from
{{ ref('hcc_risk_members') }}
),


--MOR files for DSNP members
NMI_MBI_Crosswalk as (

select *

from
{{ source('Settlement_CCI','NMI_MBI_Crosswalk')}}
),


--MOR files for DSNP members
dsnp_mor_hccmodd as (

select list.*,
patientid,
_table_suffix as tabSuf

from
{{ source('cci_cms_revenue','mor_hccmodd_3276_*')}} list

INNER join
NMI_MBI_Crosswalk xw
ON trim(list.Medicare_Beneficiary_Identifier_MBI) = trim(xw.MBI)


where
cast(substring(_table_suffix,1,4) as int64) >= extract(year from current_Date) -3
),


--rolls up hccs to categories
hcc_rollup_2020 as (

select distinct
hcc,
hcc_rollup

from
{{ source('codesets', 'hcc_rollup_2020') }}
),

medicare_mor_hccmodd as (

select list.*,
patientid,
_table_suffix as tabSuf

from
{{ source('cci_cms_revenue','mor_hccmodd_3528_*')}} list

INNER join
NMI_MBI_Crosswalk xw
ON trim(list.Medicare_Beneficiary_Identifier_MBI) = trim(xw.MBI)

where
cast(substring(_table_suffix,1,4) as int64) >= extract(year from current_Date) -3
),


tufts_mor_hccmodd as (

select mmr.*,
patientid,
_table_suffix as tabSuf

from
{{ source('tufts_revenue','mor_hccmodd_7419_*')}} mmr

inner join
member mmt
on upper(substr(trim(mmr.Beneficiary_First_Name),0,1)) = upper(substr(trim( mmt.firstname),0,1 ))
and upper(substr(trim(mmr.Beneficiary_LastName),0,6)) = upper(substr(trim( mmt.lastname ),0,6))
and parse_date("%Y%m%d",mmr.Date_of_Birth) =  mmt.dateofbirth

where
cast(substring(_table_suffix,1,4) as int64) >= extract(year from current_Date) -3
),


mor_hccmodd as (

select 'tufts' as partner, 'duals' as lob, * from tufts_mor_hccmodd
union distinct
select  'connecticare' as partner, 'medicare' as lob,  * from medicare_mor_hccmodd
union distinct
select  'connecticare' as partner, 'dsnp' as lob, * from dsnp_mor_hccmodd
),



list as (

select
patientid,
mor_hccmodd.partner,
mor_hccmodd.lob,
Medicare_Beneficiary_Identifier_MBI,
Record_Type_Code,
tabSuf,
    [
STRUCT("1" AS hcc, HCC001 AS Data),
STRUCT("2" AS hcc, HCC002 AS Data),
STRUCT("6" AS hcc, HCC006 AS Data),
STRUCT("8" AS hcc, HCC008 AS Data),
STRUCT("9" AS hcc, HCC009 AS Data),
STRUCT("10" AS hcc, HCC010 AS Data),
STRUCT("11" AS hcc, HCC011 AS Data),
STRUCT("12" AS hcc, HCC012 AS Data),
STRUCT("17" AS hcc, HCC017 AS Data),
STRUCT("18" AS hcc, HCC018 AS Data),
STRUCT("19" AS hcc, HCC019 AS Data),
STRUCT("21" AS hcc, HCC021 AS Data),
STRUCT("22" AS hcc, HCC022 AS Data),
STRUCT("23" AS hcc, HCC023 AS Data),
STRUCT("27" AS hcc, HCC027 AS Data),
STRUCT("28" AS hcc, HCC028 AS Data),
STRUCT("29" AS hcc, HCC029 AS Data),
STRUCT("33" AS hcc, HCC033 AS Data),
STRUCT("34" AS hcc, HCC034 AS Data),
STRUCT("35" AS hcc, HCC035 AS Data),
STRUCT("39" AS hcc, HCC039 AS Data),
STRUCT("40" AS hcc, HCC040 AS Data),
STRUCT("46" AS hcc, HCC046 AS Data),
STRUCT("47" AS hcc, HCC047 AS Data),
STRUCT("48" AS hcc, HCC048 AS Data),
STRUCT("51" AS hcc, HCC051 AS Data),
STRUCT("52" AS hcc, HCC052 AS Data),
STRUCT("54" AS hcc, HCC054 AS Data),
STRUCT("55" AS hcc, HCC055 AS Data),
STRUCT("56" AS hcc, HCC056 AS Data),
STRUCT("57" AS hcc, HCC057 AS Data),
STRUCT("58" AS hcc, HCC058 AS Data),
STRUCT("59" AS hcc, HCC059 AS Data),
STRUCT("60" AS hcc, HCC060 AS Data),
STRUCT("70" AS hcc, HCC070 AS Data),
STRUCT("71" AS hcc, HCC071 AS Data),
STRUCT("72" AS hcc, HCC072 AS Data),
STRUCT("73" AS hcc, HCC073 AS Data),
STRUCT("74" AS hcc, HCC074 AS Data),
STRUCT("75" AS hcc, HCC075 AS Data),
STRUCT("76" AS hcc, HCC076 AS Data),
STRUCT("77" AS hcc, HCC077 AS Data),
STRUCT("78" AS hcc, HCC078 AS Data),
STRUCT("79" AS hcc, HCC079 AS Data),
STRUCT("80" AS hcc, HCC080 AS Data),
STRUCT("82" AS hcc, HCC082 AS Data),
STRUCT("83" AS hcc, HCC083 AS Data),
STRUCT("84" AS hcc, HCC084 AS Data),
STRUCT("85" AS hcc, HCC085 AS Data),
STRUCT("86" AS hcc, HCC086 AS Data),
STRUCT("87" AS hcc, HCC087 AS Data),
STRUCT("88" AS hcc, HCC088 AS Data),
STRUCT("96" AS hcc, HCC096 AS Data),
STRUCT("99" AS hcc, HCC099 AS Data),
STRUCT("099" AS hcc, HCC099 AS Data),
STRUCT("100" AS hcc, HCC100 AS Data),
STRUCT("103" AS hcc, HCC103 AS Data),
STRUCT("104" AS hcc, HCC104 AS Data),
STRUCT("106" AS hcc, HCC106 AS Data),
STRUCT("107" AS hcc, HCC107 AS Data),
STRUCT("108" AS hcc, HCC108 AS Data),
STRUCT("110" AS hcc, HCC110 AS Data),
STRUCT("111" AS hcc, HCC111 AS Data),
STRUCT("112" AS hcc, HCC112 AS Data),
STRUCT("114" AS hcc, HCC114 AS Data),
STRUCT("115" AS hcc, HCC115 AS Data),
STRUCT("122" AS hcc, HCC122 AS Data),
STRUCT("124" AS hcc, HCC124 AS Data),
STRUCT("134" AS hcc, HCC134 AS Data),
STRUCT("135" AS hcc, HCC135 AS Data),
STRUCT("136" AS hcc, HCC136 AS Data),
STRUCT("137" AS hcc, HCC137 AS Data),
STRUCT("138" AS hcc, HCC138 AS Data),
STRUCT("139" AS hcc, HCC139 AS Data),
STRUCT("140" AS hcc, HCC140 AS Data),
STRUCT("141" AS hcc, HCC141 AS Data),
STRUCT("157" AS hcc, HCC157 AS Data),
STRUCT("158" AS hcc, HCC158 AS Data),
STRUCT("159" AS hcc, HCC159 AS Data),
STRUCT("160" AS hcc, HCC160 AS Data),
STRUCT("161" AS hcc, HCC161 AS Data),
STRUCT("162" AS hcc, HCC162 AS Data),
STRUCT("166" AS hcc, HCC166 AS Data),
STRUCT("167" AS hcc, HCC167 AS Data),
STRUCT("169" AS hcc, HCC169 AS Data),
STRUCT("170" AS hcc, HCC170 AS Data),
STRUCT("173" AS hcc, HCC173 AS Data),
STRUCT("176" AS hcc, HCC176 AS Data),
STRUCT("186" AS hcc, HCC186 AS Data),
STRUCT("188" AS hcc, HCC188 AS Data),
STRUCT("189" AS hcc, HCC189 AS Data)
    ] AS hcc_Data

from
mor_hccmodd
),


hcclist as (

select distinct
patientid,
list.partner,
list.lob,
Medicare_Beneficiary_Identifier_MBI,
Record_Type_Code,
tabSuf,
substr(tabSuf,1,4) as paymentYear,
parse_date('%Y%m%d',tabSuf ) as paymentDate,
unnested.*

from
list,
unnest(hcc_Data) as unnested

where
(Data is not null and Data <> '0' and Data <> ' ')
and safe_cast(  REGEXP_REPLACE(hcc,'[^0-9 ]','') as int64) < 200
),


final as (

select hccs.*,
case when hcc_rollup is null then hccs.hcc
        else hcc_rollup
        end as hccRollup
from
hcclist hccs

left join
hcc_rollup_2020 roll
on cast(hccs.hcc as string) = roll.hcc

)


select * from final
