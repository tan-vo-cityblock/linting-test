With 
--===============================================================
--PREWORK PREPARE CLAIMS ETC FILES. APPLY LOGIC ONCE FOR FFS PAID
--================================================================
   FacilityClaimLines as (   --GET ALL FFS FACILITY CLAIM LINES IN 2019  
         select * , 
                case when claim.sv_line < 10 then concat('0', cast(claim.sv_line as string))
                     else cast(claim.sv_line as string)
                end as sv_line_1,         
                case when claim.hosp_type in ('I', 'S') then 'IP' 
                     else 'OP' 
                end as CLAIMTYP2,
                case when claim.HOSP_TYPE in ('I', 'S') and claim.adm_date is not null and claim.adm_date <= claim.from_date 
                         then claim.adm_date else claim.from_date end as Start_Date,
                claim.to_date as End_Date,
                case when claim.HOSP_TYPE in ('I', 'S') and claim.adm_date is not null then claim.adm_date 
                     else claim.from_date 
                end as DOS,
                case when claim.HOSP_TYPE in ('I', 'S') and claim.adm_date is not null 
                                 then extract(year from claim.adm_date)*100 + extract(month from claim.adm_date)
                     else extract(year from claim.from_date)*100 + extract(month from claim.from_date) end as DOS_YYYYMM,
                extract(year from claim.paid_date)*100 + extract(month from claim.paid_date) as DOP_YYYYMM,
                cast(claim.amt_allowed as NUMERIC) as Allowed_amt,
                cast(claim.amt_paid as NUMERIC) as Paid_amt,
-- it is only: claim.supergroupID = 'FD'
                case  when claim.sv_stat not in ('P', '1', 'E') then 'Open' 
                      when ((claim.supergroupID is not null and claim.supergroupID = 'FD') 
                              or claim.medicalcenternumber in ('0100', '0108', '0139', '0140', '0141','0142'
                                                               ,'0143', '0145', '0146', '14BR', '14GR', '14RP'
                                                               , '14MM', '14MN', '14MV'
                                                               --, '14MX' -- 3/23
                                                               , '14MY', '14HH'
                                                               --, '14HX' -- 3/23
                                                               , '14HY'
                                                               --, '14HZ' -- 3/23
                                                               )) then 'FD'
                      when claim.PROC_CODE = 'G9005' or claim.REV_CODE = '0500' then 'HH'
                      when claim.FFSCAPIND = 'C' or (claim.CL_DATA_SRC is not null and Trim(claim.CL_DATA_SRC) = 'QPN2') 
                           or (claim.CARECORERESPONSIBLEAMT is not null and cast(claim.CARECORERESPONSIBLEAMT as numeric) >0) 
                          then 'CAP'
                      else 'FFS' end as PayType1
                      ,claim.CARECORERESPONSIBLEAMT
         from {{ source('emblem_silver', 'facility') }}
         where patient.patientID is not null
           and claim.sv_stat in ('P', '1', 'E')
           and claim.from_date >= '2017-01-01'
         ),
    
    FacilityClaims as (
         select patient.patientid, claim.claim_id, paytype1,
                min(Start_date) as Start_Date,
                max(End_Date) as End_Date,
                sum(Paid_amt) as Paid_Amt
         from FacilityClaimLines
         group by patient.patientid, claim.claim_id, paytype1
         ),

ProfClaimLines as (     
         select * , 
                case when claim.sv_line < 10 then concat('0', cast(claim.sv_line as string))
                     else cast(claim.sv_line as string)
                end as sv_line_1,         
                'PROF' as CLAIMTYP2,
                claim.from_date as Start_Date,
                claim.to_date as End_Date,
                claim.from_date as DOS,
                extract(year from claim.from_date)*100 + extract(month from claim.from_date) as DOS_YYYYMM,
                extract(year from claim.paid_date)*100 + extract(month from claim.paid_date) as DOP_YYYYMM,
                cast(claim.amt_allowed as NUMERIC) as Allowed_amt,
                cast(claim.amt_paid as NUMERIC) as Paid_amt,
-- it is only: claim.supergroupID = 'FD'
                case  when claim.sv_stat not in ('P', '1', 'E') then 'Open' 
                      when ((claim.supergroupID is not null and claim.supergroupID = 'FD') 
                              or claim.medicalcenternumber in ('0100', '0108', '0139', '0140', '0141','0142'
                                                               ,'0143', '0145', '0146', '14BR', '14GR', '14RP'
                                                               , '14MM', '14MN', '14MV'
                                                               --, '14MX' -- 3/23
                                                               , '14MY', '14HH'
                                                               --, '14HX' -- 3/23
                                                               , '14HY'
                                                               --, '14HZ' -- 3/23
                                                               )) then 'FD'
                 when claim.PROC_CODE = 'G9005' or claim.REV_CODE = '0500' then 'HH'
                 when substr(claim.medicalcenternumber,0,2) in ('02', '03', '04', '05', '06', '07', '08', '09')
                         and claim.FFSCAPIND = 'C' and claim.responsibleindicator = 'G'
                         and (claim.SupergroupID is null or claim.Supergroupid <> 'FD')
                         then 'ACP'
                 when claim.FFSCAPIND = 'C' or (claim.CL_DATA_SRC is not null and Trim(claim.CL_DATA_SRC) = 'QPN2') 
                           or (claim.CARECORERESPONSIBLEAMT is not null and cast(claim.CARECORERESPONSIBLEAMT as numeric) >0) 
                          then 'CAP'
                      else 'FFS' end as PayType1,
                  claim.POS,
                  case when claim.BILLINGPROVLOCATIONSUFFIX is null then '000' else claim.BILLINGPROVLOCATIONSUFFIX end as PROV_LOC
                  ,claim.CARECORERESPONSIBLEAMT
         from {{ source('emblem_silver', 'professional') }}
         where patient.patientID is not null
           and claim.sv_stat in ('P', '1', 'E')
           and claim.from_date >= '2017-01-01'
         ),
--===============================================================
-- BH
--================================================================
BH_Facility as (
        select * from (        
        select c.claim.claim_id, c.claim.sv_line 
                     , case when ((c.claim.POS = '51' or c.claim.REV_CODE in ('114', '116', '124', '125', '134', '136', '144', '146', '154', '156', '204'))
                           or (c.claim.POS = '21' and substr (c.claim.ICD_DIAG_01,0,1) = 'F' )
                           or c.claim.PROC_CODE in ('90791', '90792', '90832', '90833', '90834', '90836', '90837', '90838', '90839', '90840'
                                            , '90847', '90846', '90853', '96101', '96102', '96103', '96105', '96111', '96116', '96118'
                                            , '96119', '96120', '96150', '96151', '96152', '96153', '96154', '96155'))
                          and c.claim.POS not in ('81','41','42','24','23')
                          and c.claim.PROC_CODE not between '80047' and '89398'
                          and c.claim.PROC_CODE not between '70010' and '79999'
                          and c.claim.PROC_CODE not between '00100' and '01999' then 1
                    else 0
                end as BH_Facility
        from FacilityClaimLines c
        )
        where BH_Facility = 1
        ),
BH_Prof as (
        select * from (        
        select c.claim.claim_id, c.claim.sv_line 
                     , case when ((c.claim.POS = '51' or c.claim.REV_CODE in ('114', '116', '124', '125', '134', '136', '144', '146', '154', '156', '204'))
                           or (c.claim.POS = '21' and substr (c.claim.ICD_DIAG_01,0,1) = 'F' )
                           or c.claim.PROC_CODE in ('90791', '90792', '90832', '90833', '90834', '90836', '90837', '90838', '90839', '90840'
                                            , '90847', '90846', '90853', '96101', '96102', '96103', '96105', '96111', '96116', '96118'
                                            , '96119', '96120', '96150', '96151', '96152', '96153', '96154', '96155'))
                          and c.claim.POS not in ('81','41','42','24','23')
                          and c.claim.PROC_CODE not between '80047' and '89398'
                          and c.claim.PROC_CODE not between '70010' and '79999'
                          and c.claim.PROC_CODE not between '00100' and '01999' then 1
                    else 0
                end as BH_Prof
        from ProfClaimLines c
        )
        where BH_Prof = 1
        ),

--============================================
--SNF SECTION
--============================================
--/*
SNF_cases as (
      select l.patientID, l.claim_ID, l.adm_date, l.SNFlinestart, l.SNFlineDt, l.SNFLineEnd, l.SNFLinePaid,
             f.Start_date as ClaimStart, f.End_Date as ClaimEnd, f.Paid_Amt as ClaimPaid
             from
                  (select patient.patientID, 
                         claim.claim_ID,
                         min(claim.adm_date) as Adm_date,
                         min(start_date) as SNFlineStart,
                         min(claim.from_date) as SNFLineDt,
                         max(End_date) as SNFLineEnd,
                         sum(Paid_Amt) as SNFLinePaid
                  from facilityclaimlines
                  where (substr(claim.ub_bill_type,0,3) = '021' or substr(claim.rev_code, 2,3) = '022'
                  )
                  group by patient.patientID, 
                         claim.claim_ID
                  order by Adm_date) l
             left join FacilityClaims f 
             on l.claim_id = f.claim_id and l.patientid = f.patientID
             ORDER BY l.patientID, ClaimStart
        ),
              
SNF_PROF as
        (
          select distinct p.patientID, p.claim_id, p.Start_date, p.Paid_amt
          from 
              (select patient.patientID, claim.Claim_ID, POS, min(start_date) as Start_date, sum(Paid_amt) as Paid_amt
              from ProfClaimLines
              group by patientID, Claim_ID, POS) p
          inner join SNF_cases s
          on p.patientID = s.patientID
          where ((p.start_date > s.ClaimStart and p.start_date < s.ClaimEnd)
          OR (p.start_date >= s.ClaimStart and p.start_date <= s.ClaimEnd and p.POS = '31'))
          ORDER BY p.patientID, p.Start_date
          ),

--SELECT * from SNF_Cases
--SELECT * from SNF_PROF


--============================================
--IRF SECTION
--============================================
--/*
IRF_CASES as (
        select l.patientID, l.claim_ID, l.adm_date, l.IRFlinestart, l.IRFlineDt, l.IRFLineEnd, l.IRFLinePaid,
             f.Start_date as ClaimStart, f.End_Date as ClaimEnd, f.Paid_Amt as ClaimPaid
             from
                  (select patient.patientID, 
                         claim.claim_ID,
                         min(claim.adm_date) as Adm_date,
                         min(start_date) as IRFlineStart,
                         min(claim.from_date) as IRFLineDt,
                         max(End_date) as IRFLineEnd,
                         sum(Paid_Amt) as IRFLinePaid
                  from facilityclaimlines c
                  left join `emblem-data.silver_claims.providers` p
                  on c.claim.BILL_PROV = p.provider.PROV_ID and c.claim.BILLINGPROVLOCATIONSUFFIX = p.provider.PROV_LOC
                  where substr(claim.ub_bill_type, 0,3) = '011'
                  AND  ((claim.rev_code in ('0118', '0128', '0138', '0148', '0158','0168') 
                                AND provider.PROV_TAXONOMY in ('273Y00000X', '283X00000X'))
                         OR claim.rev_code = '0024')
                  group by patient.patientID, 
                         claim.claim_ID
                  order by Adm_date) l
             left join FacilityClaims f 
             on l.claim_id = f.claim_id and l.patientid = f.patientID
        ),
        
IRF_PROF as
        (
        select distinct p.patientID, p.claim_id, p.Start_date, p.Paid_amt
        from 
            (select patient.patientID, claim.Claim_ID, POS, min(start_date) as Start_date, sum(Paid_amt) as Paid_amt
            from ProfClaimLines
            group by patientID, Claim_ID, POS) p
        inner join IRF_cases i
        on p.patientID = i.patientID
        where ((p.start_date > i.ClaimStart and p.start_date < i.ClaimEnd)
        OR (p.start_date >= i.ClaimStart and p.start_date <= i.ClaimEnd and p.POS in ('61', '21')))
        ),
--SELECT * from IRF_Cases
--SELECT * from IRF_PROF
--*/                
        
--============================================
--LTAC SECTION
--============================================
--/*
LTAC_CASES as (
        select l.patientID, l.claim_ID, l.adm_date, l.LTAClinestart, l.LTAClineDt, l.LTACLineEnd, l.LTACLinePaid,
             f.Start_date as ClaimStart, f.End_Date as ClaimEnd, f.Paid_Amt as ClaimPaid
             from
                  (select patient.patientID, 
                         claim.claim_ID,
                         min(claim.adm_date) as Adm_date,
                         min(start_date) as LTAClineStart,
                         min(claim.from_date) as LTACLineDt,
                         max(End_date) as LTACLineEnd,
                         sum(Paid_Amt) as LTACLinePaid
                  from facilityclaimlines c
                  left join `emblem-data.silver_claims.providers` p
                  on c.claim.BILL_PROV = p.provider.PROV_ID and c.claim.BILLINGPROVLOCATIONSUFFIX = p.provider.PROV_LOC
                  where substr(claim.ub_bill_type, 0,3) = '011' AND provider.PROV_TAXONOMY in ('282E00000X')
                  group by patient.patientID, 
                         claim.claim_ID
                  order by Adm_date) l
             left join FacilityClaims f 
             on l.claim_id = f.claim_id and l.patientid = f.patientID
        ),

LTAC_PROF as
        (
        select distinct p.patientID, p.claim_id, p.Start_date, p.Paid_amt
        from 
            (select patient.patientID, claim.Claim_ID, POS, min(start_date) as Start_date, sum(Paid_amt) as Paid_amt
            from ProfClaimLines
            group by patientID, Claim_ID, POS) p
        inner join LTAC_cases l
        on p.patientID = l.patientID
        where ((p.start_date > l.ClaimStart and p.start_date < l.ClaimEnd)
        OR (p.start_date >= l.ClaimStart and p.start_date <= l.ClaimEnd and p.POS ='21'))
        ),

--SELECT * from LTAC_Cases
--SELECT * from LTAC_PROF

--*/


--============================================
--HOME HEALTH SECTION
--============================================
--/*
HH_Cases as (
      select f.patientID, f.claim_ID, f.Start_date as ClaimStart, f.End_Date as ClaimEnd, f.Paid_Amt as ClaimPaid
      from facilityclaims f
      inner join 
            (select a.claim_id
            from 
                (select distinct claim.claim_id
                    from facilityclaimlines
                    where substr(claim.ub_bill_type, 0,2) = '03') a
                LEFT JOIN
                (select distinct claim.claim_id
                    from facilityclaimlines
                    where substr(claim.ub_bill_type, 0,2) = '03'
                    AND (claim.rev_code in ('0580')
                         OR claim.PROC_CODE in ('T1019','T1020', 'S5130', 'S9123', 'S9124')
                         OR (claim.rev_code in ('0500') AND claim.PROC_CODE in ('G9001', 'T2022', 'G0506', 'G9005'))
                )) b
            ON a.claim_id = b.claim_id
            where b.claim_id is null) c
      on f.claim_id = c.claim_id
      ),

--SELECT * from HH_Cases
--*/


--============================================
--HOME DME SECTION
--============================================
--/*
HomeDME_Cases as (
      select f.patient.patientID, f.claim.claim_ID,  f.claim.sv_line, f.Start_date as ClaimStart, f.End_Date as ClaimEnd, f.Paid_Amt as ClaimPaid
      from profclaimlines f
      where POS = '12'
      AND (claim.Proc_code in ('S1040', 'A0020') 
           OR substr(claim.Proc_code, 0, 1) in ('E', 'K', 'L')
           OR substr(claim.Proc_code, 0, 4) in ('A000', 'A001')
           OR (substr(claim.Proc_code, 0, 1) = 'A' and substr(claim.Proc_code, 0, 2) <> 'A0'))
      ),

--============================================
--PT/OT SECTION
--============================================
--/*
PT_OT_Cases as (
      (select f.patient.patientID, f.claim.claim_ID, f.claim.sv_line, f.Start_date as ClaimStart, f.End_Date as ClaimEnd, f.Paid_Amt as ClaimPaid
      from facilityclaimlines f
      where f.claim.POS in ('11', '19', '22')
      AND claim.Proc_code in ('97010', '97012', '97014', '97016', '97018', '97022', '97024', '97026', '97028', '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', '97124', '97139', '97140', '97150', '97161', '97162', '97163', '97164', '97165', '97166', '97167', '97168', '97530', '97533', '97535', '97542', '97545', '97546', '97750', '97760')
      and DOS_YYYYMM < 202002
      )
      UNION ALL
       (select f.patient.patientID, f.claim.claim_ID, f.claim.sv_line, f.Start_date as ClaimStart, f.End_Date as ClaimEnd, f.Paid_Amt as ClaimPaid
      from profclaimlines f
      where f.claim.POS in ('11', '19', '22')
      AND claim.Proc_code in ('97010', '97012', '97014', '97016', '97018', '97022', '97024', '97026', '97028', '97032', '97033', '97034', '97035', '97036', '97039', '97110', '97112', '97113', '97116', '97124', '97139', '97140', '97150', '97161', '97162', '97163', '97164', '97165', '97166', '97167', '97168', '97530', '97533', '97535', '97542', '97545', '97546', '97750', '97760')
      and DOS_YYYYMM < 202002
      )  
      ),
--SELECT * from PT_OT_Casses

--============================================
--CHIRO SECTION
--RELIES ON LINK TO PROVIDER TABLE WHICH IS NOT PERFECT
--ALSO DOFR INDICATES PALLADIAN SHOULD BE PROCESSING CLAIMS
--============================================
--/*
Chiro_Cases as (
      select f.claim.BILL_PROV, f.claim.BILLINGPROVLOCATIONSUFFIX, f.patient.patientID, f.claim.claim_ID, f.claim.sv_line, f.Start_date as ClaimStart, f.End_Date as ClaimEnd, f.Paid_Amt as ClaimPaid
      from profclaimlines f inner join
            (select provider.PROV_ID, provider.PROV_LOC, provider.prov_lname, provider.prov_fname
              from `emblem-data.silver_claims.providers`
              where provider.PROV_SPEC_DESC = 'Chiropractic') p
      on f.claim.BILL_PROV = p.PROV_ID and f.PROV_LOC = p.PROV_LOC
      where POS = '11'
      AND claim.Proc_code in ('71110', '71120', '71130', '72010', '72020', '72052', '72069', '72070', '72072', '72074', '72080', '72100', '72110', '72114', '72120', '72170', '72190', '72200', '72202', '72220', '72040', '72050', '73000', '73010', '73020', '73030', '73050', '73060', '73070', '73080', '73090', '73100', '73110', '73120', '73130', '73140', '73500', '73510', '73520', '73550', '73560', '73562', '73564', '73565', '73590', '73600', '73610', '73620', '73630', '73650', '73660', '99201', '99202', '99203', '99204', '99205', '99211', '99212', '99213', '99214', '99215', '98940', '98941', '98942')
      ),

--============================================
-- CLAIMS DETAILS SECTION
--============================================
--/*
claims as (
        select patient.patientID
             , claim.MEMBER_ID
--             , case when claim.ICD_DIAG_01 in ('N186','N185') or claim.ICD_DIAG_ADMIT in ('N186','N185') then 'ESRD' else '' end as bucket_esrd
             , claim.claim_id
             , claim.sv_line
             , claim.sv_stat
             , sv_line_1
             , claim.att_prov
             , claim.bill_prov
             , CLAIMTYP2
             , DOS
             , DOS_YYYYMM
             , DOP_YYYYMM
             , Allowed_amt
             , Paid_amt
             , 0 as rx_admin_fee
             , PayType1
             , carecoreresponsibleamt
        from FacilityClaimLines
        union all
        select patient.patientID
             , claim.MEMBER_ID
--             , case when claim.ICD_DIAG_01 in ('N186','N185') or claim.ICD_DIAG_ADMIT in ('N186','N185') then 'ESRD' else '' end as bucket_esrd
             , claim.claim_id
             , claim.sv_line
             , claim.sv_stat
             , sv_line_1
             , claim.att_prov
             , claim.bill_prov
             , CLAIMTYP2
             , DOS
             , DOS_YYYYMM
             , DOP_YYYYMM
             , Allowed_amt
             , Paid_amt
             , 0 as rx_admin_fee
             , PayType1
             , carecoreresponsibleamt
        from profclaimlines
        union all
        select rx.patient.patientId
             , rx.claim.MEMBER_ID
--             , '' as bucket_esrd
             , rx.claim.claim_id
             , claim.sv_line
             , rx.claim.sv_stat
             , null as sv_line_1
             , rx.claim.att_prov
             , rx.claim.bill_prov
             , 'Rx' as CLAIMTYP2
             , date(cast(substr(cast(claim.ServiceyearMonth as string),1,4) as INT64), cast(substr(cast(claim.ServiceyearMonth as string),5,2) as INT64),1) as DOS
             , cast(rx.claim.ServiceyearMonth as INT64) as DOS_YYYYMM
             , extract(year from rx.claim.paid_date)*100 + extract(month from rx.claim.paid_date) as DOP_YYYYMM
             , cast(rx.claim.amt_allowed as NUMERIC) as allowed
             , cast(rx.claim.amt_paid as NUMERIC) as paid
             , cast(rx.claim.CLAIMADMINFEEAMT as NUMERIC) as Rx_Admin
             , 'FFS' as PayType1
             , null as carecoreresponsibleamt
        from {{ source('emblem_silver', 'pharmacy') }} rx
        where rx.patient.patientid is not null
          and (rx.claim.CL_DATA_SRC is null or trim(rx.claim.CL_DATA_SRC) <> 'QPN2')
        ),

claims_with_carve_out_flags as (
        select c.*
             , case when c.PayType1 = 'Open' then 'Open'
                    when sc.claim_id is not null then 'CAP'
                    when sp.claim_id is not null then 'CAP'
                    when ic.claim_id is not null then 'CAP'
                    when ip.claim_id is not null then 'CAP'
                    when lc.claim_id is not null then 'CAP'
                    when lp.claim_id is not null then 'CAP'
                    when hhc.claim_id is not null then 'CAP'
                    when hdme.claim_id is not null then 'CAP'
                    when chiro.claim_id is not null then 'CAP'
                    when pt.claim_id is not null then 'CAP'
                    when bhf.claim_id is not null then 'CAP'
                    when bhp.claim_id is not null then 'CAP'
                    else c.PayType1
               end as PayType2
             , case when sc.claim_id is not null then 1 else 0 end as SNF_Cases
             , case when sp.claim_id is not null then 1 else 0 end as SNF_PROF
             , case when ic.claim_id is not null then 1 else 0 end as IRF_Cases
             , case when ip.claim_id is not null then 1 else 0 end as IRF_PROF
             , case when lc.claim_id is not null then 1 else 0 end as LTAC_Cases
             , case when lp.claim_id is not null then 1 else 0 end as LTAC_PROF
             , case when hhc.claim_id is not null then 1 else 0 end as HH_Cases
             , case when hdme.claim_id is not null then 1 else 0 end as HDME_Cases
             , case when chiro.claim_id is not null then 1 else 0 end as Chiro_2020_Cases
             , case when pt.claim_id is not null then 1 else 0 end as Chiro_Prior_Cases
             , case when bhf.claim_id is not null then 1 else 0 end as BH_Facility
             , case when bhp.claim_id is not null then 1 else 0 end as BH_Prof
        from claims c
        left join (SELECT distinct claim_id from SNF_Cases) sc
          on c.claim_id = sc.claim_id
        left join (SELECT distinct claim_id from SNF_PROF) sp
          on c.claim_id = sp.claim_id
        left join (SELECT distinct claim_id from IRF_Cases) ic
          on c.claim_id = ic.claim_id
        left join (SELECT distinct claim_id from IRF_PROF) ip
          on c.claim_id = ip.claim_id
        left join (SELECT distinct claim_id from LTAC_Cases) lc
          on c.claim_id = lc.claim_id
        left join (SELECT distinct claim_id from LTAC_PROF) lp
          on c.claim_id = lp.claim_id
        left join (SELECT distinct claim_id from HH_Cases) hhc
          on c.claim_id = hhc.claim_id
        left join (SELECT distinct claim_id, sv_line from HomeDME_Cases) hdme
          on c.claim_id = hdme.claim_id
          and c.sv_line= hdme.sv_line -- added 3/25
        left join (SELECT distinct claim_id, sv_line from PT_OT_Cases) pt
          on c.claim_id = pt.claim_id
          and c.sv_line = pt.sv_line
--          and c.DOS_YYYYMM < 202002
        left join (SELECT distinct claim_id, sv_line from Chiro_Cases) chiro
          on c.claim_id = chiro.claim_id
          and c.sv_line = chiro.sv_line
--          and c.DOS_YYYYMM >= 202002
        left join (SELECT distinct claim_id, sv_line from BH_Facility) bhf
          on c.claim_id = bhf.claim_id
          and c.sv_line = bhf.sv_line
        left join (SELECT distinct claim_id, sv_line from BH_Prof) bhp
          on c.claim_id = bhp.claim_id
          and c.sv_line = bhp.sv_line
        )

select * from claims_with_carve_out_flags





