with member as (

    select *
    from cityblock-analytics.mrt_commons.member
),

carePlan as (

  select patientId,
  max(goalCreatedAt) as carePlanCreatedAt,
  max(goalUpdatedAt) as carePlanUpdatedAt,
  max(goalCompletedAt) as carePlanClosedAt

  from `cityblock-analytics.mrt_commons.member_goals_tasks`
  group by patientId

),

base as (

select
--Might need the below columns back
-- "BLU" as PhpId,
--        "Healthy Blue" as PhpName,
--        "F" as load,
--        concat("NCMT_CareQualityManagement_AMH_PatientListRiskScore_BCBS_CB_", cast(format_date('%Y%m%d',current_date()) as string), ".txt") as fileName,
--        "D" as fileType,
--        "1.0" as version,
--        REGEXP_REPLACE(cast(current_date() as string), "-", "") as createDate,
--        EXTRACT(time FROM current_timestamp()) as createTime,
        data.Maintenance_Type_Code as maintenanceTypeCode,
        coalesce(data.CNDS_ID,memberId) as CNDSId,
        data.Priority_Population_1 as priorityPopulation1,
        data.Priority_Population_2 as priorityPopulation2,
        data.Priority_Population_3 as priorityPopulation3,
        data.Priority_Population_4 as priorityPopulation4,
        data.Priority_Population_5 as priorityPopulation5,
        data.Priority_Population_6 as priorityPopulation6,
        data.PHP_Risk_Score_Category as pHPRiskScoreCategory,
        data.PHP_Risk_Evidence as pHPRiskEvidence,
        case when cf.fieldValue = "true" or cfp.fieldValue = "true" then "H"
             when m.currentCareModel = "virtualIntegratedCare" then "M"
             when m.currentCareModel = "community" and cf.fieldValue = "false"  then "L"
             else "N"
        end as cmEntitiyRiskScoreCategory,
        "1760049308" as npi,
        mc.maxComprehensiveAssessmentAt,
        carePlanCreatedAt,
        carePlanUpdatedAt,
        carePlanClosedAt,

from member m
left join `cbh-healthyblue-data.silver_claims.PatientRiskList_20210618` p
  on m.memberId = p.data.CNDS_ID
left join `cityblock-analytics.abs_computed.cf_complex_care_management` cf
  using (patientId)
left join `cityblock-analytics.abs_computed.cf_complex_care_management_pediatric` cfp
  using (patientId)
left join `cityblock-analytics.mrt_commons.member_commons_completion` mc
 using (patientId)
left join carePlan
 using (patientId)

 where partnerName = "Healthy Blue" and
       patientHomeMarketName = "North Carolina"
),

CM_interactions_raw as (

  select m.memberId,
          case when groupedModality in ("text", "smsMessage", "phoneCall", "videoCall", "inPersonVisitOrMeeting", "hub", "ehr" )

                  then id
               else null
          end as successfulCmInteractionIds,

          case when groupedModality in ("inPersonVisitOrMeeting", "videoCall" )
                  then id
               else null
          end as successfulf2fEncounterIds,



from  member m
left join  `cityblock-analytics.abs_notes.abs_commons_notes`  n
using (patientId)

where lower(n.status) = "success" and
      lower(n.attendees) = "member" and partnerName = "Healthy Blue"
      and patientHomeMarketName = "North Carolina"

),

CM_interactions_count as (

 select memberId,
       count(distinct successfulCmInteractionIds) as numberOfCmInteractions,
       count(distinct successfulf2fEncounterIds) as numberOfF2FEncountersCount
 from CM_interactions_raw
 group by 1
),


final as (

select
   CNDSId,
   maintenanceTypeCode,
   priorityPopulation1,
   priorityPopulation2,
   priorityPopulation3,
   priorityPopulation4,
   priorityPopulation5,
   priorityPopulation6,
   pHPRiskScoreCategory,
   pHPRiskEvidence,
   cmEntitiyRiskScoreCategory,
   npi as assignedCMEntity,
   numberOfCmInteractions,
   numberOfF2FEncountersCount,
   maxComprehensiveAssessmentAt,
   carePlanCreatedAt,
   carePlanUpdatedAt,
   carePlanClosedAt


from base b
left join
CM_interactions_count cm
on b.CNDSId = cm.memberId

)


select * from final






