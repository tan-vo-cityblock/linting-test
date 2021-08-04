with reporting_month as (
  select
  /*
  Pulls the first and last date of the previous month for the rest of the SLA report
  To change replace date statements with desired start and end dates in YYYY-MM-DD format
  For example: 2020-03-01 as tuftsSlaReportStartDate
  */
  date_sub(date_trunc(current_date(), month), interval 1 month) as tuftsSlaReportStartDate,
  date_sub(date_trunc(current_date(), month), interval 1 day) as tuftsSlaReportEndDate

),

sla_details as (

  select slaId, slaMet
  from {{ ref('thpp_sla_master_details') }}

),

sla_num_den as (
  select
  
  -- Reporting Months
  tuftsSlaReportStartDate,
  tuftsSlaReportEndDate,
  
  {# SLAs with no tracking or manual tracking #}
  {% for sla_id in ['1', '24'] %}

    0 as sla{{ sla_id }}Num,
    0 as sla{{ sla_id }}Den,

  {% endfor %}
  
  {% for sla_id in [
      '2', '3', '4', '5', '6', '6a', '7', '8', '9', '10', '11', '12', '13', '14', '15',
      '15v2', '16', '17', '18', '19', '20', '21', '22', '23', '25', '26', '27', '28'
    ]
  %}

    ( select count(*) 
      from sla_details
      where slaId = '{{ sla_id }}' and slaMet
    ) as sla{{ sla_id }}Num,

    ( select count(*) 
      from sla_details
      where slaId = '{{ sla_id }}'
    ) as sla{{ sla_id }}Den,
  
  {% endfor %}

  from reporting_month
  
),

calculations as (
  select

    -- SLA Report Month
    concat(cast(tuftsSlaReportStartDate as string),' to ',cast(tuftsSlaReportEndDate as string)) as tuftsSlaReportMonth,

    concat(sla1Num,'/',sla1Den) as sla1Calculation,
    concat(sla2Num,'/',sla2Den) as sla2Calculation,
    concat(sla3Num,'/',sla3Den) as sla3Calculation,
    concat(sla4Num,'/',sla4Den) as sla4Calculation,
    concat(sla5Num,'/',sla5Den) as sla5Calculation,
    concat(sla6Num,'/',sla6Den) as sla6Calculation,
    concat(sla6aNum,'/',sla6aDen) as sla6aCalculation,
    concat(sla7Num,'/',sla7Den) as sla7Calculation,
    concat(sla8Num,'/',sla8Den) as sla8Calculation,
    concat(sla9Num,'/',sla9Den) as sla9Calculation,
    concat(sla10Num,'/',sla10Den) as sla10Calculation,
    concat(sla11Num,'/',sla11Den) as sla11Calculation,
    concat(sla12Num,'/',sla12Den) as sla12Calculation,
    concat(sla13Num,'/',sla13Den) as sla13Calculation,
    concat(sla14Num,'/',sla14Den) as sla14Calculation,
    concat(sla15Num,'/',sla15Den) as sla15Calculation1,
    concat(sla15v2Num,'/',sla15v2Den) as sla15Calculation2,
    concat(sla16Num,'/',sla16Den) as sla16Calculation,
    concat(sla17Num,'/',sla17Den) as sla17Calculation,
    concat(sla18Num,'/',sla18Den) as sla18Calculation,
    concat(sla19Num,'/',sla19Den) as sla19Calculation,
    concat(sla20Num,'/',sla20Den) as sla20Calculation,
    concat(sla21Num,'/',sla21Den) as sla21Calculation,
    concat(sla22Num,'/',sla22Den) as sla22Calculation,
    concat(sla23Num,'/',sla23Den) as sla23Calculation,
    concat(sla24Num,'/',sla24Den) as sla24Calculation,
    concat(sla25Num,'/',sla25Den) as sla25Calculation,
    concat(sla26Num,'/',sla26Den) as sla26Calculation,
    concat(sla27Num,'/',sla27Den) as sla27Calculation,
    concat(sla28Num,'/',sla28Den) as sla28Calculation,

    case
        when sla1Den = 0 then 'N/A'
        else cast(sla1Num/sla1Den as string)
    end as sla1OutreachIn48HoursFromInpatientTransfer,

    case
        when sla2Den = 0 then 'N/A'
        else cast(sla2Num/sla2Den as string)
    end as sla2OutreachIn48HoursFromFacilityDischarge,

    case
        when sla3Den = 0 then 'N/A'
        else cast(sla3Num/sla3Den as string)
    end as sla3OutreachIn24HoursFromAcuteHospitalAdmission,

    case
        when sla4Den = 0 then 'N/A'
        else cast(sla4Num/sla4Den as string)
    end as sla4OutreachIn30DaysOfBecomingMember,

    case
        when sla5Den = 0 then 'N/A'
        else cast(sla5Num/sla5Den as string)
    end as sla5OutreachIn30DaysOfAttribution,

    case
        when sla6Den = 0 then 'N/A'
        else cast(sla6Num/sla6Den as string)
    end as sla6BothTuftsAssessmentsCompletedIn1YearOfReassessment,

    case
        when sla6aDen = 0 then 'N/A'
        else cast(sla6aNum/sla6aDen as string)
    end as sla6aBothTuftsAssessmentsCompletedIn1YearOfReassessment,

    case
        when sla7Den = 0 then 'N/A'
        else cast(sla7Num/sla7Den as string)
    end as sla7AppropriateOutreachAttemptsOfUnableToReachMembersWithReassassment,

    case
        when sla8Den = 0 then 'N/A'
        else cast(sla8Num/sla8Den as string)
    end as sla8OutreachAttemptForUnableToReachMembersIn30DaysBeforeAssessmentDue,

    case
        when sla9Den = 0 then 'N/A'
        else cast(sla9Num/sla9Den as string)
    end as sla9AppropriateOutreachAttemptsOfMembersWhoRefusedReassessmentIn90DaysOfEnrollment,

    case
        when sla10Den = 0 then 'N/A'
        else cast(sla10Num/sla10Den as string)
    end as sla10CarePlanCompletedIn90daysDuringAssessment,

    case
        when sla11Den = 0 then 'N/A'
        else cast(sla11Num/sla11Den as string)
    end as sla11CarePlanIntiatedForHardToReachMembers,

    case
        when sla12Den = 0 then 'N/A'
        else cast(sla12Num/sla12Den as string)
    end as sla12IntialCarePlanAgreementAmongMembers,

    case
        when sla13Den = 0 then 'N/A'
        else cast(sla13Num/sla13Den as string)
    end as sla13AnnualCarePlanReviewOfMembersDueForReassessment,

    case
        when sla14Den = 0 then 'N/A'
        else cast(sla14Num/sla14Den as string)
    end as sla14AnnualCarePlanAgreementAmongMembers,

    case
        when sla15Den = 0 then 'N/A'
        else cast(sla15Num/sla15Den as string)
    end as sla15ComprehensiveAssessmentCompletedIn90DaysOfEnrollment,


    case
        when sla15v2Den = 0 then 'N/A'
        else cast(sla15v2Num/sla15v2Den as string)
    end as sla15BothTuftsAssessmentsCompletedIn90DaysOfEnrollment,

    case
        when sla16Den = 0 then 'N/A'
        else cast(sla16Num/sla16Den as string)
    end as sla16AppropriateOutreachAttemptsOfUnableToReachMembersIn90DaysOfEnrollment,

    case
        when sla17Den = 0 then 'N/A'
        else cast(sla17Num/sla17Den as string)
    end as sla17AppropriateOutreachAttemptsOfMembersWhoRefusedAnnualAssessmentIn90DaysOfEnrollment,

    case
        when sla18Den = 0 then 'N/A'
        else cast(sla18Num/sla18Den as string)
    end as sla18AppropriateOutreachAttemptsOfMembersWhoRefusedAnnualAssessmentAndHaveCarePlanDevelopmentIn90DaysOfEnrollment,

    case
        when sla19Den = 0 then 'N/A'
        else cast(sla19Num/sla19Den as string)
    end as sla19OutreachAttemptForHardToReachMembersEveryMonth,

    case
        when sla20Den = 0 then 'N/A'
        else cast(sla20Num/sla20Den as string)
    end as sla20OutreachAttemptForMembersWhoRefusedAssessmentOrCarePlanEveryMonth,

    case
        when sla21Den = 0 then 'N/A'
        else cast(sla21Num/sla21Den as string)
    end as sla21CarePlanIntiatedForMembersWhoRefusedAssessment,

    case
        when sla22Den = 0 then 'N/A'
        else cast(sla22Num/sla22Den as string)
    end as sla22LtscOfferedForMembersAssessedInCurrentMonth,

    case
        when sla23Den = 0 then 'N/A'
        else cast(sla23Num/sla23Den as string)
    end as sla23LtscInvitedToAssessmentForMembersAssessedInCurrentMonth,

    case
        when sla24Den = 0 then 'N/A'
        else cast(sla24Num/sla24Den as string)
    end as sla24ChangeInConditionAssessment,

    case
        when sla25Den = 0 then 'N/A'
        else cast(sla25Num/sla25Den as string)
    end as sla25OutreachIn48HoursFromEmergencyDepartmentDischarge,
    
    case
        when sla26Den = 0 then 'N/A'
        else cast(sla26Num/sla26Den as string)
    end as sla26OutreachIn48HoursFromInpatientDischarge,

    case
        when sla27Den = 0 then 'N/A'
        else cast(sla26Num/sla26Den as string)
    end as sla27AppropriateOutreachAttemptsOfMembersWhoRefusedIntialAssessmentIn90DaysOfEnrollment,
    
      case
        when sla28Den = 0 then 'N/A'
        else cast(sla26Num/sla26Den as string)
    end as sla28AppropriateOutreachAttemptsOfMembersWhoRefusedIntialAssessmentAndHaveCarePlanDevelopmentIn90DaysOfEnrollment,
    
    
  from sla_num_den

),

final as (
    select
        tuftsSlaReportMonth,

        sla1Calculation,
        sla1OutreachIn48HoursFromInpatientTransfer,

        sla2Calculation,
        sla2OutreachIn48HoursFromFacilityDischarge,
        
        sla3Calculation,
        sla3OutreachIn24HoursFromAcuteHospitalAdmission,

        sla4Calculation,
        sla4OutreachIn30DaysOfBecomingMember,

        sla5Calculation,
        sla5OutreachIn30DaysOfAttribution,
        
        sla6Calculation,
        sla6BothTuftsAssessmentsCompletedIn1YearOfReassessment,
        
        sla6aCalculation,
        sla6aBothTuftsAssessmentsCompletedIn1YearOfReassessment,

        sla7Calculation,
        sla7AppropriateOutreachAttemptsOfUnableToReachMembersWithReassassment,
        
        sla8Calculation,
        sla8OutreachAttemptForUnableToReachMembersIn30DaysBeforeAssessmentDue,

        sla9Calculation,
        sla9AppropriateOutreachAttemptsOfMembersWhoRefusedReassessmentIn90DaysOfEnrollment,

        sla10Calculation,
        sla10CarePlanCompletedIn90daysDuringAssessment,

        sla11Calculation,
        sla11CarePlanIntiatedForHardToReachMembers,

        sla12Calculation,
        sla12IntialCarePlanAgreementAmongMembers, 

        sla13Calculation,
        sla13AnnualCarePlanReviewOfMembersDueForReassessment,

        sla14Calculation,
        sla14AnnualCarePlanAgreementAmongMembers,

        sla15Calculation1,
        sla15ComprehensiveAssessmentCompletedIn90DaysOfEnrollment,

        sla15Calculation2,
        sla15BothTuftsAssessmentsCompletedIn90DaysOfEnrollment,

        sla16Calculation,
        sla16AppropriateOutreachAttemptsOfUnableToReachMembersIn90DaysOfEnrollment,

        sla17Calculation,
        sla17AppropriateOutreachAttemptsOfMembersWhoRefusedAnnualAssessmentIn90DaysOfEnrollment,

        sla18Calculation,
        sla18AppropriateOutreachAttemptsOfMembersWhoRefusedAnnualAssessmentAndHaveCarePlanDevelopmentIn90DaysOfEnrollment,

        sla19Calculation,
        sla19OutreachAttemptForHardToReachMembersEveryMonth,

        sla20Calculation,
        sla20OutreachAttemptForMembersWhoRefusedAssessmentOrCarePlanEveryMonth,

        sla21Calculation,
        sla21CarePlanIntiatedForMembersWhoRefusedAssessment,

        sla22Calculation,
        sla22LtscOfferedForMembersAssessedInCurrentMonth,

        sla23Calculation,
        sla23LtscInvitedToAssessmentForMembersAssessedInCurrentMonth,

        sla24Calculation,
        sla24ChangeInConditionAssessment,

        sla25Calculation,
        sla25OutreachIn48HoursFromEmergencyDepartmentDischarge,
        
        sla26Calculation,
        sla26OutreachIn48HoursFromInpatientDischarge,

        sla27Calculation,
        sla27AppropriateOutreachAttemptsOfMembersWhoRefusedIntialAssessmentIn90DaysOfEnrollment,

        sla28Calculation,
        sla28AppropriateOutreachAttemptsOfMembersWhoRefusedIntialAssessmentAndHaveCarePlanDevelopmentIn90DaysOfEnrollment,

    from calculations

)

select * from final