package cityblock.importers.emblem.members

import cityblock.parsers.emblem.members.MemberDemographic.ParsedMemberDemographic
import cityblock.parsers.emblem.members.MemberDemographicCohort.ParsedMemberDemographicCohort

object MemberDemographics {

  def compareStringDates(xDate: Option[String], yDate: Option[String]): Int =
    xDate match {
      case Some(xdate) =>
        yDate match {
          case Some(ydate) => xdate.compare(ydate)
          case _           => 1
        }
      case _ =>
        yDate match {
          case Some(ydate) => -1
          case _           => 0
        }
    }

  def toGeneralDemographics(cohortDemo: ParsedMemberDemographicCohort): ParsedMemberDemographic =
    ParsedMemberDemographic(
      MEM_START_DATE = cohortDemo.MEM_START_DATE,
      MEM_END_DATE = cohortDemo.MEM_END_DATE,
      MEMBER_ID = cohortDemo.MEMBER_ID,
      MEMBER_QUAL = cohortDemo.MEMBER_QUAL,
      PERSON_ID = cohortDemo.PERSON_ID,
      MEM_GENDER = cohortDemo.MEM_GENDER,
      MEM_SSN = cohortDemo.MEM_SSN,
      MEM_LNAME = cohortDemo.MEM_LNAME,
      MEM_FNAME = cohortDemo.MEM_FNAME,
      MEM_MNAME = cohortDemo.MEM_MNAME,
      MEM_DOB = cohortDemo.MEM_DOB,
      MEM_DOD = cohortDemo.MEM_DOD,
      MEM_MEDICARE = cohortDemo.MEM_MEDICARE,
      MEM_ADDR1 = cohortDemo.MEM_ADDR1,
      MEM_ADDR2 = cohortDemo.MEM_ADDR2,
      MEM_CITY = cohortDemo.MEM_CITY,
      COUNTY = cohortDemo.COUNTY,
      MEM_STATE = cohortDemo.MEM_STATE,
      MEM_ZIP = cohortDemo.MEM_ZIP,
      MEM_EMAIL = cohortDemo.MEM_EMAIL,
      MEM_PHONE = cohortDemo.MEM_PHONE,
      MEM_RACE = cohortDemo.MEM_RACE,
      MEM_ETHNICITY = cohortDemo.MEM_ETHNICITY,
      MEM_LANGUAGE = cohortDemo.MEM_LANGUAGE,
      MEM_DATA_SRC = cohortDemo.MEM_DATA_SRC,
      HIRE_DATE = cohortDemo.HIRE_DATE,
      NMI = cohortDemo.NMI,
      MARITALSTATUS = cohortDemo.MARITALSTATUS,
      SERVICEYEARMONTH = None // not included in CB_COHORT file
    )
}
