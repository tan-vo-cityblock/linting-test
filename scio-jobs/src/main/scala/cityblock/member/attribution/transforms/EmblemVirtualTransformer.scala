package cityblock.member.attribution.transforms

import cityblock.member.service.api.{
  Address,
  Demographics,
  Detail,
  Email,
  Insurance,
  MemberCreateAndPublishRequest,
  Phone,
  Plan,
}
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.member.silver.ElationMappings
import cityblock.member.silver.EmblemVirtual.EmblemVirtualMember
import org.joda.time.LocalDate
import org.joda.time.format.ISODateTimeFormat

object EmblemVirtualTransformer {
  private val TODAY: LocalDate = LocalDate.now()

  def createAndPublishRequest(
    member: EmblemVirtualMember,
    multiPartnerIds: MultiPartnerIds,
    insuranceName: String,
    cohortId: Option[Int]
  ): MemberCreateAndPublishRequest = {
    val address = Address(
      id = None,
      addressType = None,
      street1 = member.MEM_ADDR1.filter(_ != "N/A"),
      street2 = member.MEM_ADDR2.filter(_ != "N/A"),
      city = member.MEM_CITY,
      zip = zipCodeTransform(member.MEM_ZIP),
      county = member.COUNTY,
      state = member.MEM_STATE,
      spanDateStart = None,
      spanDateEnd = None
    )

    val phones =
      member.MEM_PHONE
        .filter(_ != "N/A")
        .map(phone => Phone(id = None, phone = phone, phoneType = Option("main")))
        .toList

    val emails =
      member.MEM_EMAIL.filter(_ != "N/A").map(email => Email(id = None, email = email)).toList

    val demographics = Demographics(
      firstName = member.MEM_FNAME,
      middleName = member.MEM_MNAME,
      lastName = member.MEM_LNAME,
      dateOfBirth = member.MEM_DOB.map(_.toString),
      dateOfDemise = dateOfDemiseTransform(member.MEM_DOD).map(_.toString),
      sex = member.MEM_GENDER.map(genderTransform),
      gender = member.MEM_GENDER.map(genderTransform),
      ethnicity = member.MEM_ETHNICITY.map(ethnicityTransform),
      race = member.MEM_RACE.map(raceTransform),
      language = member.MEM_LANGUAGE.map(languageTransform),
      maritalStatus = member.MARITALSTATUS.map(maritalStatusTransform),
      ssn = member.MEM_SSN,
      ssnLastFour = member.MEM_SSN.map(_.takeRight(4)),
      addresses = List(address),
      phones = phones,
      emails = emails,
      updatedBy = None
    )

    // TODO: We need to modify this packaging of insurance data once the query is modified to aggregate history
    val plan = Plan(
      externalId = member.MEMBER_ID,
      rank = None,
      current = Option(isCurrent(member.MEM_START_DATE, member.MEM_END_DATE)),
      details = List(
        Detail(
          lineOfBusiness = None,
          subLineOfBusiness = None,
          spanDateStart = member.MEM_START_DATE.map(ISODateTimeFormat.yearMonthDay().print),
          spanDateEnd = member.MEM_END_DATE.map(ISODateTimeFormat.yearMonthDay().print),
        ))
    )

    val insurance = Insurance(carrier = insuranceName, plans = List(plan))

    MemberCreateAndPublishRequest(
      demographics = demographics,
      partner = Option(insuranceName),
      insurances = List(insurance),
      medicaid = List.empty[Insurance],
      medicare = List.empty[Insurance],
      pcpAddress = None,
      pcpName = None,
      pcpPhone = None,
      pcpPractice = None,
      cohortId = cohortId,
      categoryId = None,
      primaryPhysician = Option(ElationMappings.emblemVirtualPrimaryPhysician),
      caregiverPractice = Option(ElationMappings.emblemVirtualCaregiverPractice),
      elationPlan = None,
      createElationPatient = Option(true),
      marketId = multiPartnerIds.market,
      partnerId = multiPartnerIds.partner,
      clinicId = multiPartnerIds.clinic
    )
  }

  private def isCurrent(
    spanDateStart: Option[LocalDate],
    spanDateEnd: Option[LocalDate]
  ): Boolean = {
    val noSpanStart = spanDateStart.isEmpty
    val noSpanEnd = spanDateEnd.isEmpty
    val spanEndBeforeToday = spanDateEnd.get.compareTo(TODAY) <= 0

    noSpanStart || noSpanEnd || !spanEndBeforeToday
  }

  private def zipCodeTransform(zipCode: Option[String]): Option[String] =
    zipCode.filter(_.length >= 5).map(_.take(5))

  private def raceTransform(raceCode: String): String = raceCode match {
    case "1" => "White"
    case "2" => "Black or African American"
    case "3" => "American Indian and Alaska Native"
    case "4" => "Asian"
    case "5" => "Native Hawaiian and Other Pacific Islander"
    case "6" => "Some Other Race"
    case "7" => "Two or More Races"
    case _   => "Unknown Race"
  }

  private def languageTransform(languageCode: String): String = languageCode match {
    case "ENG"                                 => "English"
    case "SPA"                                 => "Spanish"
    case "ITA" | "FRA" | "RUS" | "GRE" | "POL" => "Other Indo-European Language"
    case "KOR" | "CHI" | "VIE" | "JPN" | "TAI" => "Asian and Pacific Island Languages"
    case _                                     => "Spoken Language Unknown"
  }

  private def ethnicityTransform(ethnicityCode: String): String = ethnicityCode match {
    case "1" => "Hispanic or Latino"
    case "2" => "Not Hispanic or Latino"
    case _   => "Unknown Ethnicity"
  }

  private def genderTransform(gender: String): String = gender match {
    case "M" => "male"
    case "F" => "female"
    case _   => ""
  }

  private def maritalStatusTransform(maritalStatus: String): String = maritalStatus match {
    case "S" => "neverMarried"
    case "U" => "unmarried"
    case "W" => "widowed"
    case "M" => "currentlyMarried"
    case "D" => "divorced"
    case "T" => "domesticPartner"
    case _   => "unknown"
  }

  private def dateOfDemiseTransform(dateOfDemise: Option[LocalDate]): Option[LocalDate] =
    dateOfDemise.filter(_.isBefore(new LocalDate()))

}
