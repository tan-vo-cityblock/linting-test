package cityblock.member.attribution.transforms

import cityblock.member.service.api.{
  Address,
  Demographics,
  Detail,
  Email,
  Insurance,
  MemberCreateAndPublishRequest,
  Phone,
  Plan
}
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.member.silver.CareFirst.{CareFirstInsuranceDetails, CareFirstMember}
import cityblock.member.silver.ElationMappings
import org.joda.time.LocalDate
import org.joda.time.format.ISODateTimeFormat

object CareFirstTransformer {
  private val TODAY: LocalDate = LocalDate.now()

  def createAndPublishRequest(
    member: CareFirstMember,
    multiPartnerIds: MultiPartnerIds,
    insuranceName: String,
    cohortId: Option[Int]
  ): MemberCreateAndPublishRequest = {
    val address = Address(
      id = None,
      addressType = None,
      street1 = member.MEM_ADDR1,
      street2 = member.MEM_ADDR2,
      city = member.MEM_CITY,
      zip = zipCodeTransform(member.MEM_ZIP),
      county = member.MEM_COUNTY,
      state = member.MEM_STATE,
      spanDateStart = None,
      spanDateEnd = None
    )

    val phones = member.MEM_PHONE_PRIMARY
      .map(
        phone =>
          Phone(
            id = None,
            phone = phone,
            phoneType = phoneTypeTransform(member.MEM_PHONE_PRIMARY_TYPE)
        )
      )
      .toList

    val emails = member.MEM_EMAIL.map(email => Email(id = None, email = email)).toList

    // TODO: revisit this once CareFirst starts sending us legit SSN data
    val ssn: Option[String] = None

    // TODO: fill out this mapping by consulting marital status enum in member service
    val maritalStatus = member.MEM_MARITAL.map(_ => "unknown")

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
      maritalStatus = maritalStatus,
      ssn = ssn,
      ssnLastFour = ssn.map(_.takeRight(4)),
      addresses = List(address),
      phones = phones,
      emails = emails,
      updatedBy = None
    )

    val current: Boolean = member.insuranceDetails.exists(details =>
      isCurrent(details.spanDateStart, details.spanDateEnd))

    val plan = Plan(
      externalId = member.MEM_ID,
      rank = None,
      current = Option(current),
      details = insuranceDetailTransform(member.insuranceDetails)
    )

    val insurance = Insurance(carrier = insuranceName, plans = List(plan))

    val medicaid = member.MEM_MEDICAID
      .map(
        medicaidId =>
          Insurance(
            carrier = "medicaidDC",
            plans = List(
              Plan(
                externalId = medicaidId,
                rank = None,
                current = Option(true),
                details = List(
                  Detail(
                    lineOfBusiness = Option("medicaid"),
                    subLineOfBusiness = None,
                    spanDateStart = None,
                    spanDateEnd = None,
                  ))
              ))
        )
      )
      .toList

    val medicare = member.MEM_MEDICARE
      .map(
        medicaidId =>
          Insurance(
            carrier = "medicareDC",
            plans = List(
              Plan(
                externalId = medicaidId,
                rank = None,
                current = Option(true),
                details = List(
                  Detail(
                    lineOfBusiness = Option("medicaid"),
                    subLineOfBusiness = None,
                    spanDateStart = None,
                    spanDateEnd = None,
                  ))
              ))
        )
      )
      .toList

    MemberCreateAndPublishRequest(
      demographics = demographics,
      partner = Option(insuranceName),
      insurances = List(insurance),
      medicaid = medicaid,
      medicare = medicare,
      pcpAddress = member.pcpAddress,
      pcpName = member.pcpName,
      pcpPhone = member.pcpPhone,
      pcpPractice = member.pcpPractice,
      cohortId = cohortId,
      categoryId = None,
      primaryPhysician = Option(ElationMappings.careFirstPrimaryPhysician),
      caregiverPractice = Option(ElationMappings.careFirstCaregiverPractice),
      elationPlan = member.getProductDescription,
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

  private def raceTransform(raceCode: String): String = raceCode.toLowerCase match {
    case "white"                                           => "White"
    case "black/african american" | "black (non-hispanic)" => "Black or African American"
    case "american indian/alaska native"                   => "American Indian and Alaska Native"
    case "asian or pacific islander" | "asian"             => "Asian"
    case "native hawaiian or other pacific islander"       => "Native Hawaiian and Other Pacific Islander"
    case "other race"                                      => "Some Other Race"
    case "choose not to answer"                            => "Choose Not to Answer"
    case _                                                 => "Unknown Race"
  }

  private def languageTransform(languageCode: String): String = languageCode match {
    case "1" => "English"
    case "2" => "Spanish"
    case "3" => "Other Indo-European Language"
    case "4" => "Asian and Pacific Island Languages"
    case "8" => "Other Languages"
    case _   => "Spoken Language Unknown"
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

  private def zipCodeTransform(zipCode: Option[String]): Option[String] =
    zipCode.filter(_.length >= 5).map(_.take(5))

  private def dateOfDemiseTransform(dateOfDemise: Option[LocalDate]): Option[LocalDate] =
    dateOfDemise.filter(_.isBefore(new LocalDate()))

  private def phoneTypeTransform(phoneType: Option[String]): Option[String] = phoneType.flatMap {
    case "HOME" => Option("home")
    case _      => Option("main")
  }

  private def insuranceDetailTransform(
    rawInsuranceDetails: List[CareFirstInsuranceDetails]): List[Detail] = {
    val invertedSpanExists = rawInsuranceDetails.exists { rawDetail: CareFirstInsuranceDetails =>
      val endBeforeStart = for {
        rawSpanEnd <- rawDetail.spanDateEnd
        rawSpanStart <- rawDetail.spanDateStart
      } yield rawSpanEnd.isBefore(rawSpanStart)
      endBeforeStart.getOrElse(false)
    }

    implicit val localDateOrdering: Ordering[LocalDate] = Ordering.fromLessThan(_ isBefore _)
    val latestSpanEnd: Option[String] = Option(rawInsuranceDetails.flatMap(_.spanDateEnd).max)
      .map(ISODateTimeFormat.yearMonthDay().print)

    rawInsuranceDetails.map {
      case CareFirstInsuranceDetails(lineOfBusiness,
                                     subLineOfBusiness,
                                     spanDateStart,
                                     spanDateEnd) =>
        Detail(
          lineOfBusiness = lineOfBusiness,
          subLineOfBusiness = subLineOfBusiness,
          spanDateStart = spanDateStart.map(ISODateTimeFormat.yearMonthDay().print),
          spanDateEnd =
            if (invertedSpanExists) latestSpanEnd
            else spanDateEnd.map(ISODateTimeFormat.yearMonthDay().print)
        )
      case _ => throw new IllegalStateException("Unknown type of raw insurance detail found")
    }
  }
}
