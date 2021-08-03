package cityblock.member.attribution.transforms

import cityblock.member.service.api._
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.member.silver.ElationMappings
import cityblock.member.silver.Tufts.{TuftsInsuranceDetails, TuftsMember}
import org.joda.time.LocalDate
import org.joda.time.format.ISODateTimeFormat

object TuftsTransformer {
  implicit private val localDateOrdering: Ordering[LocalDate] = Ordering.fromLessThan(_ isBefore _)

  def createAndPublishRequest(
    member: TuftsMember,
    multiPartnerIds: MultiPartnerIds,
    insuranceName: String,
    cohortId: Option[Int]
  ): MemberCreateAndPublishRequest = {
    val address = Address(
      id = None,
      addressType = None,
      street1 = member.StreetAddress,
      street2 = member.Address2,
      city = member.City,
      zip = member.Zip,
      county = None,
      state = member.State,
      spanDateStart = None,
      spanDateEnd = None
    )

    val phones =
      member.PrimaryPhone
        .map(phone => Phone(id = None, phone = phone, phoneType = Option("main")))
        .toList

    val demographics = Demographics(
      firstName = member.MemberFirstName,
      middleName = member.MemberMiddleInit,
      lastName = member.MemberLastName,
      dateOfBirth = member.DOB.map(_.toString()),
      dateOfDemise = member.DateOfDeath.map(_.toString()),
      sex = member.Gender.map(genderTransform),
      gender = member.Gender.map(genderTransform),
      ethnicity = member.PrimaryEthnicity,
      race = member.PrimaryRace.map(raceTransform),
      language = None,
      maritalStatus = member.MaritalStatus.map(maritalStatusTransform),
      ssn = ssnTransform(member.MemberSSN),
      ssnLastFour = ssnTransform(member.MemberSSN).map(_.takeRight(4)),
      addresses = List(address),
      phones = phones,
      emails = List(),
      updatedBy = None
    )

    val current: Boolean = member.insuranceDetails.exists(details =>
      Utils.isCurrent(details.spanDateStart, details.spanDateEnd))

    val plans = Plan(
      externalId = member.externalId,
      rank = None,
      current = Option(current),
      details = insuranceDetailTransform(member.insuranceDetails)
    )

    val insurance = Insurance(carrier = insuranceName, plans = List(plans))

    val medicaid = member.MMIS
      .map(
        medicaidId =>
          Insurance(
            carrier = "medicaidMA",
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

    val medicare = member.MedicareCode
      .map(
        medicareId =>
          Insurance(
            carrier = "medicareMA",
            plans = List(
              Plan(
                externalId = medicareId,
                rank = None,
                current = Option(true),
                details = List(
                  Detail(
                    lineOfBusiness = Option("medicare"),
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
      primaryPhysician = Option(ElationMappings.tuftsPrimaryPhysician),
      caregiverPractice = Option(ElationMappings.tuftsCaregiverPractice),
      elationPlan = member.getProductDescription,
      createElationPatient = Option(true),
      marketId = multiPartnerIds.market,
      partnerId = multiPartnerIds.partner,
      clinicId = multiPartnerIds.clinic
    )

  }

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

  private def maritalStatusTransform(maritalStatus: String): String = maritalStatus match {
    case "Divorced"   => "divorced"
    case "Unreported" => "unknown"
    case _            => "unknown"
  }

  private def genderTransform(gender: String): String = gender match {
    case "M" => "male"
    case "F" => "female"
    case _   => ""
  }

  private def ssnTransform(ssn: Option[String]): Option[String] = ssn.filter(_.length == 9)

  private def insuranceDetailTransform(
    rawInsuranceDetails: List[TuftsInsuranceDetails]): List[Detail] = {
    val invertedSpanExists = rawInsuranceDetails.exists { rawDetail: TuftsInsuranceDetails =>
      val endBeforeStart = for {
        rawSpanEnd <- rawDetail.spanDateEnd
        rawSpanStart <- rawDetail.spanDateStart
      } yield rawSpanEnd.isBefore(rawSpanStart)
      endBeforeStart.getOrElse(false)
    }

    val latestSpanEnd: Option[String] = Option(rawInsuranceDetails.flatMap(_.spanDateEnd).max)
      .map(ISODateTimeFormat.yearMonthDay().print)

    rawInsuranceDetails.map {
      case TuftsInsuranceDetails(lineOfBusiness, subLineOfBusiness, spanDateStart, spanDateEnd) =>
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
