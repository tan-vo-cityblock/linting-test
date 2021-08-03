package cityblock.member.attribution.transforms

import cityblock.member.attribution.transforms.common.NorthCarolinaTransforms
import cityblock.member.service.api.{
  Address,
  Demographics,
  Detail,
  Insurance,
  MemberCreateAndPublishRequest,
  Phone,
  Plan
}
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.member.silver.HealthyBlue.HealthyBlueMember
import cityblock.member.silver.ElationMappings

object HealthyBlueTransformer extends NorthCarolinaTransforms {
  def createAndPublishRequest(
    member: HealthyBlueMember,
    multiPartnerIds: MultiPartnerIds,
    insuranceName: String,
    cohortId: Option[Int]
  ): MemberCreateAndPublishRequest = {
    val phones = member.PrimaryPhone
      .map(
        phone =>
          Phone(
            id = None,
            phone = phone,
            phoneType = Option("main")
        )
      )
      .toList

    val address = Address(
      id = None,
      addressType = None,
      street1 = member.AddressLine1,
      street2 = member.AddressLine2,
      county = member.County,
      city = member.City,
      state = member.State,
      zip = zipCodeTransform(member.PostalCode),
      spanDateStart = None,
      spanDateEnd = None
    )

    val medicare = member.MBI
      .map(
        medicareId =>
          Insurance(
            carrier = "bcbsNC",
            plans = List(
              Plan(
                externalId = medicareId,
                rank = None,
                current = Option(Utils.isCurrent(member.EnrollmentDate, member.TerminationDate)),
                details = List(
                  Detail(
                    lineOfBusiness = Option("medicare"),
                    subLineOfBusiness = Option("Blue Medicare"),
                    spanDateStart = member.EnrollmentDate.map(_.toString),
                    spanDateEnd = member.TerminationDate.map(_.toString)
                  )
                )
              )
            )
        )
      )
      .toList

    val insurance = Insurance(
      carrier = "bcbsNC",
      plans = List(
        Plan(
          externalId = member.MemberId,
          rank = None,
          current = Option(Utils.isCurrent(member.EnrollmentDate, member.TerminationDate)),
          details = List(
            Detail(
              lineOfBusiness = Option("medicaid"),
              subLineOfBusiness = Option("Healthy Blue"),
              spanDateStart = member.EnrollmentDate.map(_.toString),
              spanDateEnd = member.TerminationDate.map(_.toString)
            )
          )
        )
      )
    )

    val demographics = Demographics(
      firstName = member.FirstName,
      middleName = member.MiddleName,
      lastName = member.LastName,
      dateOfBirth = member.DOB.map(_.toString),
      dateOfDemise = Utils.dateOfDemiseTransform(member.DOD).map(_.toString),
      sex = member.GenderCode.map(genderTransform),
      gender = member.GenderCode.map(genderTransform),
      ethnicity = member.EthnicityCode.map(ethnicityTransform),
      race =
        if (member.RaceCode2.isDefined) Option(raceTransform("M"))
        else member.RaceCode1.map(raceTransform),
      language = member.PrimaryLanguageCode, // TODO: so far all NULLs so we wait to implement
      maritalStatus = None,
      ssn = None,
      ssnLastFour = None,
      addresses = List(address),
      phones = phones,
      emails = List.empty,
      updatedBy = None
    )
    val pcpAddress = Option(
      member.PCPAddressLine1.getOrElse("") + " " + member.PCPAddressLine2
        .getOrElse("") + " " + member.PCPCity.getOrElse("") + ", " + member.PCPState
        .getOrElse("") + " " + member.PCPPostalCode.getOrElse("")
    )

    MemberCreateAndPublishRequest(
      demographics = demographics,
      partner = Option(insuranceName),
      insurances = List(insurance),
      // BCBS uses the medicaid id as their internal id
      medicaid = List.empty,
      medicare = medicare,
      pcpAddress = pcpAddress,
      pcpName = member.PCPName,
      pcpPhone = member.PCPPhone,
      pcpPractice = member.PCPName,
      cohortId = cohortId,
      categoryId = None,
      primaryPhysician = Option(ElationMappings.healthyBluePrimaryPhysician),
      caregiverPractice = Option(ElationMappings.healthyBlueCaregiverPractice),
      elationPlan = Option("HealthyBlue"),
      createElationPatient = Option(true),
      marketId = multiPartnerIds.market,
      partnerId = multiPartnerIds.partner,
      clinicId = member.ClinicId
    )
  }

  private def zipCodeTransform(zipCode: Option[String]): Option[String] =
    zipCode.filter(_.length >= 5).map(_.take(5))

  def genderTransform(genderId: String): String = genderId match {
    case "M" => "male"
    case "F" => "female"
    case _   => ""
  }

  def raceTransform(raceCode: String): String = raceCode match {
    case "C"           => "White"
    case "B"           => "Black or African American"
    case "M"           => "Two or More Races"
    case "I" | "K"     => "American Indian or Alaska Native"
    case "A"           => "Asian"
    case "P"           => "Pacific Islander"
    case "O" | "U" | _ => "Unknown Race"
  }
}
