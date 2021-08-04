package cityblock.member.attribution.transforms

import cityblock.member.attribution.types.Cardinal.InsuranceAndDate
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
import cityblock.member.silver.Cardinal.CardinalMember
import cityblock.member.silver.ElationMappings
import cityblock.transforms.Transform
import cityblock.member.attribution.transforms.common.NorthCarolinaTransforms
import org.joda.time.LocalDate

object CardinalTransformer extends NorthCarolinaTransforms {
  def createAndPublishRequest(
    memberAndInsuranceDates: (CardinalMember, Iterable[InsuranceAndDate]),
    multiPartnerIds: MultiPartnerIds,
    insuranceName: String,
    cohortId: Option[Int]
  ): MemberCreateAndPublishRequest = {
    val (member, insuranceDates) = memberAndInsuranceDates

    val socialSecurityNumber: Option[String] = member.SSN

    def phoneTransform(phoneNumber: String): String =
      phoneNumber.toList.filter(_.isDigit).takeRight(10).mkString

    val phones: List[Phone] = List(
      member.PrimaryPhone.map(
        phone =>
          Phone(
            id = None,
            phone = phoneTransform(phone),
            phoneType = member.PrimaryPhoneType.orElse(Option("main"))
        )
      ),
      member.SecondaryPhone.map(
        phone =>
          Phone(
            id = None,
            phone = phoneTransform(phone),
            phoneType = member.SecondaryPhoneType.orElse(Option("secondary"))
        )
      )
    ).flatten

    val email: List[Email] = member.Email.map(email => Email(id = None, email = email)).toList
    val address = Address(
      id = None,
      addressType = None,
      street1 = member.AddressLine1,
      street2 = member.AddressLine2,
      county = member.County,
      city = member.City,
      state = member.State,
      zip = member.PostalCode,
      spanDateStart = None,
      spanDateEnd = None
    )
    implicit val localDateOrdering: Ordering[Option[LocalDate]] =
      Transform.NoneMaxOptionLocalDateOrdering
    val earliestSpan =
      insuranceDates.minBy(_.EnrollmentDate)
    val latestSpan =
      insuranceDates.maxBy(_.TerminationDate)

    val medicaid = member.MedicaidId
      .map(
        medicaidId =>
          Insurance(
            carrier = "MedicaidNC",
            plans = List(
              Plan(
                externalId = medicaidId,
                rank = None,
                current = Option(
                  Utils
                    .isCurrent(earliestSpan.EnrollmentDate, earliestSpan.TerminationDate) || Utils
                    .isCurrent(latestSpan.EnrollmentDate, latestSpan.TerminationDate)
                ),
                details = insuranceDates
                  .map(insuranceDate => {
                    Detail(
                      lineOfBusiness = Option("Medicaid"),
                      subLineOfBusiness = insuranceDate.SubLineOfBusiness,
                      spanDateStart = insuranceDate.EnrollmentDate.map(_.toString),
                      spanDateEnd = insuranceDate.TerminationDate.map(_.toString)
                    )
                  })
                  .toList
              )
            )
        )
      )
      .toList

    val medicare = member.MBI
      .map(
        medicareId =>
          Insurance(
            carrier = "medicareNC",
            plans = List(
              Plan(
                externalId = medicareId,
                rank = None,
                current = Option(
                  Utils
                    .isCurrent(earliestSpan.EnrollmentDate, earliestSpan.TerminationDate) || Utils
                    .isCurrent(latestSpan.EnrollmentDate, latestSpan.TerminationDate)
                ),
                details = insuranceDates
                  .map(insuranceDate => {
                    Detail(
                      lineOfBusiness = Option("medicare"),
                      subLineOfBusiness = insuranceDate.SubLineOfBusiness,
                      spanDateStart = insuranceDate.EnrollmentDate.map(_.toString),
                      spanDateEnd = insuranceDate.TerminationDate.map(_.toString)
                    )
                  })
                  .toList
              )
            )
        )
      )
      .toList

    val insurance = Insurance(
      carrier = insuranceName,
      plans = List(
        Plan(
          externalId = member.MemberID,
          rank = None, // TODO: cardinal primary unless they are a dual and then medicare is primary
          current = Option(
            Utils.isCurrent(earliestSpan.EnrollmentDate, earliestSpan.TerminationDate) || Utils
              .isCurrent(latestSpan.EnrollmentDate, latestSpan.TerminationDate)
          ),
          details = insuranceDates
            .map(insuranceDate => {
              Detail(
                lineOfBusiness = member.LineOfBusiness.map(_.toLowerCase),
                subLineOfBusiness =
                  if (member.IsDualEligible == Option(1)) Option("Cardinal (Dual)")
                  else insuranceDate.SubLineOfBusiness,
                spanDateStart = insuranceDate.EnrollmentDate.map(_.toString),
                spanDateEnd = insuranceDate.TerminationDate.map(_.toString)
              )
            })
            .toList
        )
      )
    )

    val demographics = Demographics(
      firstName = member.FirstName,
      middleName = None,
      lastName = member.LastName,
      dateOfBirth = member.DOB.map(_.toString),
      dateOfDemise = Utils.dateOfDemiseTransform(member.DOD).map(_.toString),
      sex = member.GenderId.map(genderTransform),
      gender = member.GenderId.map(genderTransform),
      ethnicity = member.EthnicityCode.map(ethnicityTransform),
      race = member.RaceCode.map(raceTransform),
      language = member.PrimaryLanguageCode, // TODO: so far all NULLs so we wait to implement
      maritalStatus = member.MaritialStatusCode.map(maritalStatusTransform),
      ssn = socialSecurityNumber,
      ssnLastFour = Utils.lastFourSSN(socialSecurityNumber),
      addresses = List(address),
      phones = phones,
      emails = email,
      updatedBy = None
    )

    MemberCreateAndPublishRequest(
      demographics = demographics,
      partner = Option(insuranceName),
      insurances = List(insurance),
      medicaid = medicaid,
      medicare = medicare,
      pcpAddress = Option("4444 The Plaza Suite 18 Charlotte, NC 28215"),
      pcpName = Option("Cityblock Medical Practice NC, PC"),
      pcpPhone = Option("3363602407"),
      pcpPractice = Option("Cityblock Medical Practice NC, PC"),
      cohortId = cohortId,
      categoryId = None,
      primaryPhysician = Option(ElationMappings.cardinalPrimaryPhysician),
      caregiverPractice = Option(ElationMappings.cardinalCaregiverPractice),
      elationPlan =
        if (member.IsDualEligible == Option(1)) Option("Cardinal (Dual)")
        else
          Option("Cardinal"),
      createElationPatient = Option(true),
      marketId = member.MarketId,
      partnerId = multiPartnerIds.partner,
      clinicId = member.ClinicId
    )
  }

  def genderTransform(genderId: String): String = genderId match {
    case "61" => "male"
    case "62" => "female"
    case _    => ""
  }

  def raceTransform(raceCode: String): String = raceCode match {
    case "W"           => "White"
    case "B"           => "Black or African American"
    case "M"           => "Two or More Races"
    case "I" | "K"     => "American Indian and Alaska Native"
    case "A"           => "Asian"
    case "O" | "U" | _ => "Unknown Race"
  }

  private def maritalStatusTransform(maritalStatus: String): String = maritalStatus match {
    case "S" => "neverMarried"
    case "U" => "unmarried"
    case "W" => "widowed"
    case "M" => "currentlyMarried"
    case "D" => "divorced"
    case "L" => "domesticPartner"
    case _   => "unknown"
  }
}
