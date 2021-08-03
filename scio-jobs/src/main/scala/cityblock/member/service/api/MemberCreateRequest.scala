package cityblock.member.service.api

import cityblock.models.gold.Claims.MemberDemographics
import io.circe.generic.JsonCodec

@JsonCodec
case class MemberCreateRequest(
  demographics: Demographics,
  insurances: List[Insurance],
  cohortId: Option[Int],
  categoryId: Option[Int],
  primaryPhysician: Long,
  caregiverPractice: Long,
  elationPlan: Option[String],
  partner: Option[String],
  medicaid: List[Insurance],
  medicare: List[Insurance]
)

object MemberCreateRequest {

  @deprecated
  def apply(
    partner: String,
    insuranceId: String,
    cohortId: Option[Int],
    categoryId: Option[Int],
    demographics: MemberDemographics,
  ): MemberCreateRequest = {

    val emails = demographics.location.email.map(email => Email(id = None, email = email)).toList

    val phones = demographics.location.phone
      .map(phone => Phone(id = None, phone = phone, phoneType = None))
      .toList

    val address = Address(
      id = None,
      street1 = demographics.location.address1,
      street2 = demographics.location.address2,
      city = demographics.location.city,
      county = demographics.location.county,
      state = demographics.location.state,
      zip = demographics.location.zip,
      addressType = None,
      spanDateStart = demographics.date.from.map(_.toString),
      spanDateEnd = demographics.date.to.map(_.toString),
    )

    val demo = Demographics(
      firstName = demographics.identity.firstName,
      middleName = demographics.identity.middleName,
      lastName = demographics.identity.lastName,
      dateOfBirth = demographics.date.birth.map(_.toString),
      dateOfDemise = demographics.date.death.map(_.toString),
      sex = demographics.identity.gender,
      gender = None,
      ethnicity = demographics.identity.ethnicity,
      race = demographics.identity.race,
      language = demographics.identity.primaryLanguage,
      maritalStatus = demographics.identity.maritalStatus,
      ssn = None,
      ssnLastFour = None,
      addresses = List(address),
      emails = emails,
      phones = phones,
      updatedBy = None
    )

    /* N.B. We are severing the gold -> member service data transfer so nothing should go down this code path.
            Because of that, we are not going to spend time to maintain the details mapping below.
     */
    val insurance = Insurance(
      carrier = partner,
      plans = List(
        Plan(
          externalId = insuranceId,
          rank = Option("primary"),
          current = Some(true),
          details = List.empty[Detail]
        )
      )
    )

    new MemberCreateRequest(
      demographics = demo,
      partner = Some(partner),
      insurances = List(insurance),
      primaryPhysician = 150754856796162L, // TODO: Currently hard-coded to Loretta Staples (ConnectiCare partner) need to not hard-code providers.
      caregiverPractice = 145331296403460L, // TODO: Remove hard-coded providers and provide a better mechanism for determining.
      cohortId = cohortId,
      categoryId = categoryId,
      elationPlan = None,
      medicaid = List.empty[Insurance],
      medicare = List.empty[Insurance]
    )
  }
}
