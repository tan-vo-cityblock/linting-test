package cityblock.member.service

import cityblock.member.service.api.{
  Address,
  Demographics,
  Email,
  Insurance,
  MemberCreateAndPublishRequest,
  MemberInsuranceRecord,
  Phone
}

object TestData {

  val testMemberCreateAndPublishRequest =
    MemberCreateAndPublishRequest(
      demographics = Demographics(
        firstName = None,
        middleName = None,
        lastName = None,
        dateOfBirth = None,
        dateOfDemise = None,
        sex = None,
        gender = None,
        ethnicity = None,
        race = None,
        language = None,
        maritalStatus = None,
        ssn = None,
        ssnLastFour = None,
        addresses = List.empty[Address],
        emails = List.empty[Email],
        phones = List.empty[Phone],
        updatedBy = None
      ),
      partner = None,
      insurances = List.empty[Insurance],
      medicaid = List.empty[Insurance],
      medicare = List.empty[Insurance],
      pcpAddress = None,
      pcpName = None,
      pcpPhone = None,
      pcpPractice = None,
      cohortId = None,
      categoryId = None,
      primaryPhysician = None,
      caregiverPractice = None,
      elationPlan = None,
      createElationPatient = None,
      marketId = "",
      partnerId = "",
      clinicId = ""
    )

  val EXPECTED_MEMBER_INSURANCE_RECORD_RESPONSE: List[MemberInsuranceRecord] = List(
    MemberInsuranceRecord(
      memberId = "a3ccab32-d960-11e9-b350-acde48001122",
      externalId = "M1",
      carrier = "emblem",
      current = Some(false)
    )
  )
}
