package cityblock.member.service.api

import io.circe.generic.JsonCodec

@JsonCodec
case class MemberCreateAndPublishRequest(
  // demographics field
  demographics: Demographics,
  // Insurance related fields
  partner: Option[String],
  insurances: List[Insurance],
  medicaid: List[Insurance],
  medicare: List[Insurance],
  // PCP related fields
  pcpAddress: Option[String],
  pcpName: Option[String],
  pcpPhone: Option[String],
  pcpPractice: Option[String],
  // miscellaneous member fields
  cohortId: Option[Int],
  categoryId: Option[Int],
  // Elation related fields
  primaryPhysician: Option[Long],
  caregiverPractice: Option[Long],
  elationPlan: Option[String],
  createElationPatient: Option[Boolean],
  // Commons specific UUIDs for patient-related tables
  marketId: String,
  partnerId: String,
  clinicId: String
)
