package cityblock.ehrmodels.elation.datamodelapi.patient

import io.circe.generic.JsonCodec

@JsonCodec
case class Insurance(
  insurance_company: Option[Long],
  insurance_plan: Option[Long],
  rank: String,
  carrier: String,
  member_id: Option[String],
  group_id: Option[String],
  plan: Option[String],
  phone: Option[String],
  extension: Option[String],
  address_line1: Option[String],
  address_line2: Option[String],
  city: Option[String],
  state: Option[String],
  zip: Option[String],
  copay: Option[String],
  deductible: Option[String],
  insured_person_first_name: Option[String],
  insured_person_last_name: Option[String],
  insured_person_address: Option[String],
  insured_person_city: Option[String],
  insured_person_state: Option[String],
  insured_person_zip: Option[String],
  insured_person_dob: Option[String],
  insured_person_gender: Option[String],
  insured_person_ssn: Option[String],
  relationship_to_insured: Option[String]
)
