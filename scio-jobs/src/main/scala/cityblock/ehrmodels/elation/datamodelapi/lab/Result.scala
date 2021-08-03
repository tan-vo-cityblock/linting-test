package cityblock.ehrmodels.elation.datamodelapi.lab

import io.circe.generic.JsonCodec

@JsonCodec
case class Result(
  status: Option[String],
  value: String,
  value_type: Option[String],
  text: Option[String],
  note: Option[String],
  reference_min: String,
  reference_max: String,
  units: Option[String],
  is_abnormal: Option[Boolean],
  abnormal_flag: Option[String],
  test: Test,
  test_category: TestCategory
)
