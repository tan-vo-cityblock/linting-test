package cityblock.ehrmodels.elation.datamodelapi.problem

import io.circe.generic.JsonCodec

@JsonCodec
case class Diagnosis(icd9: List[String], icd10: List[String], snomed: String)
