package cityblock.streaming.jobs.aggregations

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.bigquery.types.BigQueryType
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

object Results {
  @BigQueryType.toTable
  case class ResultObservationReferenceRange(
    Low: Option[String],
    High: Option[String],
    Text: Option[String]
  )

  object ResultObservationReferenceRange {
    implicit val resultObservationReferenceRangeEncoder: Encoder[ResultObservationReferenceRange] =
      deriveEncoder[ResultObservationReferenceRange]
  }

  @BigQueryType.toTable
  case class ResultObservationCodedValue(
    Code: Option[String],
    CodeSystem: Option[String],
    CodeSystemName: Option[String],
    Name: Option[String]
  )

  object ResultObservationCodedValue {
    implicit val resultObservationCodedValueEncoder: Encoder[ResultObservationCodedValue] =
      deriveEncoder[ResultObservationCodedValue]
  }

  @BigQueryType.toTable
  case class ResultObservation(
    Code: Option[String],
    CodeSystem: Option[String],
    CodeSystemName: Option[String],
    Name: Option[String],
    Status: Option[String],
    Interpretation: Option[String],
    DateTime: Option[DateOrInstant],
    CodedValue: ResultObservationCodedValue,
    Value: Option[String],
    ValueType: Option[String],
    Units: Option[String],
    ReferenceRange: ResultObservationReferenceRange
  )

  object ResultObservation {
    implicit val resultObservationEncoder: Encoder[ResultObservation] =
      deriveEncoder[ResultObservation]
  }

  @BigQueryType.toTable
  case class Result(
    Code: Option[String],
    CodeSystem: Option[String],
    CodeSystemName: Option[String],
    Name: Option[String],
    Status: Option[String],
    Observations: List[ResultObservation]
  )

  object Result {
    implicit val resultEncoder: Encoder[Result] =
      deriveEncoder[Result]
  }

  @BigQueryType.toTable
  case class PatientResult(
    messageId: String,
    patient: Patient,
    result: Result
  )

  object PatientResult {
    implicit val patientResultEncoder: Encoder[PatientResult] =
      deriveEncoder[PatientResult]
  }
}
