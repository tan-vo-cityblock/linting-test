package cityblock.streaming.jobs.aggregations

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.bigquery.types.BigQueryType
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder

object Problems {
  @BigQueryType.toTable
  case class ProblemCategory(
    Code: String,
    CodeSystem: Option[String],
    CodeSystemName: Option[String],
    Name: Option[String]
  )

  object ProblemCategory {
    implicit val problemCategoryEncoder: Encoder[ProblemCategory] =
      deriveEncoder[ProblemCategory]
  }

  @BigQueryType.toTable //TODO: Delete case class and delete columns in bq. Kanban ticket #3810
  case class ProblemHealthStatus(
    Code: Option[String],
    CodeSystem: Option[String],
    CodeSystemName: Option[String],
    Name: Option[String]
  )

  object ProblemHealthStatus {
    implicit val problemHealthStatusEncoder: Encoder[ProblemHealthStatus] =
      deriveEncoder[ProblemHealthStatus]
  }

  @BigQueryType.toTable
  case class ProblemStatus(
    Code: Option[String],
    CodeSystem: Option[String],
    CodeSystemName: Option[String],
    Name: Option[String]
  )

  object ProblemStatus {
    implicit val problemStatusStatusEncoder: Encoder[ProblemStatus] =
      deriveEncoder[ProblemStatus]
  }

  @BigQueryType.toTable
  case class Problem(
    StartDate: Option[DateOrInstant],
    EndDate: Option[DateOrInstant],
    Code: String,
    CodeSystem: Option[String],
    CodeSystemName: Option[String],
    Name: Option[String],
    Category: ProblemCategory,
    HealthStatus: ProblemHealthStatus,
    Status: ProblemStatus
  )

  object Problem {
    implicit val problemEncoder: Encoder[Problem] =
      deriveEncoder[Problem]
  }

  @BigQueryType.toTable
  case class PatientProblem(
    messageId: String,
    patient: Patient,
    problem: Problem
  )

  object PatientProblem {
    implicit val patientProblemEncoder: Encoder[PatientProblem] =
      deriveEncoder[PatientProblem]
  }
}
