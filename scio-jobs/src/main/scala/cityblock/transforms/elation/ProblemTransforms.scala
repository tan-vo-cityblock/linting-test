package cityblock.transforms.elation

import cityblock.ehrmodels.elation.datamodelapi.problem.{Problem => ElationProblem}
import cityblock.member.service.models.PatientInfo.Patient
import cityblock.streaming.jobs.aggregations.Problems._
import cityblock.utilities.time.DateOrInstant
import cityblock.utilities.{Conversions, SnomedCT}

object ProblemTransforms {
  def transformElationProblemsToPatientProblems(
    messageId: String,
    patient: Patient,
    elationProblems: List[ElationProblem]
  ): List[PatientProblem] = {
    val problemCategory =
      ProblemCategory(
        SnomedCT.ProblemType.Complaint.code,
        Some(SnomedCT.codeSystem),
        Some(SnomedCT.codeSystemName),
        Some(SnomedCT.ProblemType.Complaint.name)
      )

    elationProblems.flatMap { elationProblem =>
      val description = elationProblem.description

      val startDate =
        elationProblem.start_date.map(Conversions.mkDateOrInstant)

      val endDate: Option[DateOrInstant] = elationProblem.resolved_date match {
        case Some(resolvedDate) =>
          Some(Conversions.mkDateOrInstant(resolvedDate))
        case None =>
          None
      }

      val problemStatus = ProblemStatus(
        Some(elationProblem.status.code),
        Some(SnomedCT.codeSystem),
        Some(SnomedCT.codeSystemName),
        Some(elationProblem.status.name)
      )

      elationProblem.dx.map { diagnosis =>
        val problem = Problem(
          startDate,
          endDate,
          diagnosis.snomed,
          Some(SnomedCT.codeSystem),
          Some(SnomedCT.codeSystemName),
          description,
          problemCategory,
          ProblemHealthStatus(None, None, None, None), //TODO: Delete ProblemHealthStatusCase class and migrate table.
          problemStatus
        )

        PatientProblem(messageId, patient, problem)
      }
    }
  }
}
