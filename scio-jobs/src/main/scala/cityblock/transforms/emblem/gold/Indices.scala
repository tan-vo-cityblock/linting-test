package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.{
  SilverDiagnosisAssociation,
  SilverDiagnosisAssociationCohort,
  SilverProcedureAssociation
}
import cityblock.models.gold.Claims.{Diagnosis, Procedure}
import cityblock.utilities.ScioUtils
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection

import scala.concurrent.Future
import scala.concurrent.duration.Duration

sealed case class Indices(
  diagnosis: SCollection[(ClaimKey, Iterable[Diagnosis])],
  procedure: SCollection[(ClaimKey, Iterable[Procedure])]
) {
  def futures: Indices.Futures = Indices.Futures(
    diagnosis.materialize,
    procedure.materialize
  )
}

object Indices {
  def apply(project: String,
            diagnosisAssociations: SCollection[SilverDiagnosisAssociation],
            procedureAssociations: SCollection[SilverProcedureAssociation]): Indices =
    Indices(
      DiagnosisTransformer.indexByClaimKey(project, diagnosisAssociations),
      ProcedureTransformer.indexByClaimKey(project, procedureAssociations)
    )

  def applyCohort(project: String,
                  diagnosisAssociations: SCollection[SilverDiagnosisAssociationCohort],
                  procedureAssociations: SCollection[SilverProcedureAssociation]): Indices =
    Indices(
      DiagnosisTransformer.indexByClaimCohortKey(project, diagnosisAssociations),
      ProcedureTransformer.indexByClaimKey(project, procedureAssociations)
    )

  case class Taps(
    diagnosis: Tap[(ClaimKey, Iterable[Diagnosis])],
    procedure: Tap[(ClaimKey, Iterable[Procedure])]
  ) extends ScioUtils.Taps

  case class Futures(
    diagnosis: Future[Tap[(ClaimKey, Iterable[Diagnosis])]],
    procedure: Future[Tap[(ClaimKey, Iterable[Procedure])]]
  ) extends ScioUtils.Futures {
    def wait(duration: Duration = Duration.Inf): Taps = Taps(
      diagnosis.waitForResult(duration),
      procedure.waitForResult(duration)
    )
  }
}
