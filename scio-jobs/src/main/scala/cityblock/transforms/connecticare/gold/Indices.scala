package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims._
import cityblock.utilities.ScioUtils
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection

import scala.concurrent.Future
import scala.concurrent.duration.Duration

sealed case class Indices(
  claimDiagnosisCom: SCollection[IndexedDiagnoses],
  claimProcedureCom: SCollection[IndexedProcedures]
) {
  def futures: Indices.Futures =
    Indices.Futures(claimDiagnosisCom.materialize, claimProcedureCom.materialize)
}

object Indices {
  def apply(project: String,
            MedicalDiagnosis: SCollection[MedicalDiagnosis],
            MedicalICDProc: SCollection[MedicalICDProcedure]): Indices = {
    val claimDiagnosisCom =
      DiagnosisTransformer.indexByClaimKey(project, MedicalDiagnosis)
    val claimProcedureCom =
      ProcedureTransformer.indexByClaimKey(project, MedicalICDProc)

    Indices(
      claimDiagnosisCom,
      claimProcedureCom
    )
  }

  case class Taps(
    claimDiagnosisCom: Tap[IndexedDiagnoses],
    claimProcedureCom: Tap[IndexedProcedures]
  ) extends ScioUtils.Taps

  case class Futures(
    claimDiagnosisCom: Future[Tap[IndexedDiagnoses]],
    claimProcedureCom: Future[Tap[IndexedProcedures]]
  ) extends ScioUtils.Futures {
    def wait(duration: Duration): Taps = Taps(
      claimDiagnosisCom.waitForResult(duration),
      claimProcedureCom.waitForResult(duration)
    )
  }
}
