package cityblock.models

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.emblem_connecticare.silver.LabResult.ParsedLabResult
import cityblock.models.emblem_connecticare.silver.ProfessionalClaim.ParsedProfessionalClaim
import cityblock.models.emblem_connecticare.silver.ProfessionalExtension.ParsedProfessionalExtension
import cityblock.models.emblem_connecticare.silver.Provider.ParsedProvider
import com.spotify.scio.bigquery.types.BigQueryType

object EmblemConnecticareSilverClaims {
  @BigQueryType.toTable
  case class SilverLabResult(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedLabResult
  )

  @BigQueryType.toTable
  case class SilverProfessionalClaim(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedProfessionalClaim
  )

  @BigQueryType.toTable
  case class SilverProvider(
    identifier: SilverIdentifier,
    data: ParsedProvider
  )

  @BigQueryType.toTable
  case class SilverProfessionalExtension(
    identifier: SilverIdentifier,
    data: ParsedProfessionalExtension
  )
}
