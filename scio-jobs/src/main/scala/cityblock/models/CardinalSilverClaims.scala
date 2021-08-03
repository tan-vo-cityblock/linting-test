package cityblock.models

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.cardinal.silver.Pharmacy.ParsedPharmacy
import cityblock.models.cardinal.silver.Provider.ParsedProvider
import cityblock.models.cardinal.silver.Physical.ParsedPhysical
import cityblock.models.cardinal.silver.PriorAuthorization.ParsedPriorAuthorization
import com.spotify.scio.bigquery.types.BigQueryType

object CardinalSilverClaims {
  @BigQueryType.toTable
  case class SilverProvider(
    identifier: SilverIdentifier,
    data: ParsedProvider
  )

  @BigQueryType.toTable
  case class SilverPharmacy(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedPharmacy
  )

  @BigQueryType.toTable
  case class SilverPhysical(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedPhysical
  )

  @BigQueryType.toTable
  case class SilverPriorAuthorization(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedPriorAuthorization
  )
}
