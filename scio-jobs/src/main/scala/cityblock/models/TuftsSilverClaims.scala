package cityblock.models

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.tufts.silver.Medical.ParsedMedical
import cityblock.models.tufts.silver.Member.ParsedMember
import cityblock.models.tufts.silver.Pharmacy.ParsedPharmacy
import cityblock.models.tufts.silver.Provider.ParsedProvider
import cityblock.models.tufts.silver.MassHealth.ParsedMassHealth
import com.spotify.scio.bigquery.types.BigQueryType

object TuftsSilverClaims {
  @BigQueryType.toTable
  case class SilverProvider(
    identifier: SilverIdentifier,
    data: ParsedProvider
  )

  @BigQueryType.toTable
  case class SilverMember(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedMember
  )

  @BigQueryType.toTable
  case class SilverPharmacy(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedPharmacy
  )

  @BigQueryType.toTable
  case class Medical(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedMedical
  )

  @BigQueryType.toTable
  case class MassHealth(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedMassHealth
  )
}
