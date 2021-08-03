package cityblock.models

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.healthyblue.silver.ProfessionalHeader.ParsedProfessionalHeader
import cityblock.models.healthyblue.silver.ProfessionalLine.ParsedProfessionalLine
import cityblock.models.healthyblue.silver.PharmacyHeader.ParsedPharmacyHeader
import cityblock.models.healthyblue.silver.PharmacyLines.ParsedPharmacyLine
import cityblock.models.healthyblue.silver.Facility.{ParsedFacilityHeader, ParsedFacilityLine}
import cityblock.models.healthyblue.silver.Provider.ParsedProvider
import com.spotify.scio.bigquery.types.BigQueryType

object HealthyBlueSilverClaims {
  @BigQueryType.toTable
  case class SilverProfessionalHeader(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedProfessionalHeader
  )

  @BigQueryType.toTable
  case class SilverProfessionalLine(
    identifier: SilverIdentifier,
    data: ParsedProfessionalLine
  )

  @BigQueryType.toTable
  case class SilverPharmacyHeader(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedPharmacyHeader
  )

  @BigQueryType.toTable
  case class SilverPharmacyLine(
    identifier: SilverIdentifier,
    data: ParsedPharmacyLine
  )

  @BigQueryType.toTable
  case class SilverProvider(
    identifier: SilverIdentifier,
    data: ParsedProvider
  )

  @BigQueryType.toTable
  case class SilverFacilityLine(
    identifier: SilverIdentifier,
    data: ParsedFacilityLine
  )

  @BigQueryType.toTable
  case class SilverFacilityHeader(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedFacilityHeader
  )
}
