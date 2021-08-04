package cityblock.models

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.carefirst.silver.DiagnosisAssociation.ParsedDiagnosisAssociation
import cityblock.models.carefirst.silver.ProcedureAssociation.ParsedProcedureAssociation
import cityblock.models.carefirst.silver.Professional.ParsedProfessional
import cityblock.models.carefirst.silver.Provider.ParsedProvider
import cityblock.models.carefirst.silver.Facility.ParsedFacility
import cityblock.models.carefirst.silver.LabResult.ParsedLabResult
import cityblock.models.carefirst.silver.Pharmacy.ParsedPharmacy
import com.spotify.scio.bigquery.types.BigQueryType

object CarefirstSilverClaims {
  @BigQueryType.toTable
  case class SilverProvider(
    identifier: SilverIdentifier,
    data: ParsedProvider
  )

  @BigQueryType.toTable
  case class SilverProfessional(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedProfessional
  )

  @BigQueryType.toTable
  case class SilverDiagnosisAssociation(
    identifier: SilverIdentifier,
    data: ParsedDiagnosisAssociation
  )

  @BigQueryType.toTable
  case class SilverProcedureAssociation(
    identifier: SilverIdentifier,
    data: ParsedProcedureAssociation
  )

  @BigQueryType.toTable
  case class SilverFacility(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedFacility
  )

  @BigQueryType.toTable
  case class SilverLabResult(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedLabResult
  )

  @BigQueryType.toTable
  case class SilverPharmacy(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedPharmacy
  )
}
