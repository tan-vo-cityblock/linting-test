package cityblock.models

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.emblem.silver.HealthRiskAssessment.ParsedHealthRiskAssessment
import cityblock.models.emblem.silver.PriorAuthorization.ParsedPriorAuthorization
import cityblock.parsers.emblem.LabResultCohort.ParsedLabResultCohort
import cityblock.parsers.emblem.Provider.ParsedProvider
import cityblock.parsers.emblem.associations.DiagnosisAssociation.ParsedDiagnosisAssociation
import cityblock.parsers.emblem.associations.DiagnosisAssociationCohort.ParsedDiagnosisAssociationCohort
import cityblock.parsers.emblem.associations.ProcedureAssociation.ParsedProcedureAssociation
import cityblock.parsers.emblem.claims.FacilityClaimCohort.ParsedFacilityClaimCohort
import cityblock.parsers.emblem.claims.PharmacyClaimCohort.ParsedPharmacyClaimCohort
import cityblock.parsers.emblem.claims.ProfessionalClaimCohort.ParsedProfessionalClaimCohort
import cityblock.parsers.emblem.members.MemberDemographic.ParsedMemberDemographic
import cityblock.parsers.emblem.members.MemberDemographicCohort.ParsedMemberDemographicCohort
import cityblock.parsers.emblem.members.MemberMonth.ParsedMemberMonth
import com.spotify.scio.bigquery.types.BigQueryType

object EmblemSilverClaims {
  @BigQueryType.toTable
  case class SilverProvider(
    identifier: SilverIdentifier,
    provider: ParsedProvider
  )

  @BigQueryType.toTable
  case class SilverMemberDemographic(
    identifier: SilverIdentifier,
    patient: Patient,
    demographic: ParsedMemberDemographic
  )

  @BigQueryType.toTable
  case class SilverMemberDemographicCohort(
    identifier: SilverIdentifier,
    patient: Patient,
    demographic: ParsedMemberDemographicCohort
  )

  @BigQueryType.toTable
  case class SilverMemberMonth(
    identifier: SilverIdentifier,
    patient: Patient,
    month: ParsedMemberMonth
  )

  @BigQueryType.toTable
  case class SilverProfessionalClaimCohort(
    identifier: SilverIdentifier,
    patient: Patient,
    claim: ParsedProfessionalClaimCohort
  )

  @BigQueryType.toTable
  case class SilverFacilityClaimCohort(
    identifier: SilverIdentifier,
    patient: Patient,
    claim: ParsedFacilityClaimCohort
  )

  @BigQueryType.toTable
  case class SilverPharmacyClaimCohort(
    identifier: SilverIdentifier,
    patient: Patient,
    claim: ParsedPharmacyClaimCohort
  )

  @BigQueryType.toTable
  case class SilverDiagnosisAssociation(
    identifier: SilverIdentifier,
    diagnosis: ParsedDiagnosisAssociation
  )

  @BigQueryType.toTable
  case class SilverDiagnosisAssociationCohort(
    identifier: SilverIdentifier,
    diagnosis: ParsedDiagnosisAssociationCohort
  )

  @BigQueryType.toTable
  case class SilverProcedureAssociation(
    identifier: SilverIdentifier,
    procedure: ParsedProcedureAssociation
  )

  @BigQueryType.toTable
  case class SilverLabResultCohort(
    identifier: SilverIdentifier,
    patient: Patient,
    result: ParsedLabResultCohort
  )

  @BigQueryType.toTable
  case class SilverPriorAuthorization(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedPriorAuthorization
  )
  @BigQueryType.toTable
  case class SilverHealthRiskAssessment(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedHealthRiskAssessment
  )
}
