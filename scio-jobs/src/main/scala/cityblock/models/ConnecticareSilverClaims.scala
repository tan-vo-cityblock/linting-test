package cityblock.models

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.connecticare.silver.LabResult.ParsedLabResults
import cityblock.models.connecticare.silver.MedicalDiagnosis.ParsedMedicalDiagnosis
import cityblock.models.connecticare.silver.MedicalICDProcedure.ParsedMedicalICDProcedure
import cityblock.models.connecticare.silver.Member.ParsedMember
import cityblock.models.connecticare.silver.Pharmacy.ParsedPharmacy
import cityblock.models.connecticare.silver.AttributedPCP.ParsedAttributedPCP
import cityblock.models.connecticare.silver.FacetsMedical.ParsedFacetsMedical
import cityblock.models.connecticare.silver.FacetsMedicalRevenueCodes.ParsedFacetsRevenueCodes
import cityblock.models.connecticare.silver.FacetsMedicalRevenueUnits.ParsedFacetsRevenueUnits
import cityblock.models.connecticare.silver.FacetsMember.ParsedFacetsMember
import cityblock.models.connecticare.silver.FacetsPharmacyMed.ParsedFacetsPharmacyMed
import cityblock.models.connecticare.silver.FacetsProvider.ParsedFacetsProvider
import cityblock.models.connecticare.silver.HealthCoverageBenefits.ParsedHealthCoverageBenefits
import cityblock.models.connecticare.silver.Medical.ParsedMedical
import cityblock.models.connecticare.silver.PharmacyMedicare.ParsedPharmacyMedicare
import cityblock.models.connecticare.silver.ProviderAddress.ParsedProviderAddress
import cityblock.models.connecticare.silver.ProviderDetail.ParsedProviderDetail
import cityblock.models.connecticare.silver.UBH.ParsedUBH
import cityblock.models.connecticare.silver.UBHDiagnosis.ParsedUBHDiagnosis
import cityblock.models.connecticare.silver.UBHMedicare.ParsedUBHMedicare
import com.spotify.scio.bigquery.types.BigQueryType

object ConnecticareSilverClaims {
  @BigQueryType.toTable
  case class ProviderDetail(
    identifier: SilverIdentifier,
    detail: ParsedProviderDetail
  )

  @BigQueryType.toTable
  case class ProviderAddress(
    identifier: SilverIdentifier,
    address: ParsedProviderAddress
  )

  @BigQueryType.toTable
  case class Member(
    identifier: SilverIdentifier,
    patient: Patient,
    member: ParsedMember
  )

  @BigQueryType.toTable
  case class FacetsMember(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedFacetsMember
  )

  @BigQueryType.toTable
  case class FacetsPharmacyMed(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedFacetsPharmacyMed
  )

  @BigQueryType.toTable
  case class FacetsProvider(
    identifier: SilverIdentifier,
    data: ParsedFacetsProvider
  )

  @BigQueryType.toTable
  case class FacetsMedical(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedFacetsMedical
  )

  @BigQueryType.toTable
  case class FacetsMedicalRevenueCodes(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedFacetsRevenueCodes
  )

  @BigQueryType.toTable
  case class FacetsMedicalRevenueUnits(
    identifier: SilverIdentifier,
    patient: Patient,
    data: ParsedFacetsRevenueUnits
  )

  @BigQueryType.toTable
  case class HealthCoverageBenefits(
    identifier: SilverIdentifier,
    data: ParsedHealthCoverageBenefits
  )

  @BigQueryType.toTable
  case class AttributedPCP(
    identifier: SilverIdentifier,
    data: ParsedAttributedPCP
  )

  @BigQueryType.toTable
  case class Medical(
    identifier: SilverIdentifier,
    patient: Patient,
    medical: ParsedMedical
  )

  @BigQueryType.toTable
  case class Pharmacy(
    identifier: SilverIdentifier,
    patient: Patient,
    pharmacy: ParsedPharmacy
  )

  @BigQueryType.toTable
  case class PharmacyMedicare(
    identifier: SilverIdentifier,
    patient: Patient,
    pharmacy: ParsedPharmacyMedicare
  )

  @BigQueryType.toTable
  case class MedicalDiagnosis(
    identifier: SilverIdentifier,
    diagnosis: ParsedMedicalDiagnosis
  )

  @BigQueryType.toTable
  case class MedicalICDProcedure(
    identifier: SilverIdentifier,
    procedure: ParsedMedicalICDProcedure
  )

  @BigQueryType.toTable
  case class LabResults(
    identifier: SilverIdentifier,
    patient: Patient,
    results: ParsedLabResults
  )

  @BigQueryType.toTable
  case class UBH(
    identifier: SilverIdentifier,
    patient: Patient,
    claim: ParsedUBH
  )

  @BigQueryType.toTable
  case class UBHMedicare(
    identifier: SilverIdentifier,
    patient: Patient,
    claim: ParsedUBHMedicare
  )

  @BigQueryType.toTable
  case class UBHDiagnosis(
    identifier: SilverIdentifier,
    diagnosis: ParsedUBHDiagnosis
  )
}
