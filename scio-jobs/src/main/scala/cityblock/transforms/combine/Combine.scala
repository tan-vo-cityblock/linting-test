package cityblock.transforms.combine

import cityblock.models.gold.Amount
import cityblock.models.gold.FacilityClaim.Facility
import cityblock.models.gold.PharmacyClaim.Pharmacy
import cityblock.models.gold.ProfessionalClaim.Professional
import com.spotify.scio.bigquery._
import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection

// NOTE: Will be deprecated once Scio is upgraded and BigDecimal values can be read in without error.
object Combine {
  def facilityQuery(dataset: String, shard: String, project: String): String =
    s"""
       |WITH
       |  claims AS (
       |  SELECT
       |    *
       |  FROM
       |    `$project.$dataset.Facility_$shard`),
       |  headerInfo AS (
       |  SELECT
       |    claimId,
       |    memberIdentifier,
       |    header
       |  FROM
       |    claims),
       |  cleanedLines AS (
       |  SELECT
       |    claimId,
       |    l.surrogate,
       |    l.lineNumber,
       |    l.revenueCode,
       |    l.cobFlag,
       |    l.capitatedFlag,
       |    l.claimLineStatus,
       |    l.inNetworkFlag,
       |    l.serviceQuantity,
       |    l.typesOfService,
       |    l.procedure,
       |    STRUCT(SAFE_CAST(l.amount.allowed * 100 AS INT64) AS allowed,
       |      SAFE_CAST(l.amount.billed * 100 AS INT64) AS billed,
       |      SAFE_CAST(l.amount.cob * 100 AS INT64) AS cob,
       |      SAFE_CAST(l.amount.copay * 100 AS INT64) AS copay,
       |      SAFE_CAST(l.amount.deductible * 100 AS INT64) AS deductible,
       |      SAFE_CAST(l.amount.coinsurance * 100 AS INT64) AS coinsurance,
       |      SAFE_CAST(l.amount.planPaid * 100 AS INT64) AS planPaid) amount
       |  FROM
       |    claims,
       |    UNNEST(lines) l),
       |  groupedLines AS (
       |  SELECT
       |    claimId,
       |    ARRAY_AGG(STRUCT(surrogate,
       |        lineNumber,
       |        revenueCode,
       |        cobFlag,
       |        capitatedFlag,
       |        claimLineStatus,
       |        inNetworkFlag,
       |        serviceQuantity,
       |        typesOfService,
       |        `procedure`,
       |        amount)) lines
       |  FROM
       |    cleanedLines
       |  GROUP BY
       |    claimId)
       |SELECT
       |  claimId,
       |  memberIdentifier,
       |  header,
       |  lines
       |FROM
       |  headerInfo
       |JOIN
       |  groupedLines
       |USING
       |  (claimId)
     """.stripMargin

  def professionalQuery(dataset: String, shard: String, project: String): String =
    s"""
       |WITH
       |  claims AS (
       |  SELECT
       |    *
       |  FROM
       |    `$project.$dataset.Professional_$shard`),
       |  headerInfo AS (
       |  SELECT
       |    claimId,
       |    memberIdentifier,
       |    header
       |  FROM
       |    claims),
       |  cleanedLines AS (
       |  SELECT
       |    claimId,
       |    l.surrogate,
       |    l.lineNumber,
       |    l.cobFlag,
       |    l.capitatedFlag,
       |    l.claimLineStatus,
       |    l.inNetworkFlag,
       |    l.serviceQuantity,
       |    l.placeOfService,
       |    l.typesOfService,
       |    l.date,
       |    l.provider,
       |    l.procedure,
       |    l.diagnoses,
       |    STRUCT(SAFE_CAST(l.amount.allowed * 100 AS INT64) AS allowed,
       |      SAFE_CAST(l.amount.billed * 100 AS INT64) AS billed,
       |      SAFE_CAST(l.amount.cob * 100 AS INT64) AS cob,
       |      SAFE_CAST(l.amount.copay * 100 AS INT64) AS copay,
       |      SAFE_CAST(l.amount.deductible * 100 AS INT64) AS deductible,
       |      SAFE_CAST(l.amount.coinsurance * 100 AS INT64) AS coinsurance,
       |      SAFE_CAST(l.amount.planPaid * 100 AS INT64) AS planPaid) amount
       |  FROM
       |    claims,
       |    UNNEST(lines) l),
       |  groupedLines AS (
       |  SELECT
       |    claimId,
       |    ARRAY_AGG(STRUCT(surrogate,
       |        lineNumber,
       |        cobFlag,
       |        capitatedFlag,
       |        claimLineStatus,
       |        inNetworkFlag,
       |        serviceQuantity,
       |        placeOfService,
       |        date,
       |        provider,
       |        `procedure`,
       |        diagnoses,
       |        typesOfService,
       |        amount)) lines
       |  FROM
       |    cleanedLines
       |  GROUP BY
       |    claimId)
       |SELECT
       |  claimId,
       |  memberIdentifier,
       |  header,
       |  lines
       |FROM
       |  headerInfo
       |JOIN
       |  groupedLines
       |USING
       |  (claimId)
     """.stripMargin

  def pharmacyQuery(dataset: String, shard: String, project: String): String =
    s"""
       |SELECT
       |  identifier,
       |  memberIdentifier,
       |  capitatedFlag,
       |  claimLineStatus,
       |  cobFlag,
       |  lineOfBusiness,
       |  subLineOfBusiness,
       |  pharmacy,
       |  prescriber,
       |  diagnosis,
       |  STRUCT(drug.ndc as ndc,
       |  SAFE_CAST(drug.quantityDispensed * 100 AS INT64) AS quantityDispensed,
       |  drug.daysSupply as daysSupply,
       |  drug.partnerPrescriptionNumber as partnerPrescriptionNumber,
       |  drug.fillNumber as fillNumber,
       |  drug.brandIndicator as brandIndicator,
       |  drug.ingredient as ingredient,
       |  drug.strength as strength,
       |  drug.dispenseAsWritten as dispenseAsWritten,
       |  drug.dispenseMethod as dispenseMethod,
       |  drug.classes as classes,
       |  drug.formularyFlag as formularyFlag) drug,
       |  STRUCT(SAFE_CAST(amount.allowed * 100 AS INT64) AS allowed,
       |  SAFE_CAST(amount.billed * 100 AS INT64) AS billed,
       |  SAFE_CAST(amount.cob * 100 AS INT64) AS cob,
       |  SAFE_CAST(amount.copay * 100 AS INT64) AS copay,
       |  SAFE_CAST(amount.deductible * 100 AS INT64) AS deductible,
       |  SAFE_CAST(amount.coinsurance * 100 AS INT64) AS coinsurance,
       |  SAFE_CAST(amount.planPaid * 100 AS INT64) AS planPaid) amount,
       |  date
       |
       |FROM
       |  `$project.$dataset.Pharmacy_$shard`
     """.stripMargin

  def fetchProfessionalClaimsAndFixAmounts(
    sc: ScioContext,
    dataset: String,
    shard: String,
    project: String
  ): SCollection[Professional] =
    sc.typedBigQuery[Professional](professionalQuery(dataset, shard, project))
      .map(prof => {
        prof.copy(lines = prof.lines.map(line => {
          line.copy(amount = Amount(
            allowed = line.amount.allowed.map(_ / 100.00),
            billed = line.amount.billed.map(_ / 100.00),
            cob = line.amount.cob.map(_ / 100.00),
            copay = line.amount.copay.map(_ / 100.00),
            deductible = line.amount.deductible.map(_ / 100.00),
            coinsurance = line.amount.coinsurance.map(_ / 100.00),
            planPaid = line.amount.planPaid.map(_ / 100.00)
          ))
        }))
      })

  def fetchFacilityClaimsAndFixAmounts(
    sc: ScioContext,
    dataset: String,
    shard: String,
    project: String
  ): SCollection[Facility] =
    sc.typedBigQuery[Facility](facilityQuery(dataset, shard, project))
      .map(fac => {
        fac.copy(lines = fac.lines.map(line => {
          line.copy(amount = Amount(
            allowed = line.amount.allowed.map(_ / 100.00),
            billed = line.amount.billed.map(_ / 100.00),
            cob = line.amount.cob.map(_ / 100.00),
            copay = line.amount.copay.map(_ / 100.00),
            deductible = line.amount.deductible.map(_ / 100.00),
            coinsurance = line.amount.coinsurance.map(_ / 100.00),
            planPaid = line.amount.planPaid.map(_ / 100.00)
          ))
        }))
      })

  def fetchPharmacyClaimsAndFixAmounts(
    sc: ScioContext,
    dataset: String,
    shard: String,
    project: String
  ): SCollection[Pharmacy] =
    sc.typedBigQuery[Pharmacy](pharmacyQuery(dataset, shard, project))
      .map(pharm => {
        pharm.copy(
          drug = pharm.drug.copy(quantityDispensed = pharm.drug.quantityDispensed.map(_ / 100.00)),
          amount = Amount(
            allowed = pharm.amount.allowed.map(_ / 100.00),
            billed = pharm.amount.billed.map(_ / 100.00),
            cob = pharm.amount.cob.map(_ / 100.00),
            copay = pharm.amount.copay.map(_ / 100.00),
            deductible = pharm.amount.deductible.map(_ / 100.00),
            coinsurance = pharm.amount.coinsurance.map(_ / 100.00),
            planPaid = pharm.amount.planPaid.map(_ / 100.00)
          )
        )
      })
}
