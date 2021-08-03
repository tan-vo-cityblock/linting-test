package cityblock.transforms.tufts

import cityblock.models.gold.Amount
import cityblock.models.gold.FacilityClaim.PreCombinedFacility
import cityblock.models.gold.ProfessionalClaim.PreCombinedProfessional
import cityblock.transforms.Transform
import cityblock.transforms.combine.Combine
import cityblock.transforms.tufts.gold.{CombinePharmacy, MergeFacility, MergeProfessional}
import cityblock.utilities.{Environment, ScioUtils}
import com.spotify.scio.{Args, ScioContext, ScioResult}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection

object CombineTuftsData {
  def main(argv: Array[String]): Unit = {
    val (_, args): (GcpOptions, Args) = ScioContext.parseArguments[GcpOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val oldShard = args.required("previousDeliveryDate")
    val newShard = args.required("deliveryDate")

    val sourceNewProject: String = args.required("sourceNewProject")
    val sourceOldProject = args.required("sourceOldProject")
    val sourceOldDataset: String = args.required("sourceOldDataset")
    val sourceNewDataset = args.required("sourceNewDataset")
    val destinationDataset = args.required("destinationDataset")
    val destinationProject = args.required("destinationProject")

    val results: List[ScioResult] =
      mergeTuftsData(
        argv,
        sourceOldDataset,
        sourceNewDataset,
        sourceNewProject,
        sourceOldProject,
        oldShard,
        newShard,
        destinationDataset,
        destinationProject
      )

    results.map(_.waitUntilDone())
  }

  // scalastyle:off method.length
  // scalastyle:off parameter.number
  def mergeTuftsData(
    args: Array[String],
    sourceOldDataset: String,
    sourceNewDataset: String,
    sourceNewProject: String,
    sourceOldProject: String,
    oldShard: String,
    newShard: String,
    destinationDataset: String,
    destinationProject: String
  )(implicit environment: Environment): List[ScioResult] = {
    val writeDisposition = WRITE_TRUNCATE
    val createDisposition = CREATE_IF_NEEDED

    def professionalQuery(dataset: String, shard: String, project: String): String =
      s"""
       |WITH
       |  memberClaims AS (
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
       |    memberClaims),
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
       |    STRUCT(CAST(l.amount.allowed * 100 AS INT64) AS allowed,
       |      CAST(l.amount.billed * 100 AS INT64) AS billed,
       |      CAST(l.amount.cob * 100 AS INT64) AS cob,
       |      CAST(l.amount.copay * 100 AS INT64) AS copay,
       |      CAST(l.amount.deductible * 100 AS INT64) AS deductible,
       |      CAST(l.amount.coinsurance * 100 AS INT64) AS coinsurance,
       |      CAST(l.amount.planPaid * 100 AS INT64) AS planPaid) amount,
       |    silver_med.data.Version_Number
       |  FROM
       |    memberClaims,
       |    UNNEST(lines) l join `$project.silver_claims.MedicalClaim_*` silver_med on l.surrogate.id = silver_med.identifier.surrogateId),
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
       |        amount,
       |        Version_Number)) lines
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

    def facilityQuery(dataset: String, shard: String, project: String): String =
      s"""
         |WITH
         |  memberClaims AS (
         |  SELECT
         |    *
         |  FROM
            `$project.$dataset.Facility_$shard`),
         |  headerInfo AS (
         |  SELECT
         |    claimId,
         |    memberIdentifier,
         |    header
         |  FROM
         |    memberClaims),
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
         |    STRUCT(CAST(l.amount.allowed * 100 AS INT64) AS allowed,
         |      CAST(l.amount.billed * 100 AS INT64) AS billed,
         |      CAST(l.amount.cob * 100 AS INT64) AS cob,
         |      CAST(l.amount.copay * 100 AS INT64) AS copay,
         |      CAST(l.amount.deductible * 100 AS INT64) AS deductible,
         |      CAST(l.amount.coinsurance * 100 AS INT64) AS coinsurance,
         |      CAST(l.amount.planPaid * 100 AS INT64) AS planPaid) amount,
         |    silver_med.data.Version_Number
         |  FROM
         |    memberClaims,
         |    UNNEST(lines) l join `$project.silver_claims.MedicalClaim_*` silver_med on l.surrogate.id = silver_med.identifier.surrogateId),
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
         |        amount,
         |        Version_Number)) lines
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

    def fetchProfessionalClaimsAndFixAmounts(
      sc: ScioContext,
      dataset: String,
      shard: String,
      project: String
    ): SCollection[PreCombinedProfessional] =
      sc.typedBigQuery[PreCombinedProfessional](professionalQuery(dataset, shard, sourceNewProject))
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
    ): SCollection[PreCombinedFacility] =
      sc.typedBigQuery[PreCombinedFacility](facilityQuery(dataset, shard, project))
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

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val newDate = LocalDate.parse(newShard, dtf)

    val (professionalResult, _) = ScioUtils.runJob("tufts-merge-gold-professional", args) { sc =>
      val professional = MergeProfessional(
        fetchProfessionalClaimsAndFixAmounts(sc, sourceOldDataset, oldShard, sourceOldProject),
        fetchProfessionalClaimsAndFixAmounts(sc, sourceNewDataset, newShard, sourceNewProject),
      ).merge()

      Transform.persist(
        professional,
        destinationProject,
        destinationDataset,
        "Professional",
        newDate,
        writeDisposition,
        createDisposition
      )
    }

    val (pharmacyResult, _) = ScioUtils.runJob("cci-merge-gold-pharmacy", args) { sc =>
      val oldPharmacy =
        Combine.fetchPharmacyClaimsAndFixAmounts(sc, sourceOldDataset, oldShard, sourceOldProject)
      val newPharmacy =
        Combine.fetchPharmacyClaimsAndFixAmounts(sc, sourceNewDataset, newShard, sourceNewProject)

      val pharmacy = CombinePharmacy(
        oldPharmacy,
        newPharmacy,
      ).combine()

      Transform.persist(
        pharmacy,
        destinationProject,
        destinationDataset,
        "Pharmacy",
        newDate,
        writeDisposition,
        createDisposition
      )
    }

    val (facilityResult, _) = ScioUtils.runJob("tufts-merge-gold-facility", args) { sc =>
      val facility = MergeFacility(
        fetchFacilityClaimsAndFixAmounts(sc, sourceOldDataset, oldShard, sourceOldProject),
        fetchFacilityClaimsAndFixAmounts(sc, sourceNewDataset, newShard, sourceNewProject)
      ).merge()

      Transform.persist(
        facility,
        destinationProject,
        destinationDataset,
        "Facility",
        newDate,
        writeDisposition,
        createDisposition
      )
    }

    List(professionalResult, pharmacyResult, facilityResult)
  }
}
