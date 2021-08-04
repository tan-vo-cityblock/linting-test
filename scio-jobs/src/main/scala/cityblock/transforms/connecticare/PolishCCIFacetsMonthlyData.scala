package cityblock.transforms.connecticare

import java.util.concurrent.TimeUnit

import cityblock.models.ConnecticareSilverClaims._
import cityblock.models.gold.FacilityClaim.Facility
import cityblock.models.gold.NewProvider.Provider
import cityblock.models.gold.PharmacyClaim.Pharmacy
import cityblock.models.gold.ProfessionalClaim.Professional
import cityblock.transforms.Transform
import cityblock.transforms.connecticare.gold.{FacilityFacetsTransformer, _}
import cityblock.utilities.{Environment, PartnerConfiguration, ScioUtils}
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object PolishCCIFacetsMonthlyData {
  implicit val waitDuration: Duration = Duration(1, TimeUnit.HOURS)

  def main(argv: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[GcpOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val project = opts.getProject

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val shard = LocalDate.parse(args.required("deliveryDate"), dtf)

    val sourceProject =
      args.getOrElse("sourceProject", PartnerConfiguration.connecticare.productionProject)
    val sourceDataset =
      args.getOrElse("sourceDataset", "silver_claims_facets")
    val destinationProject =
      args.getOrElse("destinationProject", PartnerConfiguration.connecticare.productionProject)
    val destinationIncrementalDataset =
      args.getOrElse("destinationIncrementalDataset", "gold_claims_facets")
    val destinationFinalDataset = args.getOrElse("destinationFinalDataset", "gold_claims")

    // this flag is primarily useful when testing airflow DAGs
    val checkArgsAndExit = args.boolean("checkArgsAndExit", default = false)

    if (project != "cityblock-data") {
      throw new Error(
        """
                        |Running PolishCCIFacetsMonthlyData in a project other than "cityblock-data". This
                        |would likely fail due to permissions errors. Please rerun with
                        |--project=cityblock-data.
        """.stripMargin)
    }

    if (!checkArgsAndExit) {
      val results: List[ScioResult] = polishCCIFacetsMonthlyData(
        argv,
        sourceDataset,
        sourceProject,
        destinationProject,
        destinationIncrementalDataset,
        destinationFinalDataset,
        shard
      )

      results.foreach(_.waitUntilDone())
    }
  }

  def medicalSilverQuery(shard: LocalDate, sourceProject: String, sourceDataset: String): String = {
    val tableName: String = {
      val tableShard: String = Transform.shardName("Medical_Facets_med", shard)
      s"`$sourceProject.$sourceDataset.$tableShard`"
    }

    s"""
      SELECT
        identifier,
        patient,
        STRUCT(data.EXT_4332_LOB,
          data.EXT_1208_ICD9_DIAG_CODE1,
          data.EXT_1208_ICD9_DIAG_CODE2,
          data.EXT_1208_ICD9_DIAG_CODE3,
          data.EXT_1208_ICD9_DIAG_CODE4,
          data.EXT_1208_ICD9_DIAG_CODE5,
          data.EXT_1208_ICD9_DIAG_CODE6,
          data.EXT_1208_ICD9_DIAG_CODE7,
          data.EXT_1208_ICD9_DIAG_CODE8,
          data.EXT_1208_ICD9_DIAG_CODE9,
          data.EXT_1208_ICD9_DIAG_CODE10,
          data.EXT_1208_ICD9_DIAG_CODE11,
          data.EXT_1208_ICD9_DIAG_CODE12,
          data.EXT_1216_PRIN_DIAG_CODE,
          data.EXT_1207_PLACE_OF_SERVICE,
          data.EXT_1219_PROC_MODIFY,
          data.EXT_2712_PROV_SPECIALTY,
          data.EXT_UNITS_N_1,
          data.EXT_1206_TYPE_OF_SERVICE,
          data.EXT_PAR_IND,
          data.EXT_PAYMT_STATUS_1,
          data.EXT_PAID_AMT_1,
          data.COINSURANCE,
          data.DEDUCTIBLE,
          data.COPAY,
          data.EXT_BILLED_AMT_1,
          data.EXT_ALLOWED_AMOUNT_1,
          data.EXT_END_DOS,
          data.EXT_BEG_DOS,
          data.EXT_1001_CLAIM_NUMBER,
          data.ICD_INDICATOR,
          data.EXT_2001_PROVIDER_ID,
          data.EXT_1218_PROC_CODE,
          data.EXT_1700_BILL_TYPE,
          data.EXT_1714_REVENUE_CODE1,
          data.EXT_1531_ADMIT_TYPE,
          data.EXT_1721_ADMIT_SOURCE,
          data.EXT_1704_DISCHARGE_STATUS,
          data.EXT_1503_ADMIT_DT,
          data.EXT_DISCHARGE_DT,
          data.EXT_1506_PRIN_PROC_CODE,
          data.EXT_1506_ICD9_PROC_CODE1,
          data.EXT_1506_ICD9_PROC_CODE2,
          data.EXT_1506_ICD9_PROC_CODE3,
          data.EXT_1506_ICD9_PROC_CODE4,
          data.EXT_1506_ICD9_PROC_CODE5,
          data.EXT_1506_ICD9_PROC_CODE6,
          data.EXT_1506_ICD9_PROC_CODE7,
          data.EXT_1506_ICD9_PROC_CODE8,
          data.EXT_1506_ICD9_PROC_CODE9,
          data.EXT_1506_ICD9_PROC_CODE10,
          data.EXT_1506_ICD9_PROC_CODE11,
          data.EXT_1506_ICD9_PROC_CODE12,
          data.EXT_1653_DRG,
          data.EXT_PAYMT_DATE_1,
          data.EXT_PAYMT_DATE_2
        ) as data
      FROM
        $tableName"""
  }

  def medicalRevenueCodesSilverQuery(shard: LocalDate,
                                     sourceProject: String,
                                     sourceDataset: String): String = {
    val tableName: String = {
      val tableShard: String = Transform.shardName("Medical_Facets_med", shard)
      s"`$sourceProject.$sourceDataset.$tableShard`"
    }

    s"""
      SELECT
        identifier,
        patient,
        STRUCT(data.EXT_1001_CLAIM_NUMBER,
          data.EXT_1714_REVENUE_CODE1,
          data.EXT_1714_REVENUE_CODE2,
          data.EXT_1714_REVENUE_CODE3,
          data.EXT_1714_REVENUE_CODE4,
          data.EXT_1714_REVENUE_CODE5,
          data.EXT_1714_REVENUE_CODE6,
          data.EXT_1714_REVENUE_CODE7,
          data.EXT_1714_REVENUE_CODE8,
          data.EXT_1714_REVENUE_CODE9,
          data.EXT_1714_REVENUE_CODE10,
          data.EXT_1714_REVENUE_CODE11,
          data.EXT_1714_REVENUE_CODE12,
          data.EXT_1714_REVENUE_CODE13,
          data.EXT_1714_REVENUE_CODE14,
          data.EXT_1714_REVENUE_CODE15,
          data.EXT_1714_REVENUE_CODE16,
          data.EXT_1714_REVENUE_CODE17,
          data.EXT_1714_REVENUE_CODE18,
          data.EXT_1714_REVENUE_CODE19,
          data.EXT_1714_REVENUE_CODE20,
          data.EXT_1714_REVENUE_CODE21,
          data.EXT_1714_REVENUE_CODE22,
          data.EXT_1714_REVENUE_CODE23,
          data.EXT_1714_REVENUE_CODE24,
          data.EXT_1714_REVENUE_CODE25,
          data.EXT_1714_REVENUE_CODE26,
          data.EXT_1714_REVENUE_CODE27,
          data.EXT_1714_REVENUE_CODE28,
          data.EXT_1714_REVENUE_CODE29,
          data.EXT_1714_REVENUE_CODE30,
          data.EXT_1714_REVENUE_CODE31,
          data.EXT_1714_REVENUE_CODE32,
          data.EXT_1714_REVENUE_CODE33,
          data.EXT_1714_REVENUE_CODE34,
          data.EXT_1714_REVENUE_CODE35,
          data.EXT_1714_REVENUE_CODE36,
          data.EXT_1714_REVENUE_CODE37,
          data.EXT_1714_REVENUE_CODE38,
          data.EXT_1714_REVENUE_CODE39,
          data.EXT_1714_REVENUE_CODE40,
          data.EXT_1714_REVENUE_CODE41,
          data.EXT_1714_REVENUE_CODE42,
          data.EXT_1714_REVENUE_CODE43,
          data.EXT_1714_REVENUE_CODE44,
          data.EXT_1714_REVENUE_CODE45,
          data.EXT_1714_REVENUE_CODE46,
          data.EXT_1714_REVENUE_CODE47,
          data.EXT_1714_REVENUE_CODE48,
          data.EXT_1714_REVENUE_CODE49,
          data.EXT_1714_REVENUE_CODE50,
          data.EXT_1714_REVENUE_CODE51,
          data.EXT_1714_REVENUE_CODE52,
          data.EXT_1714_REVENUE_CODE53,
          data.EXT_1714_REVENUE_CODE54,
          data.EXT_1714_REVENUE_CODE55,
          data.EXT_1714_REVENUE_CODE56,
          data.EXT_1714_REVENUE_CODE57,
          data.EXT_1714_REVENUE_CODE58,
          data.EXT_1714_REVENUE_CODE59,
          data.EXT_1714_REVENUE_CODE60,
          data.EXT_1714_REVENUE_CODE61,
          data.EXT_1714_REVENUE_CODE62,
          data.EXT_1714_REVENUE_CODE63,
          data.EXT_1714_REVENUE_CODE64,
          data.EXT_1714_REVENUE_CODE65,
          data.EXT_1714_REVENUE_CODE66,
          data.EXT_1714_REVENUE_CODE67,
          data.EXT_1714_REVENUE_CODE68,
          data.EXT_1714_REVENUE_CODE69,
          data.EXT_1714_REVENUE_CODE70,
          data.EXT_1714_REVENUE_CODE71,
          data.EXT_1714_REVENUE_CODE72,
          data.EXT_1714_REVENUE_CODE73,
          data.EXT_1714_REVENUE_CODE74,
          data.EXT_1714_REVENUE_CODE75,
          data.EXT_1714_REVENUE_CODE76,
          data.EXT_1714_REVENUE_CODE77,
          data.EXT_1714_REVENUE_CODE78,
          data.EXT_1714_REVENUE_CODE79,
          data.EXT_1714_REVENUE_CODE80,
          data.EXT_1714_REVENUE_CODE81,
          data.EXT_1714_REVENUE_CODE82,
          data.EXT_1714_REVENUE_CODE83,
          data.EXT_1714_REVENUE_CODE84,
          data.EXT_1714_REVENUE_CODE85,
          data.EXT_1714_REVENUE_CODE86,
          data.EXT_1714_REVENUE_CODE87,
          data.EXT_1714_REVENUE_CODE88,
          data.EXT_1714_REVENUE_CODE89,
          data.EXT_1714_REVENUE_CODE90,
          data.EXT_1714_REVENUE_CODE91,
          data.EXT_1714_REVENUE_CODE92,
          data.EXT_1714_REVENUE_CODE93,
          data.EXT_1714_REVENUE_CODE94,
          data.EXT_1714_REVENUE_CODE95,
          data.EXT_1714_REVENUE_CODE96,
          data.EXT_1714_REVENUE_CODE97,
          data.EXT_1714_REVENUE_CODE98,
          data.EXT_1714_REVENUE_CODE99
        ) as data
      FROM
        $tableName"""
  }

  def medicalRevenueUnitsSilverQuery(shard: LocalDate,
                                     sourceProject: String,
                                     sourceDataset: String): String = {
    val tableName: String = {
      val tableShard: String = Transform.shardName("Medical_Facets_med", shard)
      s"`$sourceProject.$sourceDataset.$tableShard`"
    }

    s"""
      SELECT
        identifier,
        patient,
        STRUCT(data.EXT_1001_CLAIM_NUMBER,
          data.EXT_REVENUE_UNITS1,
          data.EXT_REVENUE_UNITS2,
          data.EXT_REVENUE_UNITS3,
          data.EXT_REVENUE_UNITS4,
          data.EXT_REVENUE_UNITS5,
          data.EXT_REVENUE_UNITS6,
          data.EXT_REVENUE_UNITS7,
          data.EXT_REVENUE_UNITS8,
          data.EXT_REVENUE_UNITS9,
          data.EXT_REVENUE_UNITS10,
          data.EXT_REVENUE_UNITS11,
          data.EXT_REVENUE_UNITS12,
          data.EXT_REVENUE_UNITS13,
          data.EXT_REVENUE_UNITS14,
          data.EXT_REVENUE_UNITS15,
          data.EXT_REVENUE_UNITS16,
          data.EXT_REVENUE_UNITS17,
          data.EXT_REVENUE_UNITS18,
          data.EXT_REVENUE_UNITS19,
          data.EXT_REVENUE_UNITS21,
          data.EXT_REVENUE_UNITS22,
          data.EXT_REVENUE_UNITS23,
          data.EXT_REVENUE_UNITS24,
          data.EXT_REVENUE_UNITS25,
          data.EXT_REVENUE_UNITS26,
          data.EXT_REVENUE_UNITS27,
          data.EXT_REVENUE_UNITS28,
          data.EXT_REVENUE_UNITS29,
          data.EXT_REVENUE_UNITS30,
          data.EXT_REVENUE_UNITS31,
          data.EXT_REVENUE_UNITS32,
          data.EXT_REVENUE_UNITS33,
          data.EXT_REVENUE_UNITS34,
          data.EXT_REVENUE_UNITS35,
          data.EXT_REVENUE_UNITS36,
          data.EXT_REVENUE_UNITS37,
          data.EXT_REVENUE_UNITS38,
          data.EXT_REVENUE_UNITS39,
          data.EXT_REVENUE_UNITS40,
          data.EXT_REVENUE_UNITS41,
          data.EXT_REVENUE_UNITS42,
          data.EXT_REVENUE_UNITS43,
          data.EXT_REVENUE_UNITS44,
          data.EXT_REVENUE_UNITS45,
          data.EXT_REVENUE_UNITS46,
          data.EXT_REVENUE_UNITS47,
          data.EXT_REVENUE_UNITS48,
          data.EXT_REVENUE_UNITS49,
          data.EXT_REVENUE_UNITS50,
          data.EXT_REVENUE_UNITS51,
          data.EXT_REVENUE_UNITS52,
          data.EXT_REVENUE_UNITS53,
          data.EXT_REVENUE_UNITS54,
          data.EXT_REVENUE_UNITS55,
          data.EXT_REVENUE_UNITS56,
          data.EXT_REVENUE_UNITS57,
          data.EXT_REVENUE_UNITS58,
          data.EXT_REVENUE_UNITS59,
          data.EXT_REVENUE_UNITS60,
          data.EXT_REVENUE_UNITS61,
          data.EXT_REVENUE_UNITS62,
          data.EXT_REVENUE_UNITS63,
          data.EXT_REVENUE_UNITS64,
          data.EXT_REVENUE_UNITS65,
          data.EXT_REVENUE_UNITS66,
          data.EXT_REVENUE_UNITS67,
          data.EXT_REVENUE_UNITS68,
          data.EXT_REVENUE_UNITS69,
          data.EXT_REVENUE_UNITS70,
          data.EXT_REVENUE_UNITS71,
          data.EXT_REVENUE_UNITS72,
          data.EXT_REVENUE_UNITS73,
          data.EXT_REVENUE_UNITS74,
          data.EXT_REVENUE_UNITS75,
          data.EXT_REVENUE_UNITS76,
          data.EXT_REVENUE_UNITS77,
          data.EXT_REVENUE_UNITS78,
          data.EXT_REVENUE_UNITS79,
          data.EXT_REVENUE_UNITS80,
          data.EXT_REVENUE_UNITS81,
          data.EXT_REVENUE_UNITS82,
          data.EXT_REVENUE_UNITS83,
          data.EXT_REVENUE_UNITS84,
          data.EXT_REVENUE_UNITS85,
          data.EXT_REVENUE_UNITS86,
          data.EXT_REVENUE_UNITS87,
          data.EXT_REVENUE_UNITS88,
          data.EXT_REVENUE_UNITS89,
          data.EXT_REVENUE_UNITS90,
          data.EXT_REVENUE_UNITS91,
          data.EXT_REVENUE_UNITS92,
          data.EXT_REVENUE_UNITS93,
          data.EXT_REVENUE_UNITS94,
          data.EXT_REVENUE_UNITS95,
          data.EXT_REVENUE_UNITS96,
          data.EXT_REVENUE_UNITS97,
          data.EXT_REVENUE_UNITS98,
          data.EXT_REVENUE_UNITS99
        ) as data
      FROM
        $tableName"""
  }

  def polishCCIFacetsMonthlyData(
    args: Array[String],
    sourceDataset: String,
    sourceProject: String,
    destinationProject: String,
    destinationIncrementalDataset: String,
    destinationFinalDataset: String,
    shard: LocalDate
  )(implicit environment: Environment): List[ScioResult] = {
    def fetch[T <: HasAnnotation: TypeTag: ClassTag: Coder](
      sc: ScioContext,
      table: String
    ): SCollection[T] =
      Transform
        .fetchFromBigQuery[T](sc, sourceProject, sourceDataset, table, shard)

    def persist[T <: HasAnnotation: TypeTag: ClassTag: Coder](
      destinationTable: String,
      destinationDataset: String,
      goldData: SCollection[T]
    ): Future[Tap[T]] =
      Transform.persist(
        goldData,
        destinationProject,
        destinationDataset,
        destinationTable,
        shard,
        WRITE_TRUNCATE,
        CREATE_IF_NEEDED
      )

    val (_, silverMedical) = ScioUtils.runJob("polish-cci-facets-fetch-silver-medical", args) {
      sc =>
        val medicalQuery: String = medicalSilverQuery(shard, sourceProject, sourceDataset)
        sc.typedBigQuery[FacetsMedical](medicalQuery).materialize
    }

    val (_, silverRevenueCode) =
      ScioUtils.runJob("polish-cci-facets-fetch-silver-revenue-codes", args) { sc =>
        val revenueCodeQuery: String =
          medicalRevenueCodesSilverQuery(shard, sourceProject, sourceDataset)
        sc.typedBigQuery[FacetsMedicalRevenueCodes](revenueCodeQuery).materialize
      }

    val (_, silverRevenueUnit) =
      ScioUtils.runJob("polish-cci-facets-fetch-silver-revenue-units", args) { sc =>
        val revenueUnitQuery: String =
          medicalRevenueUnitsSilverQuery(shard, sourceProject, sourceDataset)
        sc.typedBigQuery[FacetsMedicalRevenueUnits](revenueUnitQuery).materialize
      }

    def isFacilityFacets(silverClaims: Iterable[FacetsMedical]): Boolean =
      silverClaims.exists(silver => {
        silver.data.EXT_1714_REVENUE_CODE1.isDefined || silver.data.EXT_1700_BILL_TYPE.isDefined
      })

    def isProfessionalFacets(silverClaims: Iterable[FacetsMedical]): Boolean =
      !isFacilityFacets(silverClaims)

    val (professionalResult, _) = ScioUtils.runJob("polish-cci-facets-gold-professional", args) {
      sc =>
        val medicals = ScioUtils
          .waitAndOpen(silverMedical, sc)
          .groupBy(line => mkFacetsPartnerClaimId(line))
          .filter {
            case (_, lines) => isProfessionalFacets(lines)
          }
          .flatMap(_._2)

        val professionalFacets: SCollection[Professional] =
          ProfessionalFacetsTransformer(medicals).transform()(sourceProject)

        persist[Professional]("Professional", destinationIncrementalDataset, professionalFacets)
    }

    val (facilityResult, _) = ScioUtils.runJob("polish-cci-facets-gold-facility", args) { sc =>
      val medicals = ScioUtils
        .waitAndOpen(silverMedical, sc)
        .groupBy(_.data.EXT_1001_CLAIM_NUMBER)
        .filter {
          case (_, lines) => isFacilityFacets(lines)
        }
        .flatMap(_._2)

      val revenueCodes = ScioUtils
        .waitAndOpen(silverRevenueCode, sc)

      val revenueUnits = ScioUtils
        .waitAndOpen(silverRevenueUnit, sc)

      val facilityFacets: SCollection[Facility] =
        FacilityFacetsTransformer(medicals, revenueCodes, revenueUnits).transform()(sourceProject)

      persist[Facility]("Facility", destinationIncrementalDataset, facilityFacets)
    }

    val (pharmacyResult, _) = ScioUtils.runJob("polish-facets-transform-gold-pharmacy", args) {
      sc =>
        val pharmacyFacets = PharmacyFacetsTransformer(
          fetch[FacetsPharmacyMed](sc, "FacetsPharmacy_med")
        ).transform()(sourceProject)

        persist[Pharmacy]("Pharmacy", destinationIncrementalDataset, pharmacyFacets)
    }

    val (providerResult, _) = ScioUtils.runJob("polish-facets-transform-gold-provider", args) {
      sc =>
        val providerFacets = ProviderFacetsTransformer(
          fetch[FacetsProvider](sc, "FacetsProvider")
        ).transform()(sourceProject)

        persist[Provider]("Provider", destinationIncrementalDataset, providerFacets)
    }

    List(professionalResult, facilityResult, pharmacyResult, providerResult)
  }
}
