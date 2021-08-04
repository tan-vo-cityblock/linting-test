package cityblock.transforms.connecticare

import java.util.concurrent.TimeUnit

import cityblock.models.ConnecticareSilverClaims._
import cityblock.models.gold.FacilityClaim.Facility
import cityblock.models.gold.ProfessionalClaim.Professional
import cityblock.transforms.Transform
import cityblock.transforms.connecticare.gold._
import cityblock.utilities.reference.tables.{DiagnosisRelatedGroup, PlaceOfService, RevenueCode}
import cityblock.utilities.{reference, Environment, PartnerConfiguration, ScioUtils}
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.concurrent.duration.Duration
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object PolishCCIClaims {
  implicit val waitDuration: Duration = Duration(1, TimeUnit.HOURS)

  def main(argv: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[GcpOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val project = opts.getProject

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val shard = LocalDate.parse(args.required("deliveryDate"), dtf)

    val sourceProject =
      args.getOrElse("sourceProject", PartnerConfiguration.connecticare.productionProject)
    val sourceDataset = args.getOrElse(
      "sourceDataset",
      "silver_claims"
    )
    val destinationProject =
      args.getOrElse("destinationProject", PartnerConfiguration.connecticare.productionProject)
    val destinationIncrementalDataset =
      args.getOrElse("destinationIncrementalDataset", "gold_claims_incremental_amysis")
    val destinationFinalDataset = args.getOrElse("destinationFinalDataset", "gold_claims")

    // this flag is primarily useful when testing airflow DAGs
    val checkArgsAndExit = args.boolean("checkArgsAndExit", default = false)

    if (project != "cityblock-data") {
      throw new Error("""
          |Running PolishCCIData in a project other than "cityblock-data". This
          |would likely fail due to permissions errors. Please rerun with
          |--project=cityblock-data.
        """.stripMargin)
    }

    if (!checkArgsAndExit) {
      val results = polishCCIClaims(
        argv,
        sourceDataset,
        sourceProject,
        destinationProject,
        destinationIncrementalDataset,
        destinationFinalDataset,
        shard
      )

      results.map(_.waitUntilDone())
    }
  }

  // scalastyle:off method.length
  def polishCCIClaims(
    args: Array[String],
    sourceDataset: String,
    sourceProject: String,
    destinationProject: String,
    destinationIncrementalDataset: String,
    destinationFinalDataset: String,
    shard: LocalDate
  )(
    implicit environment: Environment
  ): List[ScioResult] = {
    val writeDisposition = WRITE_TRUNCATE
    val createDisposition = CREATE_IF_NEEDED

    def fetch[T <: HasAnnotation: TypeTag: ClassTag: Coder](
      sc: ScioContext,
      table: String
    ): SCollection[T] =
      Transform
        .fetchFromBigQuery[T](sc, sourceProject, sourceDataset, table, shard)

    val (_, indices) = {
      ScioUtils.runJob("polish-cci-diagnosis-indices", args) { sc =>
        Indices(sourceProject,
                fetch[MedicalDiagnosis](sc, "MedicalDiagnosis"),
                fetch[MedicalICDProcedure](sc, "MedicalICDProc")).futures
      }
    }

    val (_, commercialMedical) = {
      ScioUtils.runJob("polish-cci-validate-claims", args) { sc =>
        val commercial = fetch[Medical](sc, "Medical")

        val drg = DiagnosisRelatedGroup.fetchDistinct(sc)
        val rev = RevenueCode.fetchDistinct(sc)
        val pos = PlaceOfService.fetchDistinct(sc)
        val bill = reference.tables.TypeOfBill.fetchDistinct(sc)

        val medicalValidator = new MedicalValidator(drg, rev, pos, bill)

        medicalValidator.validate(commercial).groupBy(ClaimKey(_)).materialize
      }
    }

    val (professionalResult, _) = ScioUtils.runJob("polish-cci-transform-gold-professional", args) {
      sc =>
        val professionalMedical: SCollection[Professional] = ProfessionalTransformer(
          ScioUtils.waitAndOpen(indices.claimDiagnosisCom, sc),
          ScioUtils
            .waitAndOpen(commercialMedical, sc)
            .filter {
              case (_, claims) => isProfessional(claims)
            }
            .flatMap(_._2),
        ).transform()(sourceProject)

        Transform.persist(
          professionalMedical,
          destinationProject,
          destinationIncrementalDataset,
          "Professional",
          shard,
          writeDisposition,
          createDisposition
        )
    }

    val (facilityResult, _) = ScioUtils.runJob("polish-cci-transform-gold-facility", args) { sc =>
      val facilityMedical: SCollection[Facility] = FacilityTransformer(
        ScioUtils.waitAndOpen(indices.claimDiagnosisCom, sc),
        ScioUtils.waitAndOpen(indices.claimProcedureCom, sc),
        ScioUtils
          .waitAndOpen(commercialMedical, sc)
          .filter {
            case (_, claims) => isFacility(claims)
          }
          .flatMap(_._2)
      ).transform()(sourceProject)

      Transform.persist(
        facilityMedical,
        destinationProject,
        destinationIncrementalDataset,
        "Facility",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    val (providerResult, _) = ScioUtils.runJob("polish-cci-transform-gold-provider", args) { sc =>
      val provider = ProviderTransformer(
        fetch[ProviderDetail](sc, "ProviderDetail"),
        fetch[ProviderAddress](sc, "ProviderAddresses"),
        shard
      ).transform()(sourceProject)

      Transform.persist(
        provider,
        destinationProject,
        destinationIncrementalDataset,
        "Provider",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    val (memberResult, _) = ScioUtils.runJob("polish-cci-transform-gold-member", args) { sc =>
      val member = MemberTransformer(
        fetch[Member](sc, "Member")
      ).transform()(sourceProject)

      Transform.persist(
        member,
        destinationProject,
        // TODO: Confirm plan for combining CCI Member data. Will probably update this destination dataset.
        destinationFinalDataset,
        "Member",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    val (labsResult, _) = ScioUtils.runJob("polish-cci-transform-gold-lab-result", args) { sc =>
      val labResult = LabResultTransformer(fetch[LabResults](sc, "LabResults"))
        .transform()(sourceProject)

      Transform.persist(
        labResult,
        destinationProject,
        destinationFinalDataset,
        "LabResult",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    val (pharmacyResult, _) = ScioUtils.runJob("polish-cci-transform-gold-pharmacy", args) { sc =>
      val pharmacy = PharmacyTransformer(
        fetch[Pharmacy](sc, "Pharmacy")
      ).transform()(sourceProject)

      Transform.persist(
        pharmacy,
        destinationProject,
        destinationIncrementalDataset,
        "Pharmacy",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    List(
      facilityResult,
      professionalResult,
      providerResult,
      memberResult,
      labsResult,
      pharmacyResult
    )
  }
}
