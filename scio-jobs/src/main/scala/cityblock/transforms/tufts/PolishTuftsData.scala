package cityblock.transforms.tufts

import cityblock.models.TuftsSilverClaims.{
  MassHealth,
  Medical,
  SilverMember,
  SilverPharmacy,
  SilverProvider
}
import cityblock.models.gold.FacilityClaim.Facility
import cityblock.models.gold.ProfessionalClaim.Professional
import cityblock.transforms.Transform
import cityblock.transforms.tufts.gold._
import cityblock.utilities.reference.tables.ProviderSpecialtyMappings
import cityblock.utilities.{Environment, Loggable, PartnerConfiguration, ScioUtils}
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

object PolishTuftsData extends Loggable {
  val SUPPORTED_GOLD_TABLES: List[String] =
    List("Facility",
         "Professional",
         "Pharmacy",
         "Provider",
         "Member",
         "MassHealth_Facility",
         "MassHealth_Professional")

  def main(argv: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[GcpOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val project = opts.getProject

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val shard = LocalDate.parse(args.required("deliveryDate"), dtf)

    val sourceProject =
      args.getOrElse("sourceProject", PartnerConfiguration.tufts.productionProject)
    val sourceDataset = args.getOrElse(
      "sourceDataset",
      "silver_claims"
    )
    val destinationProject =
      args.getOrElse("destinationProject", PartnerConfiguration.tufts.productionProject)
    val destinationDataset = args.getOrElse("destinationDataset", "gold_claims")

    val goldTables: List[String] = args.list("goldTables")

    // this flag is primarily useful when testing airflow DAGs
    val checkArgsAndExit = args.boolean("checkArgsAndExit", default = false)

    if (project != "cityblock-data") {
      throw new Error("""
                        |Running PolishTuftsData in a project other than "cityblock-data". This
                        |would likely fail due to permissions errors. Please rerun with
                        |--project=cityblock-data.
        """.stripMargin)
    }

    if (goldTables.exists(t => !SUPPORTED_GOLD_TABLES.contains(t))) {
      throw new Error(
        s"""--goldTables argument must be a comma-separated list of containing one or more of:
           |  ${SUPPORTED_GOLD_TABLES.mkString(", ")}
           |""".stripMargin
      )
    }
    if (goldTables.length == 0) {
      logger.warn(
        s"""--goldTables argument was blank. must be a comma-separated list of containing one or more of:
           |  ${SUPPORTED_GOLD_TABLES.mkString(", ")}
           |""".stripMargin
      )
    }

    if (!checkArgsAndExit) {
      val results =
        polishTuftsClaims(
          argv,
          goldTables,
          sourceDataset,
          sourceProject,
          destinationProject,
          destinationDataset,
          shard
        )

      results.map(_.waitUntilDone())
    }
  }

  def polishTuftsClaims(
    args: Array[String],
    goldTables: List[String],
    sourceDataset: String,
    sourceProject: String,
    destinationProject: String,
    destinationDataset: String,
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

    val (providerResult, _) = ScioUtils.maybeRunJob(
      "polish-tufts-transform-gold-provider",
      args,
      goldTables.contains("Provider")
    ) { sc =>
      val provider = ProviderTransformer(
        fetch[SilverProvider](sc, "Provider"),
        shard
      ).transform()(sourceProject)

      Transform.persist(
        provider,
        destinationProject,
        destinationDataset,
        "Provider",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    val (memberResult, _) = ScioUtils.maybeRunJob(
      "polish-tufts-transform-gold-member",
      args,
      goldTables.contains("Member")
    ) { sc =>
      val member = MemberTransformer(
        fetch[SilverMember](sc, "Member")
      ).transform()(sourceProject)

      Transform.persist(
        member,
        destinationProject,
        destinationDataset,
        "Member",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    val (pharmacyResult, _) = ScioUtils.maybeRunJob(
      "polish-tufts-transform-gold-pharmacy",
      args,
      goldTables.contains("Pharmacy")
    ) { sc =>
      val pharmacy = PharmacyTransformer(
        fetch[SilverPharmacy](sc, "PharmacyClaim")
      ).transform()(sourceProject)

      Transform.persist(
        pharmacy,
        destinationProject,
        destinationDataset,
        "Pharmacy",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    val (_, providerSpecialties) = ScioUtils.maybeRunJob(
      "polish-tufts-fetch-specialties",
      args,
      goldTables.contains("Professional") || goldTables.contains("Facility")
    ) { sc =>
      ProviderSpecialtyMappings.fetchAll(sc).materialize
    }

    val (professionalResult, _) =
      ScioUtils.maybeRunJob(
        "polish-tufts-transform-gold-professional",
        args,
        goldTables.contains("Professional")
      ) { sc =>
        val silverProfessional: SCollection[Medical] = fetch[Medical](sc, "MedicalClaim")
          .groupBy(ClaimKey.apply)
          .filter {
            case (_, claims) => isProfessional(claims)
          }
          .flatMap(_._2)

        val goldProfessional: SCollection[Professional] = {
          ProfessionalTransformer(
            silverProfessional,
            ScioUtils.waitAndOpen(providerSpecialties.get, sc)
          ).transform()(sourceProject)
        }

        Transform.persist(
          goldProfessional,
          destinationProject,
          "gold_claims_incremental",
          "Professional",
          shard,
          writeDisposition,
          createDisposition
        )
      }

    val (facilityResult, _) = ScioUtils.maybeRunJob(
      "polish-tufts-transform-gold-facility",
      args,
      goldTables.contains("Facility")
    ) { sc =>
      val silverFacility: SCollection[Medical] = fetch[Medical](sc, "MedicalClaim")
        .groupBy(ClaimKey.apply)
        .filter {
          case (_, claims) => isFacility(claims)
        }
        .flatMap(_._2)

      val goldFacility: SCollection[Facility] = {
        FacilityTransformer(silverFacility, ScioUtils.waitAndOpen(providerSpecialties.get, sc))
          .transform()(sourceProject)
      }

      Transform.persist(
        goldFacility,
        destinationProject,
        "gold_claims_incremental",
        "Facility",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    val (masshealthFacilityResult, _) = ScioUtils.maybeRunJob(
      "polish-tufts-transform-gold-masshealth-facility",
      args,
      goldTables.contains("MassHealth_Facility")
    ) { sc =>
      val silver: SCollection[MassHealth] = fetch[MassHealth](sc, "MassHealth")

      val gold: SCollection[Facility] = MassHealthFacilityTransformer(silver)
        .transform()(sourceProject)
      Transform.persist(
        gold,
        destinationProject,
        "gold_claims_incremental",
        "MassHealth_Facility",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    val (masshealthProfessionalResult, _) = ScioUtils.maybeRunJob(
      "polish-tufts-transform-gold-masshealth-professional",
      args,
      goldTables.contains("MassHealth_Professional")
    ) { sc =>
      val silver: SCollection[MassHealth] = fetch[MassHealth](sc, "MassHealth")
      val gold: SCollection[Professional] = MassHealthProfessionalTransformer(silver)
        .transform()(sourceProject)
      Transform.persist(
        gold,
        destinationProject,
        "gold_claims_incremental",
        "MassHealth_Professional",
        shard,
        writeDisposition,
        createDisposition
      )
    }

    List(
      providerResult,
      memberResult,
      pharmacyResult,
      professionalResult,
      facilityResult,
      masshealthFacilityResult,
      masshealthProfessionalResult
    ).flatten
  }
}
