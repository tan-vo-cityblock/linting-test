package cityblock.transforms.connecticare

import cityblock.transforms.connecticare.combine.{
  CombineFacility,
  CombinePharmacy,
  CombineProfessional
}
import cityblock.transforms.Transform
import cityblock.transforms.combine.Combine
import cityblock.utilities.{Environment, ScioUtils}
import com.spotify.scio.{Args, ScioContext, ScioResult}
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object CombineCCIClaims {
  def main(argv: Array[String]): Unit = {
    val (_, args): (GcpOptions, Args) = ScioContext.parseArguments[GcpOptions](argv)
    implicit val environment: Environment = Environment(argv)

    val oldShard = args.required("previousDeliveryDate")
    val newShard = args.required("deliveryDate")
    val oldSourceProject = args.required("oldSourceProject")
    val newSourceProject: String = args.required("newSourceProject")
    val oldSourceDataset: String = args.required("oldSourceDataset")
    val newSourceDataset = args.required("newSourceDataset")
    val destinationDataset = args.required("destinationDataset")
    val destinationProject = args.required("destinationProject")

    val checkArgsAndExit = args.boolean("checkArgsAndExit", default = false)

    if (!checkArgsAndExit) {
      val results: List[ScioResult] =
        combineCCIClaims(
          argv,
          oldSourceDataset,
          newSourceDataset,
          oldSourceProject,
          newSourceProject,
          oldShard,
          newShard,
          destinationDataset,
          destinationProject
        )
      results.map(_.waitUntilDone())
    }
  }

  // scalastyle:off parameter.number
  def combineCCIClaims(
    args: Array[String],
    oldSourceDataset: String,
    newSourceDataset: String,
    oldSourceProject: String,
    newSourceProject: String,
    oldShard: String,
    newShard: String,
    destinationDataset: String,
    destinationProject: String
  )(implicit environment: Environment): List[ScioResult] = {
    val writeDisposition = WRITE_TRUNCATE
    val createDisposition = CREATE_IF_NEEDED
    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val oldDate = LocalDate.parse(oldShard, dtf)
    val newDate = LocalDate.parse(newShard, dtf)

    val (facilityResult, _) = ScioUtils.runJob("cci-merge-gold-facility", args) { sc =>
      val oldFacility =
        Combine.fetchFacilityClaimsAndFixAmounts(sc, oldSourceDataset, oldShard, oldSourceProject)
      val newFacility =
        Combine.fetchFacilityClaimsAndFixAmounts(sc, newSourceDataset, newShard, newSourceProject)

      val facility = CombineFacility(
        oldFacility,
        newFacility,
        oldDate,
        newDate
      ).combine()

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

    val (pharmacyResult, _) = ScioUtils.runJob("cci-merge-gold-pharmacy", args) { sc =>
      val oldPharmacy =
        Combine.fetchPharmacyClaimsAndFixAmounts(sc, oldSourceDataset, oldShard, oldSourceProject)
      val newPharmacy =
        Combine.fetchPharmacyClaimsAndFixAmounts(sc, newSourceDataset, newShard, newSourceProject)

      val pharmacy = CombinePharmacy(
        oldPharmacy,
        newPharmacy,
        oldDate,
        newDate
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

    val (professionalResult, _) = ScioUtils.runJob("cci-merge-gold-professional", args) { sc =>
      val combinedProfessional =
        Combine.fetchProfessionalClaimsAndFixAmounts(sc,
                                                     oldSourceDataset,
                                                     oldShard,
                                                     oldSourceProject)
      val newProfessional =
        Combine.fetchProfessionalClaimsAndFixAmounts(sc,
                                                     newSourceDataset,
                                                     newShard,
                                                     newSourceProject)

      val professional = CombineProfessional(
        combinedProfessional,
        newProfessional,
        oldDate,
        newDate
      ).combine()

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

    List(facilityResult, pharmacyResult, professionalResult)
  }
}
