package cityblock.aggregators.jobs

import cityblock.aggregators.models.ClaimsEncounters.PatientClaimsEncounters
import cityblock.models.gold.Claims.MemberIdentifier
import cityblock.models.gold.FacilityClaim.Facility
import cityblock.models.gold.NewProvider.Provider
import cityblock.models.gold.ProfessionalClaim.Professional
import cityblock.models.gold.TypeOfService
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.transforms.Transform.CodeSet
import cityblock.utilities.GoldClaimsDefaults.Builders._
import cityblock.utilities.reference.tables._
import cityblock.utilities._
import com.spotify.scio.bigquery.BigQueryIO
import com.spotify.scio.io.CustomIO
import com.spotify.scio.testing.PipelineSpec
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate

class PushPatientClaimsEncountersTest extends PipelineSpec {
  implicit val environment: Environment = Environment.Test

  "PushPatientClaimsEncounters" should "produce two full professional encounters" in {
    val project = Environment.Test.projectName
    val sourceProject = PartnerConfiguration.connecticare.productionProject
    val sourceDataset = "gold_claims"
    val deliveryDate = "20190809"
    val source = "ConnectiCare"

    val id =
      MemberIdentifier(Some("commonId"), "partnerId", Some("patientId"), source)

    val professionalProvider =
      GoldClaimsDefaults.provider(Some("professional"), source)

    val professionalDate1 = LocalDate.parse("2019-1-10")
    val professionalDate2 = LocalDate.parse("2019-1-20")

    // This poor person received the same vaccine for the same illness
    // on two different occasions from the same provider. Maybe the vaccine
    // is a two-parter.
    val professional = ProfessionalBuilder
      .mk(id)
      .addLineBuilder(
        ProfessionalLineBuilder.mk
          .addDates(_.copy(from = Some(professionalDate1)))
          .addServicingProvider(professionalProvider.providerIdentifier.id)
          .addProcedure(ProcedureTier.Principal, CodeSet.HCPCS, "G0009")
          .edit(_.copy(typesOfService = List(TypeOfService(1, "10")), placeOfService = Some("20")))
      )
      .addLineBuilder(
        ProfessionalLineBuilder.mk
          .addDates(_.copy(from = Some(professionalDate2)))
          .addServicingProvider(professionalProvider.providerIdentifier.id)
          .addProcedure(ProcedureTier.Principal, CodeSet.HCPCS, "G0009")
          .edit(_.copy(typesOfService = List(TypeOfService(1, "10")), placeOfService = Some("20")))
      )
      .addDiagnosis(DiagnosisTier.Principal, CodeSet.ICD10Cm, "J61")
      .build

    val professionalDx =
      ICD10Cm(code = "J61", name = "Pneumonia related to asbestos")
    val professionalPx =
      HCPCS(code = "G0009",
            summary = Some("Pneumonia vaccine"),
            description = Some("Administration of pneumococcal vaccine"))
    val professionalTOS =
      reference.tables.TypeOfService(code = "10", description = "Doctor's Visit")
    val professionalPOS = PlaceOfService(code = "20", name = "Clinic")

    val facilityProvider =
      GoldClaimsDefaults.provider(Some("organization"), source)

    val date3 = LocalDate.parse("2019-2-5")

    // This person also took an ER trip. I hope they're okay!
    val facility = FacilityBuilder
      .mk(id)
      .addDates(_.copy(admit = Some(date3)))
      .addBillingProvider(facilityProvider.providerIdentifier.id)
      .addDiagnosis(DiagnosisTier.Principal, CodeSet.ICD10Cm, "K20") // TODO real kode
      .addProcedure(ProcedureTier.Principal, CodeSet.CPT, "A0012") // TODO real kode
      .addLineBuilder(FacilityLineBuilder.mk.addTypeOfService(TypeOfService(1, "34")))
      .build

    val facilityDx = ICD10Cm(code = "K20", name = "a diagnosis")
    val facilityPx = HCPCS(code = "A0012",
                           summary = Some("a procedure"),
                           description = Some("A longer description of a procedure"))
    val facilityTOS = reference.tables.TypeOfService(code = "34", description = "Emergency care")
    val facilityPOS = PlaceOfService(code = "45", name = "Emergency room")

    JobTest[PushPatientClaimsEncounters.type]
      .args(s"--project=$project",
            s"--sourceProject=$sourceProject",
            s"--sourceDataset=$sourceDataset",
            s"--environment=test",
            s"--deliveryDate=$deliveryDate")
      .input(
        BigQueryIO[Provider](
          PushPatientClaimsEncounters.providerQuery(sourceProject, sourceDataset, deliveryDate)),
        Seq(professionalProvider, facilityProvider))
      .input(BigQueryIO[ICD10Cm](ICD10Cm.queryAll), Seq(professionalDx, facilityDx))
      .input(BigQueryIO[HCPCS](HCPCS.queryAll), Seq(professionalPx, facilityPx))
      .input(BigQueryIO[reference.tables.TypeOfService](reference.tables.TypeOfService.queryAll),
             Seq(professionalTOS, facilityTOS))
      .input(BigQueryIO[PlaceOfService](PlaceOfService.queryAll), Seq(professionalPOS, facilityPOS))
      .input(BigQueryIO[Professional](
               PushPatientClaimsEncounters.professionalQuery(sourceProject,
                                                             sourceDataset,
                                                             deliveryDate)),
             Seq(professional))
      .input(
        BigQueryIO[Facility](
          PushPatientClaimsEncounters.facilityQuery(sourceProject, sourceDataset, deliveryDate)),
        Seq(facility))
      .output(CustomIO[PatientClaimsEncounters](PatientClaimsEncounters.gcsCustomIOId))(checkOutput)
      .run()

    def checkOutput(xs: SCollection[PatientClaimsEncounters]): Unit = {
      // TODO make this better

      Should.satisfySingle(xs)(x => id.patientId.contains(x.patientId))
      Should.satisfySingle(xs)(_.encounters.length == 3)
    }
  }
}
