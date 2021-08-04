package cityblock.streaming.jobs.aggregations

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.bigquery.types.BigQueryType
import io.circe.Encoder
import io.circe.generic.semiauto.deriveEncoder
import kantan.xpath.NodeDecoder
import kantan.xpath.literals.XPathStringContext

object Medications {
  @BigQueryType.toTable
  case class MedicationDose(
    Quantity: String,
    Units: String
  )

  object MedicationDose {
    implicit val medicationDoseEncoder: Encoder[MedicationDose] =
      deriveEncoder[MedicationDose]
    implicit val medicationDoseDecoder: NodeDecoder[MedicationDose] =
      NodeDecoder.decoder(xp"./@value", xp"./@unit")(MedicationDose.apply)
  }

  @BigQueryType.toTable
  case class MedicationRate(
    Quantity: Option[String],
    Units: Option[String]
  )

  object MedicationRate {
    implicit val medicationRateEncoder: Encoder[MedicationRate] =
      deriveEncoder[MedicationRate]
  }

  @BigQueryType.toTable
  case class MedicationRoute( //TODO: Follow up with Elation to populate code in CCDA
                             Code: String,
                             CodeSystem: String,
                             CodeSystemName: String,
                             Name: String)

  object MedicationRoute {
    implicit val medicationRouteEncoder: Encoder[MedicationRoute] =
      deriveEncoder[MedicationRoute]
  }

  @BigQueryType.toTable
  case class MedicationFrequency(
    Period: String,
    Unit: String
  )

  object MedicationFrequency {
    implicit val medicationFrequencyEncoder: Encoder[MedicationFrequency] =
      deriveEncoder[MedicationFrequency]
    implicit val medicationFrequencyDecoder: NodeDecoder[MedicationFrequency] =
      NodeDecoder.decoder(xp".")((period: String) => MedicationFrequency(period, "Free Text"))
  }

  @BigQueryType.toTable
  case class MedicationProduct(
    Code: String,
    CodeSystem: String,
    CodeSystemName: String,
    Name: String
  )

  object MedicationProduct {
    implicit val medicationProductEncoder: Encoder[MedicationProduct] =
      deriveEncoder[MedicationProduct]
    implicit val medicationProductDecoder: NodeDecoder[MedicationProduct] =
      NodeDecoder.decoder(xp"@code", xp"@codeSystem", xp"@codeSystemName", xp"@displayName")(
        MedicationProduct.apply
      )
  }

  @BigQueryType.toTable
  case class Medication(
    Prescription: Boolean,
    FreeTextSig: Option[String],
    Dose: MedicationDose,
    Rate: MedicationRate,
    Route: MedicationRoute,
    StartDate: Option[DateOrInstant],
    EndDate: Option[DateOrInstant],
    Frequency: MedicationFrequency,
    IsPRN: Option[Boolean],
    Product: MedicationProduct
  )

  object Medication {
    implicit val medicationEncoder: Encoder[Medication] =
      deriveEncoder[Medication]
  }

  @BigQueryType.toTable
  case class PatientMedication(
    messageId: String,
    patient: Patient,
    medication: Medication
  )

  object PatientMedication {
    implicit val patientMedicationEncoder: Encoder[PatientMedication] =
      deriveEncoder[PatientMedication]
  }
}
