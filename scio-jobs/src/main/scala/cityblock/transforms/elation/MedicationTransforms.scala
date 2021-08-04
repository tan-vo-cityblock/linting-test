package cityblock.transforms.elation

import cityblock.ehrmodels.elation.datamodelapi.ccda.Ccda
import cityblock.member.service.models.PatientInfo.Patient
import cityblock.streaming.jobs.aggregations.Medications._
import cityblock.utilities.Loggable
import cityblock.utilities.time.DateOrInstant
import kantan.xpath.Query
import kantan.xpath.NodeDecoder
import kantan.xpath.implicits._

object MedicationTransforms extends Loggable {
  type CcdaMedsData = (
    Option[MedicationDose],
    Option[DateOrInstant],
    Option[DateOrInstant],
    Option[MedicationFrequency],
    MedicationProduct
  )

  def transformElationCcdaToPatientMedications(
    messageId: String,
    patient: Patient,
    elationCcda: Ccda
  ): List[PatientMedication] = {
    val ccdaMedications = getCcdaMedsData(messageId, patient, elationCcda)

    for (ccdaMedsData <- ccdaMedications)
      yield PatientMedication(messageId, patient, ccdaMedToMedication(ccdaMedsData))
  }

  private def ccdaMedToMedication(medsData: CcdaMedsData): Medication = {
    val unknown = "Unknown"

    val (medDose, startDate, endDate, medFreq, medProd) = medsData

    Medication(
      Prescription = false,
      FreeTextSig = None,
      Dose = medDose.getOrElse(MedicationDose("Unknown", "Unknown")),
      Rate = MedicationRate(None, None),
      Route = MedicationRoute(unknown, unknown, unknown, unknown),
      StartDate = startDate,
      EndDate = endDate,
      Frequency = medFreq.getOrElse(MedicationFrequency(unknown, unknown)),
      IsPRN = None,
      Product = medProd
    )
  }

  private def getCcdaMedsData(
    messageId: String,
    patient: Patient,
    ccda: Ccda
  ): List[CcdaMedsData] = {
    implicit val medicationDecoder: NodeDecoder[CcdaMedsData] =
      NodeDecoder.tuple(
        xp"./doseQuantity[@value and @unit]",
        xp"./effectiveTime/low[@value]",
        xp"./effectiveTime/high[@value]",
        xp"./text",
        xp".//manufacturedMaterial/code"
      )

    val medQuery =
      Query.compile[List[CcdaMedsData]](Ccda.getMedsQueryStr) match {
        case Left(error) =>
          throw new IllegalArgumentException(
            s"Bad XPath Query String: ${Ccda.getMedsQueryStr}. Compile Error: $error\nMessageId: $messageId. ExternalId: ${patient.externalId}"
          )
        case Right(query) => query
      }

    ccda.ccdaXml
      .evalXPath(medQuery) match {
      case Right(meds) => meds
      case Left(err) =>
        logger.error(
          s"Error finding medication section/nodes. ReadError: $err\nMessageId: $messageId. ExternalId: ${patient.externalId}"
        )
        List.empty[CcdaMedsData]
    }
  }
}
