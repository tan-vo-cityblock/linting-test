package cityblock.transforms.elation

import cityblock.ehrmodels.elation.datamodelapi.lab.{Grid, Lab}
import cityblock.member.service.models.PatientInfo.Patient
import cityblock.streaming.jobs.ElationSubscriber.pullElationPhysician
import cityblock.streaming.jobs.orders.Orders._
import cityblock.utilities.time.DateOrInstant
import cityblock.utilities.{Conversions, Loggable}
import com.softwaremill.sttp.{Id, SttpBackend}

object LabTransforms extends Loggable {
  def transformLabToPatientOrders(
    lab: Lab,
    patient: Patient,
    messageId: String
  )(implicit backend: SttpBackend[Id, Nothing]): PatientOrder = {
    val order_id = lab.id.toString

    val transactionDateTime = Some(Conversions.mkDateOrInstant(lab.created_date))
    val collectionDateTime = Some(Conversions.mkDateOrInstant(lab.document_date))
    val completionDateTime =
      for (signedDate <- lab.signed_date)
        yield Conversions.mkDateOrInstant(signedDate)

    val status: Option[String] = lab.grids.headOption.map(_.status)

    val provider =
      pullElationPhysician(lab.physician) match {
        case Some(phys) =>
          Some(
            Provider(
              NPI = phys.npi,
              ID = Some(phys.id.toString),
              IDType = Some("ElationId"),
              FirstName = Some(phys.first_name),
              LastName = Some(phys.last_name),
              Credentials = List(),
              Address = None,
              Location = None,
              PhoneNumber = None
            )
          )
        case None =>
          logger.error(s"Unable to find physician with ID ${lab.physician} in map")
          None
      }

    val results = mkResults(lab.grids, completionDateTime)

    val order = Order(
      ID = order_id,
      ApplicationOrderID = None,
      TransactionDateTime = transactionDateTime,
      CollectionDateTime = collectionDateTime,
      CompletionDateTime = completionDateTime,
      Notes = List[String](),
      ResultsStatus = None,
      Procedure = None,
      Provider = provider,
      ResultCopyProviders = List[Provider](),
      Status = status.getOrElse(""),
      ResponseFlag = None,
      Priority = None,
      Results = results
    )

    PatientOrder(messageId, patient, order)
  }

  private def mkResults(
    grids: List[Grid],
    completionDateTime: Option[DateOrInstant]
  ): List[Result] =
    for {
      grid <- grids
      elationResult <- grid.results
    } yield {
      val notes = Conversions.removeEmptyString(elationResult.note) match {
        case Some(note) => List[String](note)
        case _          => List[String]()
      }

      val resultReferenceRange = ResultReferenceRange(
        Some(elationResult.reference_min),
        Some(elationResult.reference_max),
        Conversions.removeEmptyString(elationResult.text)
      )

      Result(
        Code = elationResult.test.loinc,
        Codeset = None,
        Description = Some(elationResult.test.name),
        Specimen = None,
        Value = elationResult.value,
        ValueType = elationResult.value_type.getOrElse(""),
        CompletionDateTime = completionDateTime,
        FileType = None,
        Units = elationResult.units,
        Notes = notes,
        AbnormalFlag = Conversions.removeEmptyString(elationResult.abnormal_flag),
        Status = elationResult.status.getOrElse(""),
        Producer = None,
        Performer = None,
        ReferenceRange = Some(resultReferenceRange),
        ObservationMethod = None
      )
    }
}
