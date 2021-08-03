package cityblock.ehrmodels.elation.datamodelapi.bill

import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter

import cityblock.ehrmodels.elation.datamodelapi.SubscriptionObject
import cityblock.ehrmodels.elation.service.Elation
import cityblock.ehrmodels.elation.service.auth.AuthData
import cityblock.utilities.backend.{ApiError, CirceError}
import com.softwaremill.sttp._
import io.circe.Decoder
import io.circe.generic.semiauto.deriveDecoder

/**
 *
 * A Bill can be in any of the following 4 states:
 * 1. Bill from signed off visit note
 * 2. Bill from unsigned visit note
 * 3. Bill from unsigned visit note but with attribute early_bill set to True
 * 4. Bill from deleted visit note (signed or unsigned)
 * Elation returns bills in (1), (3), and (4). Bills in (2) are not exposed in the API.
 *
 * You can use the visit_note_signed_date and the visit_note_deleted_date to determine if the visit note has been signed or deleted.
 *
 * @param id
 * @param ref_number string(50). required for PATCH that marks bill as processed.
 * @param service_date
 * @param billing_date required for PATCH that marks bill as processed.
 * @param billing_status
 * @param billing_error required for PATCH that marks bill as failed.
 * @param billing_raw_error optional for PATCH that marks bill as failed.
 * @param notes
 * @param cpts
 * @param payment
 * @param visit_note_id
 * @param visit_note_signed_date
 * @param visit_note_deleted_date
 * @param referring_provider
 * @param service_location
 * @param physician
 * @param practice
 * @param patient
 * @param metadata
 * @param created_date
 * @param last_modified_date
 */
case class Bill(
  id: Long,
  ref_number: Option[String],
  service_date: Option[Instant],
  billing_date: Option[Instant],
  billing_status: Option[String],
  billing_error: Option[String],
  billing_raw_error: Option[String],
  notes: Option[String],
  cpts: List[Cpt],
  payment: Option[Payment],
  visit_note_id: Long,
  visit_note_signed_date: Option[Instant],
  visit_note_deleted_date: Option[Instant],
  referring_provider: Option[ReferringProvider],
  service_location: Option[ServiceLocation],
  physician: Long,
  practice: Long,
  patient: Long,
  metadata: Option[Map[String, String]],
  created_date: Option[Instant],
  last_modified_date: Option[Instant]
) extends SubscriptionObject {
  override def getPatientId: Option[String] = Some(this.patient.toString)
}

object Bill {
  implicit val billIso8601Decoder: Decoder[LocalDateTime] =
    io.circe.Decoder
      .decodeLocalDateTimeWithFormatter(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX"))

  implicit val billDecoder: Decoder[Bill] = deriveDecoder[Bill]

  def findAllByPatientId(
    id: String
  )(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, List[Bill]] =
    Elation.getAll[Bill](uri"https://${AuthData.endpoint}/api/2.0/bills/?limit=50&patient=$id")

  def apply(billJson: io.circe.Json): Either[CirceError, Bill] =
    billJson.as[Bill] match {
      case Right(bill) => Right(bill)
      case Left(error) => Left(CirceError(billJson.toString, error))
    }
}
