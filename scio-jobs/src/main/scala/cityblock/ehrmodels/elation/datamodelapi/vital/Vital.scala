package cityblock.ehrmodels.elation.datamodelapi.vital

import java.time.{Instant, LocalDateTime}
import java.time.format.DateTimeFormatter

import cityblock.ehrmodels.elation.datamodelapi.SubscriptionObject
import cityblock.ehrmodels.elation.service.Elation
import cityblock.ehrmodels.elation.service.auth.AuthData
import cityblock.utilities.backend.{ApiError, CirceError}
import com.softwaremill.sttp.{Id, SttpBackend}
import com.softwaremill.sttp._
import io.circe.Decoder
import io.circe.generic.JsonCodec
import io.circe.generic.semiauto.deriveDecoder

@JsonCodec
case class VitalContent(value: String, units: Option[String], note: String, label: Option[String])
@JsonCodec
case class VitalContentBP(systolic: String, diastolic: String, note: String)

/**
 * Vitals should be processed only when the note is signed. Although the information in this datamodel consists
 * of very little data that could be preserved as a draft, for consistency reasons, Vitals must be signed and completed before processing.
 */
case class Vital(
  id: Long,
  bmi: Option[Double],
  height: List[VitalContent],
  weight: List[VitalContent],
  oxygen: List[VitalContent],
  rr: List[VitalContent],
  hr: List[VitalContent],
  hc: List[VitalContent],
  temperature: List[VitalContent],
  bp: List[VitalContentBP],
  ketone: Option[List[VitalContent]],
  bodyfat: Option[List[VitalContent]],
  dlm: Option[List[VitalContent]],
  bfm: Option[List[VitalContent]],
  wc: Option[List[VitalContent]],
  patient: Long,
  practice: Long,
  visit_note: Option[Long],
  non_visit_note: Option[Long],
  document_date: Instant,
  chart_date: Instant,
  signed_date: Option[Instant],
  signed_by: Option[Long],
  created_date: Instant,
  deleted_date: Option[Instant]
) extends SubscriptionObject {
  override def getPatientId: Option[String] = Some(this.patient.toString)

  def isSigned(): Boolean =
    signed_by.nonEmpty && signed_date.nonEmpty
}

object Vital {
  implicit val vitaliso8601Decoder: Decoder[LocalDateTime] =
    io.circe.Decoder
      .decodeLocalDateTimeWithFormatter(DateTimeFormatter.ofPattern("uuuu-MM-dd'T'HH:mm:ssX"))

  implicit val vitalDecoder: Decoder[Vital] = deriveDecoder[Vital]

  def findAllByPatientId(
    id: String
  )(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, List[Vital]] =
    Elation.getAll[Vital](
      uri"https://${AuthData.endpoint}/api/2.0/vitals/?limit=50&patient=$id"
    )

  def apply(vitalJson: io.circe.Json): Either[CirceError, Vital] =
    vitalJson.as[Vital] match {
      case Right(elationVital) => Right(elationVital)
      case Left(error)         => Left(CirceError(vitalJson.toString, error))
    }
}
