package cityblock.streaming.jobs

import cityblock.ehrmodels.redox.datamodel.encounter.Encounter
import com.spotify.scio.values.SCollection
import io.circe._
import io.circe.generic.auto._
import io.circe.optics.JsonPath._
import io.circe.parser._
import cityblock.streaming.jobs.RedoxSubscriber.RedoxMessage
import cityblock.streaming.jobs.admin.Admin._
import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.streaming.jobs.aggregations.Medications._
import cityblock.streaming.jobs.aggregations.Problems._
import cityblock.streaming.jobs.aggregations.Results
import cityblock.streaming.jobs.aggregations.VitalSigns._
import cityblock.streaming.jobs.orders.Orders._
import cityblock.streaming.jobs.hie.Events.PatientHieEvent
import cityblock.streaming.jobs.hie.HieMessagesHandler.{ParsedAdminMessage, ParsedNoteMessage}
import cityblock.streaming.jobs.notes.Notes.RawNote
import cityblock.transforms.redox.EncountersTransforms
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.{CREATE_IF_NEEDED, WRITE_APPEND}
import org.joda.time.{DateTimeZone, Instant, LocalDate}
import org.joda.time.format.DateTimeFormat
import cityblock.utilities.{Environment, Loggable}
import com.spotify.scio.io.Tap
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition

import scala.concurrent.Future
import scala.util.{Failure, Success, Try}
import com.spotify.scio.bigquery._
import com.spotify.scio.coders.Coder

package object common {
  // from https://cloud.google.com/bigquery/docs/reference/standard-sql/migrating-from-legacy-sql#timestamp_differences
  final val STANDARD_SQL_MIN_INSTANT = "0001-01-01T00:00:00.000Z"
  final val STANDARD_SQL_MAX_INSTANT = "9999-12-31T23:59:59.999Z"

  // TODO the following hardcoded time zone is expected to break when we operations are no longer on the East Coast of the US
  final val DATE_INGESTER = (pattern: String) =>
    DateTimeFormat
      .forPattern(pattern)
      .withZone(DateTimeZone.forID("America/New_York"))

  case class ParsedPatientMedicalDataLists(
    patientProblemLists: SCollection[List[PatientProblem]],
    patientMedicationLists: SCollection[List[PatientMedication]],
    patientEncounterLists: SCollection[List[PatientEncounter]],
    patientVitalSignLists: SCollection[List[PatientVitalSign]],
    patientResultLists: Option[SCollection[List[Results.PatientResult]]]
  )

  case class ParsedMessagePayload(cursor: HCursor, result: Json, messageId: String)

  //noinspection ScalaStyle
  trait ParsableData extends Serializable with Loggable {
    var project: Option[String] = None

    private def standardSQLValidateDateOrInstant(
      doi: Option[DateOrInstant]
    ): Option[DateOrInstant] = {
      def validateInstant(instant: Instant): Boolean = {
        lazy val minMillis: Long = new Instant(STANDARD_SQL_MIN_INSTANT).getMillis
        lazy val maxMillis: Long = new Instant(STANDARD_SQL_MAX_INSTANT).getMillis
        instant.getMillis >= minMillis && instant.getMillis <= maxMillis
      }

      doi match {
        case None => None
        case Some(DateOrInstant(_, Some(instant), _)) =>
          if (validateInstant(instant)) doi else None
        case _ => doi
      }
    }

    protected def parseDateOrInstant(raw: Option[String]): Option[DateOrInstant] = {
      val doi = raw match {
        case None     => None
        case Some("") => None
        case Some(string) =>
          val longMonthRegex = raw"(?i)([a-z ]+[0-9 ]+:[0-9 ]+[pa]m)".r
          val dateTimeRegex =
            raw"(\d{4})-(\d{2})-(\d{2}) (\d{2}):(\d{2}):(\d{2})".r
          val dateRegex = raw"(\d{4})-(\d{2})-(\d{2})".r
          string match {
            case longMonthRegex(_*) =>
              val dateFormatTemplate = "MMMM d yyyy hh:mma"
              val canonicalizedInstant = Some(
                Instant.parse(string, DATE_INGESTER(dateFormatTemplate))
              )
              val canonicalizedDateTime = Some(
                LocalDate.parse(string, DATE_INGESTER(dateFormatTemplate))
              )
              Some(DateOrInstant(string, canonicalizedInstant, canonicalizedDateTime))
            case dateTimeRegex(_*) =>
              val dateFormatTemplate = "yyyy-MM-dd HH:mm:ss"
              val canonicalizedInstant = Some(
                Instant.parse(string, DATE_INGESTER(dateFormatTemplate))
              )
              val canonicalizedDateTime = Some(
                LocalDate.parse(string, DATE_INGESTER(dateFormatTemplate))
              )
              Some(DateOrInstant(string, canonicalizedInstant, canonicalizedDateTime))
            case dateRegex(_*) =>
              Some(DateOrInstant(string, None, Some(LocalDate.parse(string))))
            case _ =>
              Try(Instant.parse(string)) match {
                case Success(parsedTimestamp) =>
                  Some(DateOrInstant(string, Some(parsedTimestamp), None))
                case Failure(f) =>
                  logger.error(f.toString)
                  None
              }
          }
      }
      standardSQLValidateDateOrInstant(doi)
    }

    implicit val timestampDecoder: Decoder[Option[DateOrInstant]] =
      Decoder
        .decodeOption[String]
        .map {
          parseDateOrInstant
        }

    private def parseVitalSigns(
      parsedJson: io.circe.Json
    ): Either[Nothing, List[VitalSign]] = {
      val observations: List[io.circe.Json] =
        root.VitalSigns.each.Observations.arr.getAll(parsedJson).flatten
      val rightObservations: List[VitalSign] =
        observations.map(o => o.as[VitalSign]).flatMap {
          case Right(item) => Seq(item)
          case Left(decodingFailure) =>
            logger.error(decodingFailure.toString())
            Seq()
        }
      Right(rightObservations)
    }

    def parseEncountersList(
      rows: SCollection[RedoxMessage],
      isStreaming: Boolean
    )(implicit environment: Environment): SCollection[List[PatientEncounter]] =
      rows
        .withName(s"Parse Encounter data")
        .map { message =>
          val payloadJson = parse(message.payload).getOrElse(Json.Null)

          val messageIdJsonPath = root.Meta.Message.ID.long
          val messageId = messageIdJsonPath
            .getOption(payloadJson)
            .fold(
              throw new Exception("Could not parse Redox Message Id")
            )(_.toString)

          val encountersJsonPath = root.Encounters.json
          val encountersJson =
            encountersJsonPath.getOption(payloadJson).getOrElse(Json.Null)

          val insertedAt = Instant.now
          val redoxEncounters = Encounter(encountersJson)
            .fold(error => throw new Exception(s"Could not parse Redox Encounter $error"), identity)
          val transformedEncounters = redoxEncounters.map(
            redoxEncounter =>
              EncountersTransforms
                .transformRedoxEncounterToPatientEncounter(
                  messageId,
                  insertedAt,
                  isStreaming,
                  message.patient,
                  redoxEncounter
              )
          )
          transformedEncounters
        }

    def parseList[T: Coder](rows: SCollection[RedoxMessage], key: String): SCollection[List[T]] = {
      /*NOTE: we creating several case classes from Strings. As of such, the tuple thus holds an object of Type `Product with Serializable with BigQueryType.HasAnnotation`
              As of now, we are defaulting to using the old KyroCoder. Our goal should be to create a CustomCoder or handle the way class instantiation is done.
              Possibly using Tuples or Hlists
       */

      type SuperClass = Product with Serializable with BigQueryType.HasAnnotation

      implicit def coderSuperClass: Coder[SuperClass] =
        Coder.kryo[SuperClass]

      rows
        .withName(s"Parse data from Streaming Messages ($key)")
        .map(message => {
          val parsedMessagePayload = parseMessagePayload(message.payload)
          val cursor = parsedMessagePayload.cursor
          val messageId = parsedMessagePayload.messageId
          val listItems = key match {
            case "Problems"    => cursor.downField(key).as[List[Problem]]
            case "Medications" => cursor.downField(key).as[List[Medication]]
            case "Orders"      => cursor.downField(key).as[List[Order]]
            case "VitalSigns"  => parseVitalSigns(parsedMessagePayload.result)
            case "Results"     => cursor.downField(key).as[List[Results.Result]]
            case _ =>
              throw new Exception("Attempting to parse an invalid list type")
          }

          listItems match {
            case Right(items) =>
              items
                .map(item => (message.patient, item, messageId))
            case Left(decodingError) =>
              logger.error(decodingError.toString())
              List()
          }
        })
        .map(msg => {
          msg
            .map(listItem => {
              val patient = listItem._1
              val messageId = listItem._3

              key match {
                case "Problems" =>
                  val item = listItem._2.asInstanceOf[Problem]
                  PatientProblem(messageId, patient, item).asInstanceOf[T]
                case "Medications" =>
                  val item = listItem._2.asInstanceOf[Medication]
                  PatientMedication(messageId, patient, item).asInstanceOf[T]
                case "Orders" =>
                  val item = listItem._2.asInstanceOf[Order]
                  PatientOrder(messageId, patient, item).asInstanceOf[T]
                case "VitalSigns" =>
                  val item = listItem._2.asInstanceOf[VitalSign]
                  PatientVitalSign(messageId, patient, item).asInstanceOf[T]
                case "Results" =>
                  val item = listItem._2.asInstanceOf[Results.Result]
                  Results.PatientResult(messageId, patient, item).asInstanceOf[T]
              }
            })
        })
    }

    def parseMessagePayload(jsonString: String): ParsedMessagePayload = {
      val parsedJson = parse(jsonString).getOrElse(Json.Null)
      val cursor: HCursor = parsedJson.hcursor
      val parsedMessageId =
        cursor.downField("Meta").downField("Message").get[Long]("ID")
      val messageId = parsedMessageId match {
        case Right(id) => id.toString
        case Left(_)   => throw new Exception("Could not parse MessageId")
      }

      ParsedMessagePayload(cursor, parsedJson, messageId)
    }

    def parsePatientMedicalData(
      redoxMessages: SCollection[RedoxMessage],
      isStreaming: Boolean
    )(implicit environment: Environment): ParsedPatientMedicalDataLists = {
      val problemLists = parseList[PatientProblem](redoxMessages, "Problems")
      val medicationLists =
        parseList[PatientMedication](redoxMessages, "Medications")
      val encounterLists =
        parseEncountersList(redoxMessages, isStreaming)
      val vitalSignLists =
        parseList[PatientVitalSign](redoxMessages, "VitalSigns")
      val resultLists =
        parseList[Results.PatientResult](redoxMessages, "Results")

      ParsedPatientMedicalDataLists(
        problemLists,
        medicationLists,
        encounterLists,
        vitalSignLists,
        Some(resultLists)
      )
    }

    def filterAndParsePatientOrders(
      unfilteredMessages: SCollection[RedoxMessage]
    ): SCollection[List[PatientOrder]] = {
      val filteredMessages = unfilteredMessages
        .filter(message => message.eventType == "Results.New")
      parseList[PatientOrder](filteredMessages, "Orders")
    }

    def savePatientHieEventsToBigQuery(
      patientHieEvents: SCollection[PatientHieEvent],
      writeDisposition: WriteDisposition = WRITE_APPEND
    )(implicit environment: Environment): Future[Tap[PatientHieEvent]] =
      patientHieEvents.saveAsTypedBigQuery(
        s"${environment.projectName}:medical.patient_hie_events",
        writeDisposition,
        CREATE_IF_NEEDED
      )

    def parsePatientNotesMessages(
      messages: SCollection[RedoxMessage]
    ): SCollection[PatientHieEvent] =
      messages
        .flatMap(message => {
          val parsedMessagePayload = parseMessagePayload(message.payload)
          val cursor = parsedMessagePayload.cursor
          val messageId = parsedMessagePayload.messageId

          cursor.as[RawNote] match {
            case Right(parsedRawNote) =>
              Seq(ParsedNoteMessage(messageId, message, parsedRawNote))

            case Left(decodingError) =>
              logger.error(decodingError.toString())
              Seq()
          }
        })
        .flatMap(message => Seq(PatientHieEvent.fromParsedNoteMessage(message)))

    def parsePatientAdminMessages(
      messages: SCollection[RedoxMessage]
    ): SCollection[PatientHieEvent] =
      messages
        .flatMap(message => {
          val parsedMessagePayload = parseMessagePayload(message.payload)
          val cursor = parsedMessagePayload.cursor
          val messageId = parsedMessagePayload.messageId

          cursor.as[RawAdminMessage] match {
            case Right(parsedRawAdminMessage) =>
              Seq(ParsedAdminMessage(messageId, message, parsedRawAdminMessage))
            case Left(decodingError) =>
              logger.error(decodingError.toString())
              Seq()
          }
        })
        .flatMap(message => Seq(PatientHieEvent.fromParsedPatientAdminMessage(message)))
  }
}
