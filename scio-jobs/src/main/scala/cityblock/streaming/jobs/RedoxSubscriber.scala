package cityblock.streaming.jobs

import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.types.BigQueryType
import cityblock.member.service.io.MemberService
import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.streaming.jobs.scheduling.Event.ParsedEvent
import com.spotify.scio.bigquery.{CREATE_IF_NEEDED, WRITE_APPEND}
import io.circe._
import io.circe.parser._
import io.circe.syntax._
import io.circe.generic.auto._
import com.spotify.scio.values.SCollection
import cityblock.streaming.jobs.common.ParsableData
import cityblock.streaming.jobs.hie.HieMessagesHandler
import cityblock.utilities.{EhrMedicalDataIO, Environment, ResultSigner, SubscriberHelper}
import org.joda.time.Instant
import com.spotify.scio.bigquery._
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions

object RedoxSubscriber extends EhrMedicalDataIO with ParsableData with SubscriberHelper {
  val datasource = "acpny"

  @BigQueryType.toTable
  case class RedoxMessage(patient: Patient, eventType: String, payload: String, insertedAt: Instant)
  case class MessageWithPatient(eventType: String,
                                messageId: String,
                                patientId: String,
                                payload: String)

  case class PatientIdentifiers(patientId: Option[String], externalId: PatientExternalIdentifier)
  case class PatientExternalIdentifier(id: String, datasource: String)
  case class MessageSource(id: String, name: String)

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)
    implicit val environment: Environment = Environment(cmdlineArgs)
    val sc = ScioContext(opts)
    val project = Some(opts.getProject)

    val subscription: String = args.required("subscription")
    val schedulingTopicName: String = args.required("schedulingTopic")
    val hieTopic: String = args.required("hieTopic")
    val medicalDataset: String = args.required("medicalDataset")
    val redoxMessagesTable: String = args.required("redoxMessagesTable")
    val rawHieWithPatientTopic: String = args.required("rawHieWithPatientTopic")

    val redoxGcpTableName: String = s"streaming.$redoxMessagesTable"

    val messagesWithPatients = processMessages(sc, subscription)

    messagesWithPatients.saveAsTypedBigQuery(redoxGcpTableName, WRITE_APPEND, CREATE_IF_NEEDED)
    handleSchedulingMessages(messagesWithPatients, schedulingTopicName)
    handleResultsMessages(messagesWithPatients, medicalDataset)
    handleNotesMessages(messagesWithPatients, hieTopic)
    handlePatientAdminMessages(messagesWithPatients, hieTopic)
    publishMessageWithPatients(messagesWithPatients, rawHieWithPatientTopic)

    sc.close().waitUntilFinish()
  }

  def publishMessageWithPatients(unfilteredMessages: SCollection[RedoxMessage], topicName: String)(
    implicit environment: Environment): Unit = {
    val hieEventTopic =
      s"projects/${environment.projectName}/topics/$topicName"
    unfilteredMessages
      .flatMap(message => {
        message.patient.patientId match {
          case Some(patientId) =>
            val parsedMessagePayload = parseMessagePayload(message.payload)
            val messageId = parsedMessagePayload.messageId
            val event = MessageWithPatient(
              eventType = message.eventType,
              messageId = messageId,
              patientId = patientId,
              payload = message.payload
            )
            val eventPayload = event.asJson.noSpaces
            Seq((eventPayload, Map("topic" -> topicName)))
          case None => Seq()
        }
      })
      .withName("publishMessagesWithPatients")
      .saveAsPubsubWithAttributes[String](hieEventTopic)
  }

  def processMessages(sc: ScioContext, subscription: String)(
    implicit environment: Environment
  ): SCollection[RedoxMessage] =
    sc.pubsubSubscriptionWithAttributes[String](subscription)
      .flatMap {
        case (msg, attributes) =>
          logger.info(s"received message $msg")

          val eventType = attributes.getOrElse("type", "Unknown")

          val patient: Option[Patient] = patientForMsg(msg)

          patient.map(patient => RedoxMessage(patient, eventType, msg, Instant.now))
      }

  def getPatientIdentifiersFromPayload(payload: String): PatientIdentifiers = {
    val parsedPayload = parse(payload).getOrElse(Json.Null)
    val cursor: HCursor = parsedPayload.hcursor
    val patientIdentifiers =
      cursor.downField("Patient").get[List[Map[String, String]]]("Identifiers")

    patientIdentifiers match {
      case Right(identifiers: List[Map[String, String]]) =>
        val patientIdIdentifier = identifiers.find(identifier => {
          identifier.contains("IDType") && List("Cityblock MRN", "ctyblk").contains(
            identifier("IDType")
          )
        })
        val patientId = patientIdIdentifier match {
          case Some(identifier) => identifier.get("ID")
          case None             => None
        }

        val patientExternalIdentifier = identifiers.find(identifier => {
          identifier.contains("IDType") && List("EPI", "ENS_CTYBLKDC").contains(
            identifier("IDType")
          )
        })
        val patientExternalId = patientExternalIdentifier.fold[PatientExternalIdentifier](
          PatientExternalIdentifier("Unknown", datasource)
        ) { identifier =>
          val externalId = identifier.getOrElse("ID", "Unknown")
          val externalDatasource = identifier
            .get("IDType")
            .map {
              case "EPI"          => datasource
              case "ENS_CTYBLKDC" => "carefirst"
              case _              => datasource
            }
            .getOrElse(datasource)
          PatientExternalIdentifier(externalId, externalDatasource)
        }

        PatientIdentifiers(patientId, patientExternalId)
      case Left(_) =>
        PatientIdentifiers(None, PatientExternalIdentifier("Unknown", datasource))
    }
  }

  def getMessageSourceFromPayload(payload: String): MessageSource = {
    val parsedPayload = parse(payload).getOrElse(Json.Null)
    val cursor: HCursor = parsedPayload.hcursor
    val messageSource =
      cursor.downField("Meta").get[Map[String, String]]("Source")

    messageSource match {
      case Right(source: Map[String, String]) =>
        val id = source.getOrElse("ID", "Unknown")
        val name = source.getOrElse("Name", "Unknown")

        MessageSource(id, name)
      case Left(_) =>
        MessageSource("Unknown", "Unknown")
    }
  }

  def handleSchedulingMessages(
    unfilteredMessages: SCollection[RedoxMessage],
    schedulingTopicName: String
  )(implicit environment: Environment): Unit = {
    val schedulingTopic =
      s"projects/${environment.projectName}/topics/$schedulingTopicName"

    val filteredMessages = unfilteredMessages
      .filter(message => {
        val schedulingPattern = "(Scheduling.*)".r

        message.eventType match {
          case schedulingPattern(message.eventType) => true
          case _                                    => false
        }
      })

    processSchedulingMessages(filteredMessages)
      .withName("republishSchedulingMessages")
      .saveAsPubsubWithAttributes[String](schedulingTopic)
  }

  def processSchedulingMessages(
    schedulingMessages: SCollection[RedoxMessage]
  ): SCollection[(String, Map[String, String])] =
    schedulingMessages
      .flatMap(message => {
        decodeSchedulingEvent(message.payload) match {
          case Right(decodedEvent) =>
            val eventJsonObject = decodedEvent.asJsonObject

            message.patient.patientId match {
              case Some(patientId) =>
                val eventJsonObjectWithPatientId =
                  eventJsonObject.add("patientId", patientId.asJson)
                val messageJson = eventJsonObjectWithPatientId.asJson.noSpaces
                val hmac = ResultSigner.signResult(messageJson)
                Seq(
                  (
                    messageJson,
                    Map(
                      "type" -> message.eventType,
                      "schedulingEventType" -> decodedEvent.eventType,
                      "topic" -> "scheduling",
                      "hmac" -> hmac
                    )
                  )
                )
              case _ =>
                Seq()
            }
          case Left(error) =>
            throw new Exception(
              "Error decoding scheduling payload: " + message.payload,
              error
            )
          case _ =>
            throw new Exception(
              "Uknown error decoding scheduling payload: " + message.payload
            )
        }
      })

  def decodeSchedulingEvent(payload: String): Either[Error, ParsedEvent] =
    decode[ParsedEvent](payload)

  def handleNotesMessages(unfilteredMessages: SCollection[RedoxMessage], hieTopicName: String)(
    implicit environment: Environment
  ): Unit = {
    val messagesNeedingProcessing =
      unfilteredMessages.filter(message => message.eventType == "Notes.New")

    HieMessagesHandler.processAndPersistNotesMessages(
      messagesNeedingProcessing,
      hieTopicName
    )
  }

  def handlePatientAdminMessages(
    unfilteredMessages: SCollection[RedoxMessage],
    hieTopicName: String
  )(implicit environment: Environment): Unit = {
    val messagesNeedingProcessing =
      unfilteredMessages
      // https://cityblock.kanbanize.com/ctrl_board/2/cards/2589/details
        .filter(
          message =>
            message.eventType == "PatientAdmin.Discharge" || message.eventType == "PatientAdmin.Arrival"
        )
        // https://cityblock.kanbanize.com/ctrl_board/2/cards/2588/details
        .filter(message => {
          val messageSource = getMessageSourceFromPayload(message.payload)
          messageSource.name == "PatientPing Source (p)" ||
          messageSource.name == "PatientPing Source (s)" ||
          messageSource.name == "CRISP [PROD] ADT Source (p)"
        })

    HieMessagesHandler.processAndPersistPatientAdminMessages(
      messagesNeedingProcessing,
      hieTopicName
    )
  }

  private def handleResultsMessages(
    unfilteredMessages: SCollection[RedoxMessage],
    medicalDataset: String
  )(implicit environment: Environment): Unit = {
    val patientOrders = filterAndParsePatientOrders(unfilteredMessages)
    persistPatientMedicalData(
      patientOrders,
      s"$medicalDataset.patient_orders",
      WRITE_APPEND
    )
  }

  private def patientForMsg(msg: String)(implicit environment: Environment): Option[Patient] = {
    val patientIdentifiers = getPatientIdentifiersFromPayload(msg)
    val externalIdentifier = patientIdentifiers.externalId

    patientIdentifiers.patientId match {
      case Some(id) => Some(getPatientFromMessageWithPatientId(msg, id))
      case _ if externalIdentifier.datasource == "carefirst" =>
        MemberService.getInsurancePatients(
          None,
          Option(externalIdentifier.id),
          Option(externalIdentifier.datasource)
        ) match {
          case Right(patients) => patients.headOption
          case Left(apiError) =>
            throw new Exception(
              s"Failed to find insurance [error: ${apiError.toString}]"
            )
        }
      case _ =>
        Some(
          getPatientByExternalId(
            externalId = externalIdentifier.id,
            carrier = externalIdentifier.datasource,
            logger = logger
          )
        )
    }
  }

  private def getPatientFromMessageWithPatientId(msg: String, memberId: String)(
    implicit environment: Environment
  ): Patient = {
    val messageSource = getMessageSourceFromPayload(msg)
    MemberService.getMemberMrn(memberId) match {
      case Right(_) =>
        logger.info(
          s"Found member with ID $memberId in Member Service (${environment.name})"
        )
        Patient(Some(memberId), memberId, Datasource(messageSource.name))
      case Left(error) =>
        logger.error(
          s"Got error querying for a member with ID $memberId in Member Service (${environment.name}): $error"
        )
        Patient(None, memberId, Datasource(messageSource.name))
    }
  }
}
