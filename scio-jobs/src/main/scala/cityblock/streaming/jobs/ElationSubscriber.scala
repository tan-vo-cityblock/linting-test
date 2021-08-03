package cityblock.streaming.jobs

import cityblock.ehrmodels.elation.datamodelapi.SubscriptionObject
import cityblock.ehrmodels.elation.datamodelapi.appointment.Appointment
import cityblock.ehrmodels.elation.datamodelapi.ccda.Ccda
import cityblock.ehrmodels.elation.datamodelapi.lab.Lab
import cityblock.utilities.backend.SttpHelper
import cityblock.ehrmodels.elation.datamodelapi.problem.Problem
import cityblock.ehrmodels.elation.datamodelapi.vital.Vital
import cityblock.ehrmodels.elation.datamodelapi.physician.Physician
import cityblock.ehrmodels.elation.datamodelapi.practice.Practice
import cityblock.member.service.models.PatientInfo.Patient
import cityblock.streaming.jobs.aggregations.Encounters.PatientEncounter
import cityblock.streaming.jobs.aggregations.Medications.PatientMedication
import cityblock.transforms.elation.{EventTransforms, LabTransforms}
import cityblock.streaming.jobs.common.ParsedPatientMedicalDataLists
import cityblock.streaming.jobs.orders.Orders.PatientOrder
import cityblock.transforms.elation.ProblemTransforms._
import cityblock.transforms.elation.VitalTransforms._
import cityblock.transforms.elation.MedicationTransforms._
import cityblock.utilities.{EhrMedicalDataIO, Environment, SubscriberHelper}
import com.softwaremill.sttp.{Id, SttpBackend}
import io.circe.optics.JsonPath._
import io.circe._
import io.circe.parser._
import com.spotify.scio.values.{SCollection, WindowOptions}
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.{CREATE_IF_NEEDED, WRITE_APPEND}
import javax.naming.ServiceUnavailableException
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.transforms.windowing.{AfterPane, Repeatedly}
import org.apache.beam.sdk.values.WindowingStrategy.AccumulationMode
import org.joda.time.{Duration, Instant}

/**
 * See scio-jobs/README.md for details on this objects purpose.
 */
object ElationSubscriber extends SubscriberHelper with EhrMedicalDataIO {
  val datasource = "elation"

  @BigQueryType.toTable
  case class ElationMessage(
    patient: Patient,
    eventType: String,
    messageId: String,
    payload: String,
    insertedAt: Instant
  )

  def main(cmdlineArgs: Array[String]): Unit = {
    implicit val environment: Environment = Environment(cmdlineArgs)

    val (opts, args) = ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)
    val sc = ScioContext(opts)

    val subscription: String = args.required("subscription")
    val schedulingTopic: String = args.required("schedulingTopic")
    val elationMessagesTable: String = args.required("elationMessagesTable")

    val medicalDataset: String = args.required("medicalDataset")

    val testing = sc.isTest

    val msgAndAttributes: SCollection[(String, Map[String, String])] =
      sc.pubsubSubscriptionWithAttributes[String](subscription)
        .withFixedWindows(
          Duration.standardSeconds(1),
          Duration.ZERO,
          WindowOptions(
            Repeatedly.forever(AfterPane.elementCountAtLeast(1)),
            AccumulationMode.DISCARDING_FIRED_PANES,
            Duration.standardDays(7)
          )
        )

    val elationMsgsAndObjs: SCollection[(ElationMessage, SubscriptionObject)] =
      processElationPubSubMessages(msgAndAttributes, testing)

    val elationMsgs = elationMsgsAndObjs.map(_._1)
    saveElationMessagesToBigQuery(elationMessagesTable, elationMsgs)

    val backend = SttpHelper.getBackend(testing)
    val allElationPractices =
      pullAllElationPractices()(backend)

    val elationApptsPubSubMsgs =
      processElationAppointments(
        elationMsgsAndObjs,
        allElationPractices,
        testing
      )

    elationApptsPubSubMsgs
      .withName("Publish Scheduling Events to Commons")
      .saveAsPubsubWithAttributes[String](schedulingTopic)

    val patientOrders: SCollection[PatientOrder] =
      processElationLabs(elationMsgsAndObjs, testing)

    patientOrders.saveAsTypedBigQuery(
      s"${environment.projectName}:$medicalDataset.patient_orders",
      WRITE_APPEND,
      CREATE_IF_NEEDED
    )

    val parsedPatientMedicalDataLists =
      pullAndProcessElationMedicalInfo(
        elationMsgs,
        testing
      )

    aggregateAndPersistPatientMedicalData(
      patientMedicalDataLists = parsedPatientMedicalDataLists,
      medicalDataset = medicalDataset
    )

    sc.close().waitUntilFinish()
  }

  /** [[processElationLabs]] filters the web hook for all Lab Results where the patients have a CityBlockID.
   * After the filtering, rather than using the json response from the elation hook, this method grabs the
   * ID of the LabResult and directly queries against the Elation server. Since elation currently does
   * not provide a message signature, it is not possible to prove that the message has not been tampered with.
   * In order to prevent any such issues, all events are re-generated from a single Id, i.e. the Lab Id.
   * We also discredit the validity of the patient id and reconstruct it under the circumstances that there
   * is inconsistency */
  def processElationLabs(
    elationMsgAndObj: SCollection[(ElationMessage, SubscriptionObject)],
    testing: Boolean
  )(
    implicit environment: Environment
  ): SCollection[PatientOrder] =
    elationMsgAndObj
      .withName(
        "Select lab objects for patients with a cityblock id and transform to PatientOrder"
      )
      .flatMap { msgAndObj =>
        val (msg, subscription) = msgAndObj

        // Filters for only lab events and patients that have a CityBlock Id
        (msg, subscription) match {
          case (msg, labresult: Lab) if msg.patient.patientId.nonEmpty =>
            val backend = SttpHelper.getBackend(testing)

            Lab.findById(labresult.id.toString)(backend) match {
              case Right(lab) =>
                //Verify that the patient id provided in the lab is the same as the json response.
                //If not, then reconstruct the Patient object from the queried lab data. Otherwise,
                //use the already constructed patient object.
                val patient =
                  if (msg.patient.externalId == labresult.patient.toString) {
                    msg.patient
                  } else {
                    getPatientByExternalId(
                      testing = testing,
                      externalId = lab.patient.toString,
                      carrier = datasource,
                      logger = logger
                    )
                  }
                Seq(
                  LabTransforms.transformLabToPatientOrders(
                    lab,
                    patient,
                    msg.messageId
                  )(backend)
                )
              case Left(error) =>
                logger.error(
                  s"Unable to find LabResult with ID = ${labresult.id}, Error = $error"
                )
                Seq()
            }
          case _ => Seq()
        }
      }

  def processElationAppointments(
    elationMsgAndObj: SCollection[(ElationMessage, SubscriptionObject)],
    allElationPractices: Map[Long, Practice],
    testing: Boolean
  ): SCollection[(String, Map[String, String])] =
    // TODO: Add filtering at a higher level for all Elation Patients who are not members of CityBlock/Commons
    elationMsgAndObj
      .withName(
        "Filter for memberIds and Appointment objects for transform to Redox calendar events"
      )
      .flatMap { msgAndObj =>
        val (msg, subObj) = msgAndObj

        (msg.patient.patientId, subObj) match {
          case (Some(memberId), appt: Appointment) =>
            Seq(
              EventTransforms.appointmentToPubSubMessage(
                appt,
                msg.messageId.toLong,
                memberId,
                allElationPractices,
                testing
              )
            )
          case _ => Seq()
        }
      }

  def processElationPubSubMessages(
    msgAndAttributes: SCollection[(String, Map[String, String])],
    testing: Boolean
  )(implicit environment: Environment): SCollection[(ElationMessage, SubscriptionObject)] =
    msgAndAttributes
      .flatMap {
        case (msg, attributes) =>
          logger.debug(s"received message $msg")

          val msgId = attributes.get("messageId") match {
            case Some(messageId) => messageId
            case None =>
              throw new Exception(
                "messageId from Elation is not populated in the Pubsub event message"
              )
          }

          val resource = attributes.getOrElse("resource", "Unknown")
          val action = attributes.getOrElse("action", "Unknown")

          val eventType = s"${resource}_$action"
          val msgJson = parse(msg).getOrElse(Json.Null)

          val dataJsonPath = root.data.json
          val dataJson = dataJsonPath.getOption(msgJson).getOrElse(Json.Null)

          val elationObj = SubscriptionObject(dataJson, resource)

          elationObj match {
            case Left(error) =>
              logger.error(
                s"Elation Json of resource $resource could not be converted to Elation Message. Error: $error."
              )
              Seq()
            case Right(elationObject) =>
              elationObject.getPatientId match {
                case None => Seq()
                case Some(patientId) =>
                  val patient = getPatientByExternalId(
                    testing = testing,
                    patientId,
                    carrier = datasource,
                    logger = logger
                  )
                  Seq(
                    (
                      ElationMessage(
                        patient = patient,
                        eventType = eventType,
                        messageId = msgId,
                        payload = msg,
                        insertedAt = Instant.now
                      ),
                      elationObject
                    )
                  )
              }
          }
      }

  def pullAndProcessElationMedicalInfo(
    elationMessages: SCollection[ElationMessage],
    testing: Boolean
  )(implicit environment: Environment): ParsedPatientMedicalDataLists = {
    val collectedMedicalList = elationMessages.flatMap { pubSubElationMsg =>
      pubSubElationMsg.patient.patientId match {
        case Some(_) =>
          implicit val backend: SttpBackend[Id, Nothing] = SttpHelper.getBackend(testing)

          val elationProblems =
            pullElationProblemsForPatient(pubSubElationMsg.patient)
          val elationVitals =
            pullElationVitalsForPatient(pubSubElationMsg.patient)
          val elationCcda =
            pullElationCcdaForPatient(pubSubElationMsg.patient)
          Seq(
            (
              transformElationProblemsToPatientProblems(
                pubSubElationMsg.messageId,
                pubSubElationMsg.patient,
                elationProblems
              ),
              elationCcdaToPatientMeds(
                pubSubElationMsg.messageId,
                pubSubElationMsg.patient,
                elationCcda
              ),
              List.empty[PatientEncounter],
              transformElationVitalsToPatientVitals(
                pubSubElationMsg.messageId,
                pubSubElationMsg.patient,
                elationVitals
              )
            )
          )

        case _ =>
          logger.info(
            s"""Not pulling elation medical info for patient with
               |externalId: ${pubSubElationMsg.patient.externalId} but no patientId.""".stripMargin
          )
          Seq()
      }
    }

    ParsedPatientMedicalDataLists(
      patientProblemLists = collectedMedicalList.map { _._1 },
      patientMedicationLists = collectedMedicalList.map { _._2 },
      patientEncounterLists = collectedMedicalList.map { _._3 },
      patientVitalSignLists = collectedMedicalList.map { _._4 },
      patientResultLists = None // No need to handle these for Elation
    )
  }

  private def pullElationProblemsForPatient(
    patient: Patient
  )(implicit backend: SttpBackend[Id, Nothing]): List[Problem] =
    Problem.findAllByPatientId(patient.externalId) match {
      case Right(problems) =>
        logger.info(
          s"""Pulled ${problems.length} problems for member.
             |ExternalId - ${patient.externalId}, patientId - ${patient.patientId}""".stripMargin
        )
        problems
      case Left(error) =>
        throw new ServiceUnavailableException(s"""
             |Could not pull problems for member. ExternalId - ${patient.externalId}, patientId - ${patient.patientId}.
             |Error: ${error.toString}
            """.stripMargin)
    }

  private def pullElationVitalsForPatient(
    patient: Patient
  )(implicit backend: SttpBackend[Id, Nothing]): List[Vital] =
    Vital.findAllByPatientId(patient.externalId) match {
      case Right(vitals) =>
        logger.info(
          s"""Pulled ${vitals.length} vitals for member.
             |ExternalId - ${patient.externalId}, patientId - ${patient.patientId}""".stripMargin
        )
        vitals.filter(_.isSigned())
      case Left(error) =>
        throw new ServiceUnavailableException(s"""
             |Could not pull vitals for member. ExternalId - ${patient.externalId}, patientId - ${patient.patientId}.
             |Error: ${error.toString}
            """.stripMargin)
    }

  private def pullElationCcdaForPatient(
    patient: Patient
  )(implicit backend: SttpBackend[Id, Nothing]): Option[Ccda] = {
    val Patient(patientId, externalId, _) = patient
    Ccda
      .findByPatientId(patient.externalId)
      .fold[Option[Ccda]](
        error => {
          logger.error(
            s"Error pulling CCDA for member [externalId: $externalId, patientId: $patientId. error: ${error.toString}]"
          )
          None
        },
        ccda => {
          logger.info(s"Pulled CCDA for member [externalId: $externalId, patientId: $patientId]")
          Option(ccda)
        }
      )
  }

  private def pullAllElationPractices()(
    implicit backend: SttpBackend[Id, Nothing]
  ): Map[Long, Practice] =
    Practice.findAll match {
      case Left(error) =>
        throw new Exception(
          s"Could not pull all Elation practices. Error: ${error.toString}"
        )

      case Right(practices) if practices.isEmpty =>
        throw new Exception(
          "Elation all practices end point returned an empty list."
        )

      case Right(practices) =>
        logger.info(s"Pulled ${practices.length} practices in all of elation.")

        val practicesMap = practices.map { practice =>
          practice.id -> practice
        }.toMap

        practicesMap
    }

  def pullElationPhysician(id: Long)(
    implicit backend: SttpBackend[Id, Nothing]
  ): Option[Physician] =
    Physician.findPhysician(id) match {
      case Left(error) =>
        throw new Exception(
          s"Could not pull Elation physician. Error: ${error.toString}"
        )
      case Right(physician) =>
        logger.info(
          s"Pulled physician $id from elation."
        )
        Option(physician)
    }

  //Function broken out to facilitate testing of MedicationTransforms.transformElationCcdaToPatientMedications
  private def elationCcdaToPatientMeds(
    messageId: String,
    patient: Patient,
    elationCcda: Option[Ccda]
  ): List[PatientMedication] =
    elationCcda.fold[List[PatientMedication]](
      List.empty[PatientMedication]
    )(
      transformElationCcdaToPatientMedications(messageId, patient, _)
    )

  private def saveElationMessagesToBigQuery(
    tableName: String,
    rows: SCollection[ElationMessage]
  )(implicit environment: Environment): Unit =
    rows.saveAsTypedBigQuery(
      s"${environment.projectName}:streaming.$tableName",
      WRITE_APPEND,
      CREATE_IF_NEEDED
    )
}
