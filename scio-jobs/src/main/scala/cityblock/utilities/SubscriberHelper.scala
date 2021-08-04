package cityblock.utilities

import cityblock.member.service.io.MemberService
import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import org.slf4j.Logger
//TODO: Rename to MemberServiceHelper to be more generic for extending batch jobs.
trait SubscriberHelper {

  protected def getPatientByExternalId(
    testing: Boolean = false,
    externalId: String,
    carrier: String,
    logger: Logger
  )(implicit environment: Environment): Patient =
    if (testing) {
      getTestPatient(externalId = externalId, carrier = carrier)
    } else {
      getPatientByMrn(
        externalId,
        carrier,
        logger
      )
    }

  private def getTestPatient(externalId: String = "99", carrier: String): Patient =
    Patient(Some("DummyId"), externalId, Datasource(carrier))

  private def getPatientByMrn(externalId: String, carrier: String, logger: Logger)(
    implicit environment: Environment
  ): Patient =
    MemberService.getMrnPatients(carrier = Some(carrier), externalId = Some(externalId)) match {
      case Right(response) =>
        response match {
          case member :: _ =>
            logger.info(
              s"Found matching member in member service [memberId: ${member.patientId}, carrier: ${carrier}, externalId: ${externalId}, env: ${environment.name}]"
            )
          case _ =>
            logger.error(
              s"Could not find a matching member in member service [carrier: ${carrier}, externalId: ${externalId}, env: ${environment.name}]"
            )
        }
        response.headOption.getOrElse(Patient(None, externalId, Datasource(carrier)))
      case Left(error) =>
        logger.error(s"Error querying Member Service (${environment.name}: $error")
        Patient(None, externalId, Datasource(carrier))
    }
}
