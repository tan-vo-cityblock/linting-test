package cityblock.transforms.redox

import cityblock.ehrmodels.redox.service.RedoxApiQueryResponse
import cityblock.utilities.{Environment, Loggable}
import com.github.vitalsoftware.scalaredox.models.PatientQuery
import play.api.libs.json.JsValue

object PrepareClinicalSummary extends PrepareQuery with Loggable {
  def pullClinicalSummary(
    externalId: String
  )(implicit environment: Environment): Option[String] = {
    val patientJsonQueryString = preparePatientQueryString(externalId)
    val patientQuery = RedoxApiQueryResponse.createQuery[PatientQuery](patientJsonQueryString)
    logger.info(s"""Attempting clinical summary query for [externalId: $externalId]""")
    val clinicalSummary =
      RedoxApiQueryResponse.redoxClient.get[PatientQuery, JsValue](patientQuery)
    RedoxApiQueryResponse
      .awaitResponse[JsValue](clinicalSummary)(externalId)
      .map(ccd => ccd.toString())
  }
}
