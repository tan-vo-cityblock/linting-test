package cityblock.transforms.redox

import cityblock.ehrmodels.redox.service.RedoxApiConfig
import cityblock.utilities.Environment
import cityblock.utilities.time.DateOrInstant

trait PrepareQuery {
  def preparePatientQueryString(externalId: String)(implicit environment: Environment): String = {
    val test = environment == Environment.Prod
    s"""
       |{
       |	"Meta": {
       |		"DataModel": "Clinical Summary",
       |		"EventType": "PatientQuery",
       |		"Test": $test,
       |		"Destinations": [
       |			{
       |				"ID": "${RedoxApiConfig.destinationId}",
       |				"Name": "${RedoxApiConfig.destinationName}"
       |			}
       |		]
       |	},
       |	"Patient": {
       |		"Identifiers": [
       |			{
       |				"ID": "$externalId",
       |				"IDType": "MR"
       |			}
       |		]
       |	}
       |}
     """.stripMargin
  }

  def prepareVisitQueryString(
    externalId: String,
    encounterStartTime: Option[DateOrInstant],
    encounterEndTime: Option[DateOrInstant]
  )(implicit environment: Environment): String = {
    val test = environment == Environment.Prod

    var startTime = cleanKeyForQuery(encounterStartTime, "StartDateTime").concat(",")
    val endTime = cleanKeyForQuery(encounterEndTime, "EndDateTime")

    if (startTime != "\n" && endTime == "\n") {
      startTime = startTime.dropRight(1)
    }
    s"""
       |{
       |	"Meta": {
       |		"DataModel": "Clinical Summary",
       |		"EventType": "VisitQuery",
       |		"Test": $test,
       |		"Destinations": [
       |			{
       |				"ID": "${RedoxApiConfig.destinationId}",
       |				"Name": "${RedoxApiConfig.destinationName}"
       |			}
       |		]
       |	},
       |	"Patient": {
       |		"Identifiers": [
       |			{
       |				"ID": "$externalId",
       |				"IDType": "MR"
       |			}
       |		]
       |	},
       |  "Visit": {
       |    $startTime
       |    $endTime
       |  }
       |}
     """
  }

  private def cleanKeyForQuery(dateTime: Option[DateOrInstant], keyString: String): String =
    dateTime match {
      case Some(DateOrInstant(_, instantIso, _)) if instantIso.isDefined =>
        s"""\"$keyString\": \"${instantIso.get}\""""
      case Some(DateOrInstant(_, _, localDate)) if localDate.isDefined =>
        s"""\"$keyString\": \"${localDate.get}\""""
      case None => "\n"
    }
}
