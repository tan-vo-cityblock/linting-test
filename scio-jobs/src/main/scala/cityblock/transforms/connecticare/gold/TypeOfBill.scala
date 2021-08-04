package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.Medical
import cityblock.utilities.{reference, Loggable, Strings}

object TypeOfBill extends Loggable {

  // as defined in https://jira.cityblock.com/browse/COM-2575
  private def locationToTypeOfBillMap: Map[String, String] =
    Map(
      "24" -> "0831",
      "11" -> "0131",
      "19" -> "0131",
      "22" -> "0131",
      "23" -> "0131",
      "12" -> "0331",
      "21" -> "0111",
      "20" -> "0781",
      "31" -> "0211",
      "34" -> "0821",
      "65" -> "0721",
      "62" -> "0751",
      "25" -> "0841"
    )

  private def format(code: String): String =
    Strings.zeroPad(code, reference.tables.TypeOfBill.padTo)

  private def impute(locations: List[String]): Option[String] =
    locations.flatMap(locationToTypeOfBillMap.get).headOption

  def get(silver: List[Medical]): Option[String] = {
    val validCodes = silver.flatMap(_.medical.BillTypeCd)

    // TODO write this to a table instead of logs
    if (validCodes.distinct.length > 1) {
      val ids = silver.map(_.identifier.surrogateId).mkString(",")
      logger.warn(s"CCI Amysis Facility claim with multiple BillTypeCd [surrogates: ($ids)]")
    }

    validCodes.headOption match {
      case Some(code) => Some(format(code))
      case _ =>
        impute(silver.flatMap(_.medical.Location)) // TODO make sure Location is already imputed
    }
  }
}
