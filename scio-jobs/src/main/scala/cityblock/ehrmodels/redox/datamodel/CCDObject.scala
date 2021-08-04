package cityblock.ehrmodels.redox.datamodel

import cityblock.ehrmodels.redox.datamodel.encounter.Encounter
import cityblock.utilities.backend.{ApiError, UnknownResponseError}
import io.circe.Json

trait CCDObject

object CCDObject {
  def apply(dataJson: Json, resource: String): Either[ApiError, List[CCDObject]] =
    resource match {
      case "Encounters" => Encounter(dataJson)
      case _ =>
        Left(UnknownResponseError(s"Unable to find data model type for redox resource $resource"))
    }
}
