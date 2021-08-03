package cityblock.ehrmodels.elation.service

import io.circe.generic.JsonCodec

/** Is meant to be used to hold object lists for datamodel types when using a findAll request
 * such as [[cityblock.ehrmodels.elation.service.Elation.getAll]]. This case class is based off Elation's
 * Pagination object which can be found here https://docs.elationhealth.com/reference#pagination.
 * Although not enforced, T must have it's own Encoder/Decoder in order to function correctly. You may use Circe's
 * AutoEncoder or Semi-Auto Encoder*/

@JsonCodec
private[service] case class ElationList[T](count: Int,
                                           next: Option[String],
                                           previous: Option[String],
                                           results: List[T])
