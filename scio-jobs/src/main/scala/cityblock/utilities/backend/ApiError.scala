package cityblock.utilities.backend

import com.softwaremill.sttp.DeserializationError
import io.circe.generic.JsonCodec
import io.circe.parser.parse
import io.circe.{Decoder, DecodingFailure, Json, ParsingFailure}

sealed trait ApiError

sealed trait CirceError extends ApiError
sealed trait BackendError extends ApiError

@JsonCodec
case class ResponseError[T <: BackendError](error: T, url: Option[String]) extends ApiError
@JsonCodec
case class ObjectDoesNotExistError(detail: String) extends BackendError
@JsonCodec
case class TokenError(detail: String) extends BackendError
@JsonCodec
case class UnknownResponseError(detail: String) extends BackendError
@JsonCodec
case class UnknownDeserializationFailure(original: String, message: Option[String])
    extends CirceError
@JsonCodec
case class CirceDeserializationFailure[T](original: String, error: T, message: String)
    extends CirceError

/** The CirceError companion object is meant to convert an STTP based Circe Error to our own custom CirceError Object */
object CirceError {
  def apply(jsonString: String, circeError: DeserializationError[io.circe.Error]): CirceError =
    circeError match {
      case DeserializationError(original, error, message) =>
        CirceDeserializationFailure[io.circe.Error](original, error, message)
      case _ => UnknownDeserializationFailure(jsonString, None)
    }

  def apply(
    jsonString: String,
    error: io.circe.Error
  ): CirceDeserializationFailure[io.circe.Error] = {
    val msg = error match {
      case ParsingFailure(message, _) => message
      case other: DecodingFailure     => other.message
      case _                          => error.getMessage
    }

    CirceDeserializationFailure(jsonString, error, msg)
  }
}

object ApiError {

  /**
   * Upon receiving a response from an HTTP backend, you will receive either a CirceError, a String response error from the server
   * or an instance of the Requested Case Class [T].
   *
   * This object is meant to Normalize them to either an [[ApiError]] or the requested case class T
   *
   * This should never be exposed in any of the datamodels and is meant to normalize responses to Error case classes
   *
   * @param response this is the response received from the server which needs to be flattened
   * @param url      this is the url to which the request was made to. Only used if an ElationError is received from the server
   * @tparam T this is the generic Class of the object we requested. Only returns when request is successful
   * @tparam U this is the [[BackendError]] sub-type you wish to match to.
   * @return Either[ApiError, T]
   */
  def convertResponse[T, U <: BackendError: Decoder](
    response: Either[String, Either[DeserializationError[io.circe.Error], T]],
    url: Option[String]
  ): Either[ApiError, T] =
    response match {
      case Right(Right(success)) => Right(success)
      case Right(Left(sttpError)) =>
        Left(CirceError(sttpError.original, sttpError))
      case Left(errorString) => {
        val parseResponse = parse(errorString)
          .getOrElse(Json.Null)
          .as[U]
          .getOrElse(UnknownResponseError(errorString))
        Left(ResponseError(parseResponse, url))
      }
    }

}
