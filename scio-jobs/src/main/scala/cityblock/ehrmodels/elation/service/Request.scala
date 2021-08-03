package cityblock.ehrmodels.elation.service

import io.circe.Decoder
import com.softwaremill.sttp._
import com.softwaremill.sttp.circe._
import cityblock.ehrmodels.elation.service.auth.AuthenticationResponse
import cityblock.utilities.backend.{ApiError, ObjectDoesNotExistError}

object Request {

  private def makeGetRequest[U: Decoder](access_token: AccessToken, url: Uri)(
    implicit backend: SttpBackend[Id, Nothing]
  ): Either[ApiError, U] = {

    val request = sttp
      .get(url)
      .auth
      .bearer(access_token.toString)
      .response(asJson[U])

    val ret = request.send.body

    ApiError
      .convertResponse[U, ObjectDoesNotExistError](ret, Some(url.toString))
  }

  private[service] def getRequest[T: Decoder](
    url: Uri
  )(implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, T] = {

    val objectResponse: Either[ApiError, T] = for {
      auth <- AuthenticationResponse.getAuthToken()
      body <- makeGetRequest[T](auth.access_token, url)
    } yield body

    objectResponse
  }
}
