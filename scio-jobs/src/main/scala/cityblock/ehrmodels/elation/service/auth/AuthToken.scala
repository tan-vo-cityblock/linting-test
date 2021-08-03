package cityblock.ehrmodels.elation.service.auth

import cityblock.utilities.backend.{ApiError, TokenError}
import com.softwaremill.sttp.circe.asJson
import com.softwaremill.sttp.{sttp, _}

/**
 * AuthToken is a singleton object used to generate tokens for the AuthenticationResponse class.
 * This class uses the aid of AuthData to grab sensitive data when connecting to Elation
 */
private[auth] class AuthToken {

  def generateAuthToken()(
    implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, AuthenticationResponse] = {

    val client = AuthData.client
    val system = AuthData.system
    val endpoint = AuthData.endpoint

    val url: Uri = uri"https://${endpoint}/api/2.0/oauth2/token/"

    val req = sttp.auth
      .basic(client.id, client.secret)
      .body(
        Map("grant_type" -> system.grant_type,
            "username" -> system.username,
            "password" -> system.password))
      .response(asJson[AuthenticationResponse])
      .post(url)

    val response = req.send().body

    ApiError.convertResponse[AuthenticationResponse, TokenError](response, Some(url.toString()))
  }

}
