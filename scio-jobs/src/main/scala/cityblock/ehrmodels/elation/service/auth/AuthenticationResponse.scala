package cityblock.ehrmodels.elation.service.auth

import cityblock.ehrmodels.elation.service.AccessToken
import cityblock.utilities.backend.ApiError
import com.softwaremill.sttp.{Id, SttpBackend}
import io.circe.generic.JsonCodec

/**
 * AuthenticationResponse contains information required in order to connect to Elation Services
 * @param access_token this is the authentication string needed to be passed to every request made to the server
 * @param expires_in time till the token expires. This is generally 36000 secs, i.e. 10 hours
 * @param scope permissions and access the delegated token has to the server resources
 */

@JsonCodec
case class AuthenticationResponse(access_token: AccessToken,
                                  token_type: String,
                                  expires_in: Int,
                                  refresh_token: String,
                                  scope: String)

/**
 * Meant to provide an AuthToken for connecting to Elation. The tokens have a life span of 10 hours, i.e. 36000 Secs
 * Since the life of the token is long, this companion object is treated as a singleton and meant to be initialized just once
 */
object AuthenticationResponse {

  private var auth_token: Option[Either[ApiError, AuthenticationResponse]] =
    None

  /**
   * Is meant to generate a new Token under the circumstance that one has expired. Currently this is Not used anywhere in the code base
   */
  private[service] def requestNewToken()(implicit backend: SttpBackend[Id, Nothing]): Unit =
    auth_token = Some(new AuthToken().generateAuthToken())

  /**
   * Meant to provide complete details for the authenticated token to use when connecting to Elation
   * @return Either[ApiError, AuthenticationResponse]
   */
  private[service] def getAuthToken()(
    implicit backend: SttpBackend[Id, Nothing]): Either[ApiError, AuthenticationResponse] =
    auth_token match {
      case Some(token) => token
      case _           => new AuthToken().generateAuthToken()
    }
}
