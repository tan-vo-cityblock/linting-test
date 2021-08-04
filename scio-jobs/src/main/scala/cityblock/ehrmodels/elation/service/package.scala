package cityblock.ehrmodels.elation

package object service {

  /**
   * This is a value class meant to provide convenience and speed to accessing tokens while authenticating
   * rather than using the [[cityblock.ehrmodels.elation.service.auth.AuthenticationResponse]] class
   */
  type AccessToken = String

}
