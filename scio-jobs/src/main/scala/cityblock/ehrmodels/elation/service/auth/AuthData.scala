package cityblock.ehrmodels.elation.service.auth

import cityblock.utilities.Loggable

/** Note: ConfigFactory is not type safe and throws an exception when not found. Need to find another library*/
import com.typesafe.config.ConfigFactory

sealed trait AuthData

final private[auth] case class ClientData(val id: String, val secret: String) extends AuthData

final private[auth] case class SystemData(val username: String,
                                          val password: String,
                                          val grant_type: String = "password")
    extends AuthData

/**
 * The AuthData Companion object is meant to be a factory object for the two child classes, i.e. ClientData
 * and SystemData.
 */
object AuthData extends Loggable {
  val conf = ConfigFactory.load()
  val EmptyLine = ""

  private def emptyFieldException(missingField: String): String =
    s"""Unable to find properties for ${missingField}.
       |Any code related to the Elation package will most likely fail.\n
       |In order to pass ${missingField}, please configure the value for
       |${missingField} in the application.conf file. For reference, look
       |at the application.conf.example"
        """.stripMargin

  private def getProperty(prop: String): String =
    Option(conf.getString(prop)) match {
      case Some(client_id) => client_id
      case None => {
        logger.warn(emptyFieldException(prop))
        System.exit(1)
        ""
      }
    }

  private val CLIENT_ID: String = getProperty("Elation.client_id")
  private val CLIENT_SECRET: String = getProperty("Elation.client_secret")
  private val USERNAME: String = getProperty("Elation.username")
  private val PASSWORD: String = getProperty("Elation.password")

  val client = new ClientData(CLIENT_ID, CLIENT_SECRET)
  val system = new SystemData(USERNAME, PASSWORD)
  val endpoint: String = getProperty("Elation.endpoint")

}
