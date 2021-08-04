package cityblock.ehrmodels.redox.service

import com.github.vitalsoftware.scalaredox.client.ClientConfig
import com.typesafe.config.ConfigFactory

object RedoxApiConfig {
  private val applicationConf = ConfigFactory.load()
  val destinationId: String =
    applicationConf.getString("redox.destinationId")
  val destinationName: String =
    applicationConf.getString("redox.destinationName")
  val conf: ClientConfig = ClientConfig(applicationConf)
}
