package cityblock.utilities

import cityblock.utilities.secrets.{GSMSecret, GSMSecretsSeer, KMSSecret, KMSSecretsSeer}
import com.softwaremill.sttp.{HttpURLConnectionBackend, Id, SttpBackend}
import com.spotify.scio.Args

import scala.util.Random

case class Environment(
  name: String,
  projectName: String,
  patientDataBucket: String,
  memberServiceUrl: String,
  memberServiceSecretData: String,
  backends: Backends
) {
  lazy val jobId: String = Random.alphanumeric.take(8).mkString.toLowerCase
}

/**
 * This class holds references to various kinds of backend handlers. It exists for two reasons:
 * 1. We want to be able to override these handlers with mocks in some tests.
 * 2. We can't pass them in directly as arguments in Environment because it needs to be
 *    serializable.
 *
 * When adding new backends here, make sure to increment the serial version.
 */
@SerialVersionUID(1L)
class Backends extends Serializable {
  @transient lazy val sttpBackend: SttpBackend[Id, Nothing] =
    HttpURLConnectionBackend()
}

object Environment {
  lazy val Prod: Environment = Environment(
    "prod",
    "cityblock-data",
    "gs://cityblock-production-patient-data",
    "https://cbh-member-service-prod.appspot.com",
    KMSSecretsSeer.see(
      KMSSecret(
        gcsBucket = "cbh-member-service-prod-secrets",
        fileName = "app.yaml.enc",
        keyProject = "cbh-kms",
        keyLocation = "us",
        keyRing = "cbh-member-service-prod",
        keyName = "app-yaml-key"
      )
    ),
    new Backends()
  )
  lazy val Staging: Environment = Environment(
    "staging",
    "staging-cityblock-data",
    "gs://cityblock-staging-patient-data",
    "https://cbh-member-service-staging.appspot.com",
    GSMSecretsSeer.see(
      GSMSecret(
        projectId = "451611519620",
        secretId = "staging-member-service-app-yaml"
      )
    ),
    new Backends()
  )

  lazy val Test: Environment = Environment(
    "test",
    "test-projectName",
    "test-patientDataBucket",
    "test-memberServiceUrl",
    "test-secret",
    new Backends()
  )

  def apply(cmdlineArgs: Array[String]): Environment = {
    val args = Args(cmdlineArgs)

    val environmentName = args.required("environment")
    val baseEnvironment = environmentName match {
      case Prod.name    => Prod
      case Staging.name => Staging
      case Test.name    => Test
      case _ =>
        throw new IllegalArgumentException(
          s"Invalid environment specified: $environmentName"
        )
    }

    val projectName = args.optional("project")
    val patientDataBucket = args.optional("patientDataBucket")

    baseEnvironment.copy(
      projectName = projectName.getOrElse(baseEnvironment.projectName),
      patientDataBucket = patientDataBucket.getOrElse(baseEnvironment.patientDataBucket)
    )
  }
}
