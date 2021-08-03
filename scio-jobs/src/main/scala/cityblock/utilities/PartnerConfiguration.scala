package cityblock.utilities

/**
 * @param partner The Partner structure that stores the name.
 * @param productionProject The Google Cloud Project where production ingested data should be stored.
 * @param stagingProject The Google Cloud Project where staging ingested data should be stored.
 * @param indexName The name of the Partner's key in the externalIds field in the Patient Index.
 *                  Usually downcased like "emblem" or "connecticare".
 */
case class PartnerConfiguration(
  partner: Partner,
  productionProject: String,
  stagingProject: String,
  indexName: String,
) {
  def hasProject(project: String): Boolean =
    project == productionProject || project == stagingProject
}

/**
 * Add new partner configurations as public values in this companion object. Also, add that value
 * to [[PartnerConfiguration.configurations]] and [[PartnerConfiguration.partners]].
 */
object PartnerConfiguration {
  val emblem: PartnerConfiguration = PartnerConfiguration(
    Partner("Emblem"),
    "emblem-data",
    "staging-emblem-data",
    "emblem",
  )

  val connecticare: PartnerConfiguration = PartnerConfiguration(
    Partner("ConnectiCare"),
    "connecticare-data",
    "staging-connecticare-data",
    "connecticare",
  )

  val tufts: PartnerConfiguration = PartnerConfiguration(
    Partner("Tufts"),
    "tufts-data",
    "staging-tufts-data",
    "tufts"
  )

  val carefirst: PartnerConfiguration = PartnerConfiguration(
    Partner("CareFirst"),
    "cbh-carefirst-data",
    "cbh-carefirst-data-staging",
    "carefirst"
  )

  val cardinal: PartnerConfiguration = PartnerConfiguration(
    Partner("Cardinal"),
    "cbh-cardinal-data",
    "cbh-cardinal-data-staging",
    "cardinal"
  )

  val healthyblue: PartnerConfiguration = PartnerConfiguration(
    Partner("HealthyBlue"),
    "cbh-healthyblue-data",
    "cbh-healthyblue-data-staging",
    "Healthy Blue"
  )

  val partners: Map[String, PartnerConfiguration] =
    Map(emblem.indexName -> emblem,
        connecticare.indexName -> connecticare,
        tufts.indexName -> tufts)

  private val configurations: List[PartnerConfiguration] =
    List(emblem, connecticare, tufts)

  require(
    configurations.map(_.indexName).distinct.length == configurations.length,
    "Partner index names must be unique."
  )

  def getPatientIndexProject(project: String): Option[String] = project match {
    case _ if configurations.map(_.productionProject).contains(project) =>
      Some(Environment.Prod.projectName)
    case _ if configurations.map(_.stagingProject).contains(project) =>
      Some(Environment.Staging.projectName)
    case _ => None
  }

  def projectIsStaging(project: String): Boolean =
    configurations.exists(_.stagingProject == project)

  def getPartnerByProject(project: String): Option[Partner] = project match {
    case emblem.productionProject | emblem.stagingProject =>
      Some(emblem.partner)
    case connecticare.productionProject | connecticare.stagingProject =>
      Some(connecticare.partner)
    case tufts.productionProject | tufts.stagingProject =>
      Some(tufts.partner)
    case _ => None
  }

  def getConfigurationByPartner(partner: Partner): Option[PartnerConfiguration] =
    configurations.find(_.partner == partner)
}
