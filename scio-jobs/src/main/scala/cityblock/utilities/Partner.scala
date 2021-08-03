package cityblock.utilities

case class Partner(name: String) {
  def configuration: Option[PartnerConfiguration] =
    PartnerConfiguration.getConfigurationByPartner(this)
}

object Partner {}
