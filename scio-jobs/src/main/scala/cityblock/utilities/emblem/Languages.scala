package cityblock.utilities.emblem

object Languages {

  def getHumanReadableLanguage(languageCode: Option[String]): String =
    languageCode match {
      case Some("1") => "English"
      case Some("2") => "Spanish"
      case Some("3") => "Other Indo-European Language"
      case Some("4") => "Asian and Pacific Island Languages"
      case Some("8") => "Other Languages"
      case _         => "Spoken Language Unknown"
    }
}
