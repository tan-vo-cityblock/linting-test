package cityblock.models.gold.enums

abstract private[enums] class Enum

private[enums] trait CanConvertFromString[T <: Enum] {
  protected val default: T
  protected val values: List[T]
  protected def name(t: T): String
  def fromString(s: String): T = values.find(name(_) == s).getOrElse(default)
}
