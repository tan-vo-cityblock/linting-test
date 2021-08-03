package cityblock.models.gold.enums

sealed abstract class SpecialtyTier(val name: String) extends Enum

object SpecialtyTier extends CanConvertFromString[SpecialtyTier] {
  case object One extends SpecialtyTier("1")
  case object Two extends SpecialtyTier("2")
  case object Three extends SpecialtyTier("3")
  case object Four extends SpecialtyTier("4")

  override val default: SpecialtyTier = One
  override val values: List[SpecialtyTier] = List(One, Two, Three)
  override protected def name(t: SpecialtyTier): String = t.name
}
