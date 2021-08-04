package cityblock.models.gold.enums

sealed abstract class TaxonomyTier(val name: String) extends Enum

object TaxonomyTier extends CanConvertFromString[TaxonomyTier] {
  case object One extends TaxonomyTier("1")
  case object Two extends TaxonomyTier("2")
  case object Three extends TaxonomyTier("3")

  override val default: TaxonomyTier = One
  override val values: List[TaxonomyTier] = List(One, Two, Three)
  override protected def name(t: TaxonomyTier): String = t.name
}
