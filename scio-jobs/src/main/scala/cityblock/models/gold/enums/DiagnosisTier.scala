package cityblock.models.gold.enums

sealed abstract class DiagnosisTier(val name: String) extends Enum

object DiagnosisTier extends CanConvertFromString[DiagnosisTier] {
  case object Principal extends DiagnosisTier("principal")
  case object Admit extends DiagnosisTier("admit")
  case object Secondary extends DiagnosisTier("secondary")

  override val default: DiagnosisTier = Secondary
  override val values: List[DiagnosisTier] = List(Principal, Admit, Secondary)
  override protected def name(t: DiagnosisTier): String = t.name
}
