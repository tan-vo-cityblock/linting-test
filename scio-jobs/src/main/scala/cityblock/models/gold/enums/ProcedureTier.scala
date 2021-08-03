package cityblock.models.gold.enums

sealed abstract class ProcedureTier(val name: String) extends Enum

object ProcedureTier extends CanConvertFromString[ProcedureTier] {
  case object Principal extends ProcedureTier("principal")
  case object Secondary extends ProcedureTier("secondary")

  override val default: ProcedureTier = Secondary
  override val values: List[ProcedureTier] = List(Principal, Secondary)
  override protected def name(t: ProcedureTier): String = t.name
}
