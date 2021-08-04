package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.SilverProvider
import cityblock.transforms.Transform.{Key, UUIDComparable}

sealed case class ProviderKey(id: String, suffix: Option[String]) extends Key with UUIDComparable {
  override protected val elements: List[String] = List(partner, id) ++ suffix
}

object ProviderKey {
  def apply(provider: SilverProvider): ProviderKey =
    ProviderKey(provider.provider.PROV_ID, provider.provider.PROV_LOC)
  def apply(id: String): ProviderKey = ProviderKey(id, None)
}
