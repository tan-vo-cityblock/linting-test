package cityblock.transforms.carefirst.gold

import cityblock.models.CarefirstSilverClaims.SilverProvider
import cityblock.transforms.Transform.{Key, UUIDComparable}

sealed case class ProviderKey(id: String) extends Key with UUIDComparable {
  override protected val elements: List[String] = List(partner, id)
}

object ProviderKey {
  def apply(provider: SilverProvider): ProviderKey =
    ProviderKey(provider.data.PROV_ID)
}
