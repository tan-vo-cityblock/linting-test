package cityblock.transforms.tufts

import cityblock.transforms.Transform.{Key, UUIDComparable}
import cityblock.transforms.tufts.gold.partner

sealed case class ProviderKey(providerId: String) extends Key with UUIDComparable {
  override protected val elements: List[String] = List(providerId, partner)
}
