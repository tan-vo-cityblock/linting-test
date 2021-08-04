package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.{ProviderAddress, ProviderDetail}
import cityblock.transforms.Transform.{Key, UUIDComparable}

/**
 * A provider id that is guaranteed to be unique across partners.
 *
 * We extend [[UUIDComparable]] to standardize the construction of a [[java.util.UUID]].
 * @param num The CCI-specific provider id, usually given by a field suffixed with `ProvNum`.
 */
sealed case class ProviderKey(num: String) extends Key with UUIDComparable {
  override protected val elements: List[String] = List(partner, num)
}

object ProviderKey {
  def apply(num: String): ProviderKey = new ProviderKey(num)
  def apply(detail: ProviderDetail): ProviderKey =
    new ProviderKey(detail.detail.ProvNum)
  def apply(address: ProviderAddress): ProviderKey =
    new ProviderKey(address.address.ProvNum)
}
