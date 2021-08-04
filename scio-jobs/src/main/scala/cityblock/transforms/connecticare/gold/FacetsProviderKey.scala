package cityblock.transforms.connecticare.gold

import cityblock.models.connecticare.silver.FacetsProvider.ParsedFacetsProvider
import cityblock.transforms.Transform.{Key, UUIDComparable}

sealed case class FacetsProviderKey(num: String) extends Key with UUIDComparable {
  override protected val elements: List[String] = List(partner, num)
}

object FacetsProviderKey {
  def apply(provider: ParsedFacetsProvider): FacetsProviderKey =
    new FacetsProviderKey(provider.Tru_Prov_Nbr + provider.Tru_Prov_Location)
}
