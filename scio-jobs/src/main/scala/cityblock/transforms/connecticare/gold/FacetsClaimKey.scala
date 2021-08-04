package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.FacetsMedical
import cityblock.transforms.Transform.{Key, UUIDComparable}

sealed case class FacetsClaimKey(silver: FacetsMedical) extends Key with UUIDComparable {
  override protected val elements: List[String] =
    List(partner, silver.patient.externalId, mkFacetsPartnerClaimId(silver))
}
