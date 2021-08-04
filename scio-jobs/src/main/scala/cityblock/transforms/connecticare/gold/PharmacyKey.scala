package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.{FacetsPharmacyMed, Pharmacy}
import cityblock.transforms.Transform.{Key, UUIDComparable}

sealed case class PharmacyKey(
  num: String,
  patientId: Option[String],
  fillDate: Option[String]
) extends Key
    with UUIDComparable {
  override protected val elements: List[String] =
    List(Some(partner), Some(num), patientId, fillDate).flatten
}

object PharmacyKey {
  def apply(claim: FacetsPharmacyMed): PharmacyKey =
    PharmacyKey(claim.data.claim_number, None, None)
  def apply(claim: Pharmacy): PharmacyKey = PharmacyKey(
    claim.pharmacy.ClmNum,
    Some(claim.patient.externalId),
    claim.pharmacy.DateFill.map(_.toString)
  )
}
