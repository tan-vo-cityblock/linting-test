package cityblock.transforms.tufts

import cityblock.models.TuftsSilverClaims.{MassHealth, Medical}
import cityblock.utilities.PartnerConfiguration

package object gold {
  val partner: String = PartnerConfiguration.tufts.indexName

  def isFacility(silverClaims: Iterable[Medical]): Boolean =
    silverClaims.exists(silver => {
      silver.data.Revenue_Code.isDefined || silver.data.Type_of_Bill_on_Facility_Claims.isDefined
    })

  def isProfessional(silverClaims: Iterable[Medical]): Boolean =
    !isFacility(silverClaims)

  def isMassHealthFacility(massHealthSilverClaims: Iterable[MassHealth]): Boolean =
    massHealthSilverClaims.exists(masshealth => {
      masshealth.data.CDE_REVENUE.isDefined || masshealth.data.CDE_TYPE_OF_BILL.isDefined
    })
  def isMassHealthProfessional(massHealthSilverClaims: Iterable[MassHealth]): Boolean =
    !isMassHealthFacility(massHealthSilverClaims)
}
