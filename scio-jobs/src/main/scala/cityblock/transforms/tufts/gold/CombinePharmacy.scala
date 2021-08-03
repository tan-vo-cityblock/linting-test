package cityblock.transforms.tufts.gold

import cityblock.models.gold.Amount
import cityblock.models.gold.PharmacyClaim.Pharmacy
import cityblock.transforms.Transform
import com.spotify.scio.values.SCollection

case class CombinePharmacy(
  historicalPharmacy: SCollection[Pharmacy],
  newPharmacy: SCollection[Pharmacy]
) {
  def combine(): SCollection[Pharmacy] =
    CombinePharmacy.combine(historicalPharmacy, newPharmacy)
}

object CombinePharmacy {

  def combine(
    historicalPharmacy: SCollection[Pharmacy],
    newPharmacy: SCollection[Pharmacy]
  ): SCollection[Pharmacy] = {

    val allPharmacies =
      (historicalPharmacy ++ newPharmacy).groupBy(_.drug.partnerPrescriptionNumber)
    val allPharmaciesAndFills = allPharmacies.map(_._2.groupBy(_.drug.fillNumber)).map(_.values)
    allPharmaciesAndFills.flatMap(pharmaciesByRx => {
      pharmaciesByRx.flatMap(pharmaciesByFill => {
        val sorted = pharmaciesByFill.toList.sortBy(_.date.paid)(
          Transform.NoneMinOptionLocalDateOrdering.reverse)
        val mostRecent = sorted.headOption

        val linesByRefill = pharmaciesByFill.toList

        val allAllowedAmounts: BigDecimal = linesByRefill.flatMap(_.amount.allowed).sum
        val allBilledAmounts: BigDecimal = linesByRefill.flatMap(_.amount.billed).sum
        val allCOBAmounts: BigDecimal = linesByRefill.flatMap(_.amount.cob).sum
        val allCopayAmounts: BigDecimal = linesByRefill.flatMap(_.amount.copay).sum
        val allDeductibleAmounts: BigDecimal = linesByRefill.flatMap(_.amount.deductible).sum
        val allCoinsuranceAmounts: BigDecimal = linesByRefill.flatMap(_.amount.coinsurance).sum
        val allPlanPaidAmounts: BigDecimal = linesByRefill.flatMap(_.amount.planPaid).sum

        val amountsPerRefill = Amount(
          Some(allAllowedAmounts),
          Some(allBilledAmounts),
          Some(allCOBAmounts),
          Some(allCopayAmounts),
          Some(allDeductibleAmounts),
          Some(allCoinsuranceAmounts),
          Some(allPlanPaidAmounts)
        )

        mostRecent.map(_.copy(amount = amountsPerRefill))
      })
    })
  }
}
