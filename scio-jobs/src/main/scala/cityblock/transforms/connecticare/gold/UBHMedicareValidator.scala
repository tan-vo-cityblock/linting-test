package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.UBHMedicare
import cityblock.utilities.{reference, Strings}
import cityblock.utilities.reference.ValidatableSCollectionFunctions._
import cityblock.utilities.reference.tables.{DiagnosisRelatedGroup, PlaceOfService, RevenueCode}
import com.spotify.scio.values.SCollection

class UBHMedicareValidator(drgCodes: SCollection[DiagnosisRelatedGroup.Valid],
                           revenueCodes: SCollection[RevenueCode.Valid],
                           placeOfServiceCodes: SCollection[PlaceOfService.Valid],
                           typeOfBillCodes: SCollection[reference.tables.TypeOfBill.Valid]) {

  def validate(lines: SCollection[UBHMedicare]): SCollection[UBHMedicare] =
    lines
      .map(UBHMedicareValidator.normalizeRevenueCode)
      .map(UBHMedicareValidator.normalizeDiagnosisRelatedGroup)
      .map(UBHMedicareValidator.normalizePlaceOfService)
      .map(UBHMedicareValidator.normalizeTypeOfBill)
      .nullifyInvalidCodes(drgCodes, _.claim.DRG, m => m.copy(claim = m.claim.copy(DRG = None)))
      .nullifyInvalidCodes(placeOfServiceCodes,
                           _.claim.POS,
                           m => m.copy(claim = m.claim.copy(POS = None)))
      .nullifyInvalidCodes(revenueCodes,
                           _.claim.REV_CD,
                           u => u.copy(claim = u.claim.copy(REV_CD = None)))

}

object UBHMedicareValidator {
  def normalizeRevenueCode(line: UBHMedicare): UBHMedicare = {
    val normalized = line.claim.REV_CD.map(Strings.zeroPad(_, RevenueCode.padTo))
    line.copy(claim = line.claim.copy(REV_CD = normalized))
  }

  def normalizeDiagnosisRelatedGroup(line: UBHMedicare): UBHMedicare = {
    val normalized = line.claim.DRG.map(Strings.zeroPad(_, DiagnosisRelatedGroup.padTo))
    line.copy(claim = line.claim.copy(DRG = normalized))
  }

  def normalizePlaceOfService(line: UBHMedicare): UBHMedicare = {
    val normalized = line.claim.POS.map(Strings.zeroPad(_, PlaceOfService.padTo))
    line.copy(claim = line.claim.copy(POS = normalized))
  }

  def normalizeTypeOfBill(line: UBHMedicare): UBHMedicare = {
    val normalized =
      line.claim.BILL.map(Strings.zeroPad(_, reference.tables.TypeOfBill.padTo))
    line.copy(claim = line.claim.copy(BILL = normalized))
  }
}
