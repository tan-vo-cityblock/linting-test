package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.Medical
import cityblock.utilities.{reference, Strings}
import cityblock.utilities.reference.ValidatableSCollectionFunctions._
import cityblock.utilities.reference.tables.{DiagnosisRelatedGroup, PlaceOfService, RevenueCode}
import com.spotify.scio.values.SCollection

class MedicalValidator(drgCodes: SCollection[DiagnosisRelatedGroup.Valid],
                       revenueCodes: SCollection[RevenueCode.Valid],
                       placeOfServiceCodes: SCollection[PlaceOfService.Valid],
                       typeOfBillCodes: SCollection[reference.tables.TypeOfBill.Valid]) {

  def validate(lines: SCollection[Medical]): SCollection[Medical] =
    lines
      .map(MedicalValidator.normalizeRevenueCode)
      .map(MedicalValidator.normalizeDiagnosisRelatedGroup)
      .map(MedicalValidator.normalizePlaceOfService)
      .map(MedicalValidator.normalizeTypeOfBill)
      .nullifyInvalidCodes(drgCodes,
                           _.medical.DRG,
                           m => m.copy(medical = m.medical.copy(DRG = None)))
      .nullifyInvalidCodes(placeOfServiceCodes,
                           _.medical.Location,
                           m => m.copy(medical = m.medical.copy(Location = None)))
      .nullifyInvalidCodes(
        revenueCodes,
        m => if (m.medical.Proc1Type.contains(ProcType.REV.toString)) m.medical.Proc1 else None,
        m => m.copy(medical = m.medical.copy(Proc1 = None)))
      .nullifyInvalidCodes(
        revenueCodes,
        m => if (m.medical.Proc2Type.contains(ProcType.REV.toString)) m.medical.Proc2 else None,
        m => m.copy(medical = m.medical.copy(Proc2 = None)))
}

object MedicalValidator {
  def normalizeRevenueCode(line: Medical): Medical = {
    val proc1 =
      if (line.medical.Proc1Type.contains(ProcType.REV.toString)) {
        line.medical.Proc1.map(Strings.zeroPad(_, RevenueCode.padTo))
      } else { None }
    val proc2 =
      if (line.medical.Proc2Type.contains(ProcType.REV.toString)) {
        line.medical.Proc2.map(Strings.zeroPad(_, RevenueCode.padTo))
      } else { None }

    if (proc1.isDefined || proc2.isDefined) {
      line.copy(medical = line.medical.copy(Proc1 = proc1, Proc2 = proc2))
    } else {
      line
    }
  }

  def normalizeDiagnosisRelatedGroup(line: Medical): Medical = {
    val normalized = line.medical.DRG.map(Strings.zeroPad(_, DiagnosisRelatedGroup.padTo))
    line.copy(medical = line.medical.copy(DRG = normalized))
  }

  def normalizePlaceOfService(line: Medical): Medical = {
    val normalized = line.medical.Location.map(Strings.zeroPad(_, PlaceOfService.padTo))
    line.copy(medical = line.medical.copy(Location = normalized))
  }

  def normalizeTypeOfBill(line: Medical): Medical = {
    val normalized =
      line.medical.BillTypeCd.map(Strings.zeroPad(_, reference.tables.TypeOfBill.padTo))
    line.copy(medical = line.medical.copy(BillTypeCd = normalized))
  }
}
