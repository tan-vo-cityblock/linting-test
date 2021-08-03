package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.Medical
import com.spotify.scio.testing.PipelineSpec
import cityblock.utilities.{CCISilverClaimsMocks => Mocks}

class TypeOfBillTest extends PipelineSpec {
  def setBillTypeCd(m: Medical, c: Option[String]): Medical =
    m.copy(medical = m.medical.copy(BillTypeCd = c))

  def setLocation(m: Medical, l: Option[String]): Medical =
    m.copy(medical = m.medical.copy(Location = l))

  "UB-04 bill type codes" should "be correctly zero-padded" in {
    def test(BillTypeCd: Option[String]): Option[String] = {
      val claim = setBillTypeCd(Mocks.facility(), BillTypeCd)
      TypeOfBill.get(List(claim))
    }

    test(None) shouldBe None
    test(Some("123")) shouldBe Some("0123")
    test(Some("0123")) shouldBe Some("0123")
    test(Some("")) shouldBe Some("0000")
    test(Some("0INV")) shouldBe Some("0INV")
  }

  they should "correctly impute from Location" in {
    def test(pairs: (Option[String], Option[String])*): Option[String] = {
      val claims = pairs.map {
        case (code, location) =>
          val tmp = setBillTypeCd(Mocks.facility(), code)
          setLocation(tmp, location)
      }
      TypeOfBill.get(claims.toList)
    }

    test((None, None)) shouldBe None
    test((None, Some("not a real code"))) shouldBe None
    test((None, Some("24"))) should contain("0831")
    test((None, None), (None, Some("24"))) should contain("0831")
    test((None, Some("24")), (None, None)) should contain("0831")
    test((None, Some("not a real code")), (None, Some("24"))) should contain("0831")
    test((None, Some("24")), (None, Some("not a real code"))) should contain("0831")
  }
}
