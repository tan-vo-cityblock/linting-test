package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.Medical
import com.spotify.scio.testing.PipelineSpec
import cityblock.utilities.{CCISilverClaimsMocks => Mocks}

class IsFacilityTest extends PipelineSpec {

  "facility claims" should "be marked as facility" in {
    def test(
      billTypeCd1: Option[String],
      billTypeCd2: Option[String],
      proc1Type1: Option[String],
      proc1Type2: Option[String]
    ): Boolean = {
      val facilityClaims: List[Medical] = {
        List(
          Mocks.facility(BillTypeCd = billTypeCd1, Proc1Type = proc1Type1),
          Mocks.facility(BillTypeCd = billTypeCd2, Proc1Type = proc1Type2)
        )
      }
      isFacility(facilityClaims)
    }

    test(None, None, None, None) shouldBe false
    test(None, Some("BillTypeCode"), None, None) shouldBe true
    test(Some("BillTypeCode"), None, None, None) shouldBe true
    test(Some("BillTypeCode1"), Some("BillTypeCode2"), None, None) shouldBe true
    test(None, None, Some("REV"), None) shouldBe true
    test(None, None, None, Some("REV")) shouldBe true
  }
}
