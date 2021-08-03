package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.Medical
import cityblock.models.Surrogate
import cityblock.transforms.Transform.CodeSet
import cityblock.utilities.CCISilverClaimsMocks.professional
import org.scalatest.{FlatSpec, Matchers}

class MedicalMappingTest extends FlatSpec with Matchers with MedicalMapping {
  "chooseFirstProcedure()" should "work" in {
    def mkMock(Proc1: Option[String],
               Proc1Type: Option[String],
               Proc2: Option[String],
               Proc2Type: Option[String]): Medical = {
      val mock = professional()
      mock.copy(
        medical = mock.medical.copy(
          Proc1 = Proc1,
          Proc2 = Proc2,
          Proc1Type = Proc1Type,
          Proc2Type = Proc2Type
        ))
    }

    val mocks = Array(
      mkMock(Some("code"), Some("CPT"), None, None),
      mkMock(None, None, Some("code"), Some("CPT")),
      mkMock(None, None, None, None),
      mkMock(None, None, Some("code"), Some("REV")),
      mkMock(Some("code1"), Some("CPT"), Some("code2"), Some("CPT")),
      mkMock(Some("code1"), Some("HCPC"), Some("code2"), Some("REV"))
    )

    val expected = Set(
      (mocks(0).identifier.surrogateId, "code", CodeSet.CPT.toString),
      (mocks(1).identifier.surrogateId, "code", CodeSet.CPT.toString),
      (mocks(4).identifier.surrogateId, "code1", CodeSet.CPT.toString),
      (mocks(5).identifier.surrogateId, "code1", CodeSet.HCPCS.toString)
    )

    val result = mocks
      .flatMap { m =>
        chooseFirstProcedure(Surrogate(m.identifier.surrogateId, "", "", ""), m)
      }
      .map(p => (p.surrogate.id, p.code, p.codeset))
      .toSet

    result should equal(expected)
  }
}
