package cityblock.transforms

import cityblock.models.Surrogate
import cityblock.models.gold.Claims.{Diagnosis, Procedure}
import cityblock.models.gold.FacilityClaim.HeaderProcedure
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.transforms.Transform.CodeSet
import org.scalatest.{FlatSpec, Matchers}

class TransformTest extends FlatSpec with Matchers {
  def dx(tier: DiagnosisTier, codeset: CodeSet.Value, code: String): Diagnosis =
    Diagnosis(surrogate = Surrogate("", "", "", ""),
              tier = tier.name,
              codeset = codeset.toString,
              code = code)

  def px(tier: ProcedureTier, codeset: CodeSet.Value, code: String): Procedure =
    Procedure(surrogate = Surrogate("", "", "", ""),
              tier = tier.name,
              codeset = codeset.toString,
              code = code,
              modifiers = List())

  def headerPx(tier: ProcedureTier, codeset: CodeSet.Value, code: String): HeaderProcedure =
    HeaderProcedure(
      surrogate = Surrogate("", "", "", ""),
      tier = tier.name,
      codeset = codeset.toString,
      code = code
    )

  "uniqueDiagnoses()" should "select correct diagnosis when multiple are present with same code/codeset" in {
    val dxs = Seq(
      dx(DiagnosisTier.Principal, CodeSet.ICD10Cm, "foo"),
      dx(DiagnosisTier.Admit, CodeSet.ICD10Cm, "foo"),
      dx(DiagnosisTier.Secondary, CodeSet.ICD10Cm, "foo"),
      dx(DiagnosisTier.Principal, CodeSet.ICD10Cm, "bar"),
      dx(DiagnosisTier.Secondary, CodeSet.ICD10Cm, "bar"),
      dx(DiagnosisTier.Secondary, CodeSet.ICD10Cm, "bar"),
      dx(DiagnosisTier.Admit, CodeSet.ICD10Cm, "baz"),
      dx(DiagnosisTier.Secondary, CodeSet.ICD9Cm, "baz"),
      dx(DiagnosisTier.Secondary, CodeSet.ICD10Cm, "baz"),
      dx(DiagnosisTier.Secondary, CodeSet.ICD10Cm, "baz")
    )

    val unique = Transform.uniqueDiagnoses(dxs)
    unique.size shouldBe 4
    unique should contain(dx(DiagnosisTier.Principal, CodeSet.ICD10Cm, "foo"))
    unique should contain(dx(DiagnosisTier.Principal, CodeSet.ICD10Cm, "bar"))
    unique should contain(dx(DiagnosisTier.Admit, CodeSet.ICD10Cm, "baz"))
    unique should contain(dx(DiagnosisTier.Secondary, CodeSet.ICD9Cm, "baz"))
  }

  "uniqueProcedures()" should "select correct procedures when multiple are present with same code/codeset" in {
    val pxs = Seq(
      px(ProcedureTier.Principal, CodeSet.ICD10Pcs, "foo"),
      px(ProcedureTier.Secondary, CodeSet.ICD10Pcs, "foo"),
      px(ProcedureTier.Principal, CodeSet.ICD10Pcs, "bar"),
      px(ProcedureTier.Secondary, CodeSet.ICD10Pcs, "bar"),
      px(ProcedureTier.Secondary, CodeSet.ICD10Pcs, "bar"),
      px(ProcedureTier.Secondary, CodeSet.ICD9Pcs, "baz"),
      px(ProcedureTier.Secondary, CodeSet.ICD10Pcs, "baz"),
      px(ProcedureTier.Secondary, CodeSet.ICD10Pcs, "baz")
    )

    val unique = Transform.uniqueProcedures(pxs)
    unique.size shouldBe 4
    unique should contain(headerPx(ProcedureTier.Principal, CodeSet.ICD10Pcs, "foo"))
    unique should contain(headerPx(ProcedureTier.Principal, CodeSet.ICD10Pcs, "bar"))
    unique should contain(headerPx(ProcedureTier.Secondary, CodeSet.ICD10Pcs, "baz"))
    unique should contain(headerPx(ProcedureTier.Secondary, CodeSet.ICD9Pcs, "baz"))
  }

}
