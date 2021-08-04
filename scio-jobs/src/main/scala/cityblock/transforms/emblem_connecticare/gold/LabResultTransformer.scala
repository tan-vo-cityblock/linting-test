package cityblock.transforms.emblem_connecticare.gold

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.EmblemConnecticareSilverClaims.SilverLabResult
import cityblock.models.emblem_connecticare.silver.LabResult.ParsedLabResult
import cityblock.models.{Identifier, Surrogate}
import cityblock.models.gold.Claims.{LabResult, MemberIdentifier, Procedure}
import cityblock.models.gold.enums.ProcedureTier
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet
import cityblock.utilities.Conversions
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object LabResultTransformer {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val bigqueryProject: String = args.required("bigqueryProject")

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val deliveryDate: LocalDate = LocalDate.parse(args.required("deliveryDate"), dtf)

    val silverDataset = args.required("silverDataset")
    val silverTable = args.required("silverTable")

    val goldDataset = args.required("goldDataset")
    val goldTable = args.required("goldTable")

    val labResults: SCollection[SilverLabResult] =
      Transform.fetchFromBigQuery[SilverLabResult](
        sc,
        bigqueryProject,
        silverDataset,
        silverTable,
        deliveryDate
      )

    val goldLabResults = labResults
      .withName("Add surrogates")
      .map {
        Transform.addSurrogate(
          bigqueryProject,
          silverDataset,
          silverTable,
          _
        )(_.identifier.surrogateId)
      }
      .withName("Construct lab results")
      .map {
        case (surrogate, silver) =>
          LabResultTransformer.labResult(surrogate, silver)
      }

    Transform.persist(
      goldLabResults,
      bigqueryProject,
      goldDataset,
      goldTable,
      deliveryDate,
      WRITE_TRUNCATE,
      CREATE_IF_NEEDED
    )

    sc.close().waitUntilFinish()
  }

  private def labResult(surrogate: Surrogate, silver: SilverLabResult): LabResult =
    LabResult(
      identifier = mkIdentifier(surrogate, silver),
      memberIdentifier = mkMemberIdentifier(silver.patient),
      name = silver.data.LCL_RSLT_NM,
      result = result(silver.data),
      resultNumeric = resultNumeric(silver.data),
      units = silver.data.RSLT_UNIT,
      loinc = silver.data.LCL_INCDT_CD,
      date = silver.data.SVC_DT,
      procedure = procedure(surrogate, silver.data)
    )

  private def mkIdentifier(surrogate: Surrogate, silver: SilverLabResult): Identifier =
    Identifier(
      id = silver.data.EDM_LAB_RESULT_KEY,
      partner = mapPayerName(silver.data.PARENT_COMPANY_CD),
      surrogate = surrogate
    )

  private def mapPayerName(companyCode: Option[String]): String =
    companyCode match {
      case Some("HIP") => "emblem"
      case Some("CCI") => "connecticare"
      case _           => "Unknown"
    }

  private def mkMemberIdentifier(member: Patient): MemberIdentifier =
    MemberIdentifier(
      commonId = None,
      partnerMemberId = member.externalId,
      patientId = member.patientId,
      partner = member.source.name
    )

  private def result(result: ParsedLabResult): Option[String] =
    result match {
      case r if r.LCL_RSLT_VAL_2.nonEmpty => r.LCL_RSLT_VAL_2
      case r if r.LCL_RSLT_VAL_1.nonEmpty => r.LCL_RSLT_VAL_1
      case _                              => None
    }

  private def resultNumeric(result: ParsedLabResult): Option[BigDecimal] =
    result.LCL_RSLT_VAL_1.flatMap(Conversions.safeParse(_, BigDecimal(_)))

  private def procedure(surrogate: Surrogate, result: ParsedLabResult): Option[Procedure] =
    for (code <- result.CURR_PROC_TRMLGY_CD)
      yield
        Procedure(
          surrogate = surrogate,
          tier = ProcedureTier.Secondary.name,
          codeset = CodeSet.CPT.toString,
          code = code,
          modifiers = List()
        )

}
