package cityblock.transforms.healthyblue.gold

import java.util.UUID

import cityblock.models.HealthyBlueSilverClaims.{SilverProfessionalHeader, SilverProfessionalLine}
import cityblock.models.gold.Claims.{Diagnosis, Procedure}
import cityblock.models.gold.ProfessionalClaim._
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.models.gold.{Amount, Constructors, ProviderIdentifier}
import cityblock.models.healthyblue.silver.ProfessionalHeader.ParsedProfessionalHeader
import cityblock.models.healthyblue.silver.ProfessionalLine.ParsedProfessionalLine
import cityblock.models.{SilverIdentifier, Surrogate}
import cityblock.transforms.Transform
import cityblock.utilities.Conversions
import cityblock.utilities.Insurance.LineOfBusiness
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object ProfessionalTransformer {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val bigqueryProject: String = args.required("bigqueryProject")

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val deliveryDate: LocalDate = LocalDate.parse(args.required("deliveryDate"), dtf)

    val silverDataset = args.required("silverDataset")
    val silverHeaderTable = args.required("silverHeaderTable")
    val silverLinesTable = args.required("silverLinesTable")

    val goldDataset = args.required("goldDataset")
    val goldTable = args.required("goldTable")

    val silverProfessionalHeaders: SCollection[SilverProfessionalHeader] =
      Transform.fetchFromBigQuery[SilverProfessionalHeader](
        sc,
        bigqueryProject,
        silverDataset,
        silverHeaderTable,
        deliveryDate
      )

    val silverProfessionalLines: SCollection[SilverProfessionalLine] =
      Transform.fetchFromBigQuery[SilverProfessionalLine](
        sc,
        bigqueryProject,
        silverDataset,
        silverLinesTable,
        deliveryDate
      )

    val joinedHeaderLines
      : SCollection[(SilverProfessionalHeader, Option[Iterable[SilverProfessionalLine]])] =
      joinHeaderToLines(sc,
                        dedupKeySilverHeaders(silverProfessionalHeaders),
                        silverProfessionalLines)

    val goldProfessional: SCollection[Professional] =
      transform(sc,
                joinedHeaderLines,
                bigqueryProject,
                silverDataset,
                silverHeaderTable,
                silverLinesTable)

    Transform.persist(
      goldProfessional,
      bigqueryProject,
      goldDataset,
      goldTable,
      deliveryDate,
      WRITE_TRUNCATE,
      CREATE_IF_NEEDED
    )

    sc.close().waitUntilFinish()
  }

  def dedupKeySilverHeaders(silverHeaders: SCollection[SilverProfessionalHeader])
    : SCollection[(String, SilverProfessionalHeader)] =
    silverHeaders
      .withName("Remove duplicate rows")
      .distinctBy {
        _.copy(identifier = SilverIdentifier("", None))
      }
      .withName("Key by transaction ID")
      .keyBy(_.data.Transaction_Control_Number)

  def joinHeaderToLines(sc: ScioContext,
                        silverHeaders: SCollection[(String, SilverProfessionalHeader)],
                        silverLines: SCollection[SilverProfessionalLine])
    : SCollection[(SilverProfessionalHeader, Option[Iterable[SilverProfessionalLine]])] =
    silverHeaders
      .withName("Join with grouped professional lines")
      .leftOuterJoin(silverLines.groupBy(_.data.Transaction_Control_Number))
      .values

  def transform(sc: ScioContext,
                silverHeaderLines: SCollection[(SilverProfessionalHeader,
                                                Option[Iterable[SilverProfessionalLine]])],
                bqProject: String,
                silverDataset: String,
                silverHeaderTable: String,
                silverLinesTable: String): SCollection[Professional] =
    silverHeaderLines
      .withName("Add header surrogates")
      .map {
        Transform.addSurrogate(
          bqProject,
          silverDataset,
          silverHeaderTable,
          _
        )(_._1.identifier.surrogateId)
      }
      .withName("Create gold professional claims")
      .map {
        case (headerSurrogate, (silverHeader, maybeSilverLines)) =>
          Professional(
            claimId = mkClaimId(silverHeader),
            memberIdentifier =
              Transform.mkMemberIdentifierForPatient(silverHeader.patient, "healthyblue"),
            header = mkHeader(headerSurrogate, silverHeader),
            lines =
              mkLines(bqProject, silverDataset, silverLinesTable, silverHeader, maybeSilverLines)
          )
      }

  private def mkClaimId(silverHeader: SilverProfessionalHeader): String =
    UUID
      .nameUUIDFromBytes(
        ("healthyblue" + silverHeader.data.Subscriber_Primary_Identifier + silverHeader.data.Transaction_Control_Number).getBytes
      )
      .toString

  private def mkHeader(surrogate: Surrogate, silverHeader: SilverProfessionalHeader): Header =
    Header(
      partnerClaimId = silverHeader.data.Transaction_Control_Number,
      lineOfBusiness = Some(LineOfBusiness.Medicaid.toString),
      subLineOfBusiness = None, // TODO figure this out lol
      provider = HeaderProvider(
        billing = mkClaimProvider(
          silverHeader.data.Billing_Provider_Identifier,
          silverHeader.data.Provider_Taxonomy_Code), // TODO translate from taxonomy to specialty?
        referring = mkClaimProvider(silverHeader.data.Referring_Provider_Identifier, None),
      ),
      diagnoses = mkHeaderDiagnoses(surrogate, silverHeader.data)
    )

  private def mkClaimProvider(idField: Option[String],
                              specialtyField: Option[String]): Option[ProviderIdentifier] =
    for (id <- idField)
      yield
        ProviderIdentifier(
          id = Transform.mkIdentifier(id),
          specialty = specialtyField
        )

  private def mkHeaderDiagnoses(surrogate: Surrogate,
                                data: ParsedProfessionalHeader): List[Diagnosis] = {
    val secondaryDiagnoses: List[Diagnosis] = List(
      data.Diagnosis_Code_02,
      data.Diagnosis_Code_03,
      data.Diagnosis_Code_04,
      data.Diagnosis_Code_05,
      data.Diagnosis_Code_06,
      data.Diagnosis_Code_07,
      data.Diagnosis_Code_08,
      data.Diagnosis_Code_09,
      data.Diagnosis_Code_10,
      data.Diagnosis_Code_11,
      data.Diagnosis_Code_12
    ).flatMap(mkDiagnosis(surrogate, _, DiagnosisTier.Secondary.name))

    secondaryDiagnoses ++ mkDiagnosis(surrogate,
                                      data.Diagnosis_Code_01,
                                      DiagnosisTier.Principal.name)
  }

  private def mkDiagnosis(surrogate: Surrogate,
                          maybeCode: Option[String],
                          tier: String): Option[Diagnosis] =
    maybeCode.map { code =>
      Diagnosis(
        surrogate: Surrogate,
        tier = tier,
        codeset = Transform.determineDiagnosisCodeset(code),
        code = code
      )
    }

  private def mkLines(bqProject: String,
                      silverDataset: String,
                      silverLinesTable: String,
                      header: SilverProfessionalHeader,
                      maybeSilverLines: Option[Iterable[SilverProfessionalLine]]): List[Line] = {
    val surrogateLines = maybeSilverLines match {
      case Some(lines) =>
        lines.map {
          Transform.addSurrogate(
            bqProject,
            silverDataset,
            silverLinesTable,
            _
          )(_.identifier.surrogateId)
        }
      case _ => List.empty
    }

    surrogateLines.map {
      case (surrogate, line) =>
        Line(
          surrogate = surrogate,
          lineNumber = mkLineNumber(line.data.Line_Number),
          cobFlag = None, // TODO ask HB
          capitatedFlag = None, // TODO ask HB
          claimLineStatus = Some(mkClaimLineStatus(line.data.Line_Status_Code)),
          inNetworkFlag = None, // TODO ask HB
          serviceQuantity = line.data.Quantity.flatMap(Conversions.safeParse(_, _.toInt)),
          placeOfService = line.data.Place_of_Service_Code,
          date = mkDate(header.data, line.data),
          provider = LineProvider(
            servicing = mkClaimProvider(line.data.Rendering_Provider_Identifier, None)),
          procedure = mkProcedure(surrogate, line.data),
          amount = mkAmount(line.data),
          diagnoses = mkLineDiagnoses(surrogate, header.data, line.data),
          typesOfService = List.empty
        )
    }.toList
  }

  private def mkDate(header: ParsedProfessionalHeader,
                     line: ParsedProfessionalLine): ProfessionalDate =
    ProfessionalDate(
      from = line.Service_Date,
      to = line.Last_Seen_Date,
      paid = header.Date_of_Payment
    )

  private def mkProcedure(surrogate: Surrogate, line: ParsedProfessionalLine): Option[Procedure] = {
    val tier = if (line.Line_Number.contains("1")) {
      ProcedureTier.Principal
    } else {
      ProcedureTier.Secondary
    }

    Transform.mkProcedure(
      surrogate,
      line,
      tier,
      (s: ParsedProfessionalLine) => s.Procedure_Code,
      (s: ParsedProfessionalLine) =>
        (s.Procedure_Code ++ s.Procedure_Code2 ++ s.Procedure_Code3).toList
    )
  }

  private def mkAmount(line: ParsedProfessionalLine): Amount =
    Constructors.mkAmount(
      allowed = line.Claim_Line_Allowed_Amount,
      billed = line.Total_Claim_Line_Charge_Amount,
      COB = None, // TODO ask HB
      copay = None, // TODO ask HB
      deductible = None, // TODO ask HB
      coinsurance = None, // TODO ask HB
      planPaid = line.Payers_Claim_Line_Payment_Amount
    )

  private def mkLineDiagnoses(surrogate: Surrogate,
                              header: ParsedProfessionalHeader,
                              line: ParsedProfessionalLine): List[Diagnosis] =
    List(line.Diagnosis_Code_Pointer_01,
         line.Diagnosis_Code_Pointer_02,
         line.Diagnosis_Code_Pointer_03,
         line.Diagnosis_Code_Pointer_04).flatten.flatMap { pointer =>
      pointer match {
        case "1" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_01, DiagnosisTier.Principal.name)
        case "2" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_02, DiagnosisTier.Secondary.name)
        case "3" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_03, DiagnosisTier.Secondary.name)
        case "4" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_04, DiagnosisTier.Secondary.name)
        case "5" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_05, DiagnosisTier.Secondary.name)
        case "6" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_06, DiagnosisTier.Secondary.name)
        case "7" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_07, DiagnosisTier.Secondary.name)
        case "8" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_08, DiagnosisTier.Secondary.name)
        case "9" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_09, DiagnosisTier.Secondary.name)
        case "10" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_10, DiagnosisTier.Secondary.name)
        case "11" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_11, DiagnosisTier.Secondary.name)
        case "12" =>
          mkDiagnosis(surrogate, header.Diagnosis_Code_12, DiagnosisTier.Secondary.name)
        case _ => None
      }
    }
}
