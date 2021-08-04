package cityblock.transforms.healthyblue.gold

import cityblock.models.HealthyBlueSilverClaims.{SilverFacilityHeader, SilverFacilityLine}
import cityblock.models.{SilverIdentifier, Surrogate}
import cityblock.models.gold.Claims.{Diagnosis, Procedure}
import cityblock.models.gold.{Constructors, ProviderIdentifier}
import cityblock.models.gold.FacilityClaim.{
  DRG,
  Date,
  Facility,
  Header,
  HeaderProcedure,
  HeaderProvider,
  Line
}
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.models.healthyblue.silver.Facility.{ParsedFacilityHeader, ParsedFacilityLine}
import cityblock.transforms.Transform
import cityblock.utilities.Conversions
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import com.spotify.scio.bigquery._

object FacilityTransformer {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val bigqueryProject: String = args.required("bigqueryProject")

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val deliveryDate: LocalDate = LocalDate.parse(args.required("deliveryDate"), dtf)

    val silverDataset = args.required("silverDataset")

    val goldDataset = args.required("goldDataset")
    val goldTable = args.required("goldTable")

    val silverFacilityLines: SCollection[SilverFacilityLine] =
      Transform
        .fetchFromBigQuery[SilverFacilityLine](
          sc,
          "cbh-healthyblue-data",
          silverDataset,
          "institutional_lines",
          deliveryDate
        )
        .withName("Dedup lines")
        .distinctBy {
          _.copy(identifier = SilverIdentifier("", None))
        }

    def headerQuery(tableName: String): String =
      s"""SELECT
        |identifier,
        |patient,
        |STRUCT(data.Transaction_Control_Number,
        |data.Provider_Taxonomy_Code,
        |data.Billing_Provider_Identifier,
        |data.Billing_Provider_Tax_Identification_Number,
        |data.Claim_Frequency_Code,
        |data.Discharge_Time,
        |data.Statement_From_Date,
        |data.Statement_To_Date,
        |data.Admission_Date,
        |data.Admission_Type_Code,
        |data.Admission_Source_Code,
        |data.Patient_Status_Code,
        |data.Principal_Diagnosis_Code,
        |data.Present_on_Admission_Indicator,
        |data.Admitting_Diagnosis_Code,
        |data.Diagnosis_Related_Group_DRG_Code,
        |data.Other_Diagnosis_01,
        |data.Other_Diagnosis_02,
        |data.Other_Diagnosis_03,
        |data.Other_Diagnosis_04,
        |data.Other_Diagnosis_05,
        |data.Other_Diagnosis_06,
        |data.Other_Diagnosis_07,
        |data.Other_Diagnosis_08,
        |data.Other_Diagnosis_09,
        |data.Other_Diagnosis_10,
        |data.Other_Diagnosis_11,
        |data.Other_Diagnosis_12,
        |data.Other_Diagnosis_13,
        |data.Other_Diagnosis_14,
        |data.Other_Diagnosis_15,
        |data.Other_Diagnosis_16,
        |data.Other_Diagnosis_17,
        |data.Other_Diagnosis_18,
        |data.Other_Diagnosis_19,
        |data.Other_Diagnosis_20,
        |data.Other_Diagnosis_21,
        |data.Other_Diagnosis_22,
        |data.Other_Diagnosis_23,
        |data.Other_Diagnosis_24,
        |data.Principal_Procedure_Code,
        |data.Procedure_Code_01,
        |data.Procedure_Code_02,
        |data.Procedure_Code_03,
        |data.Procedure_Code_04,
        |data.Procedure_Code_05,
        |data.Procedure_Code_06,
        |data.Procedure_Code_07,
        |data.Procedure_Code_08,
        |data.Procedure_Code_09,
        |data.Procedure_Code_10,
        |data.Procedure_Code_11,
        |data.Procedure_Code_12,
        |data.Procedure_Code_13,
        |data.Procedure_Code_14,
        |data.Procedure_Code_15,
        |data.Procedure_Code_16,
        |data.Procedure_Code_17,
        |data.Procedure_Code_18,
        |data.Procedure_Code_19,
        |data.Procedure_Code_20,
        |data.Procedure_Code_21,
        |data.Procedure_Code_22,
        |data.Procedure_Code_23,
        |data.Procedure_Code_24,
        |data.Attending_Provider_Primary_Identifier,
        |data.Provider_Taxonomy_Code_Attending,
        |data.Operating_Physician_Primary_Identifier,
        |data.Rendering_Provider_Identifier,
        |data.Referring_Provider_Identifier,
        |data.Date_of_Payment,
        |data.Facility_Type_Code) as data
        |FROM `cbh-healthyblue-data.silver_claims.$tableName`
        |""".stripMargin

    val silverFacilityHeaders: SCollection[SilverFacilityHeader] =
      sc.typedBigQuery[SilverFacilityHeader](
          headerQuery(
            tableName = Transform.shardName("institutional_header", deliveryDate)
          ))
        .withName("Dedup headers")
        .distinctBy {
          _.copy(identifier = SilverIdentifier("", None))
        }

    val (linesWithSurrogates, headerWithSurrogates) =
      addSurrogates(silverFacilityLines, silverFacilityHeaders)

    val goldFacility = mkFacility(linesWithSurrogates, headerWithSurrogates)

    Transform.persist(
      goldFacility,
      bigqueryProject,
      goldDataset,
      goldTable,
      deliveryDate,
      WRITE_TRUNCATE,
      CREATE_IF_NEEDED
    )

    sc.close().waitUntilFinish()
  }

  def addSurrogates(
    lines: SCollection[SilverFacilityLine],
    headers: SCollection[SilverFacilityHeader]
  ): (SCollection[(Surrogate, SilverFacilityLine)], SCollection[(Surrogate, SilverFacilityHeader)]) = {
    val linesWithSurrogates: SCollection[(Surrogate, SilverFacilityLine)] =
      lines.map {
        Transform.addSurrogate(
          "cbh-healthyblue-data",
          "silver_claims",
          "institutional_lines",
          _
        )(_.identifier.surrogateId)
      }

    val headerWithSurrogates: SCollection[(Surrogate, SilverFacilityHeader)] =
      headers.map {
        Transform.addSurrogate(
          "cbh-healthyblue-data",
          "silver_claims",
          "institutional_header",
          _
        )(_.identifier.surrogateId)
      }
    (linesWithSurrogates, headerWithSurrogates)
  }

  def mkFacility(
    lines: SCollection[(Surrogate, SilverFacilityLine)],
    headers: SCollection[(Surrogate, SilverFacilityHeader)]
  ): SCollection[Facility] = {
    val joinedClaims: SCollection[((Surrogate, SilverFacilityHeader),
                                   Option[Iterable[(Surrogate, SilverFacilityLine)]])] =
      headers
        .keyBy(_._2.data.Transaction_Control_Number)
        .leftOuterJoin(lines.groupBy(_._2.data.Transaction_Control_Number))
        .values

    joinedClaims.map {
      case ((headerSurrogate, header), maybeLines) =>
        Facility(
          claimId = Transform.mkIdentifier(header.data.Transaction_Control_Number),
          memberIdentifier =
            Transform.mkMemberIdentifierForPatient(header.patient, partner = "healthyblue"),
          header = mkHeader(headerSurrogate, header, maybeLines.flatMap(_.headOption.map(_._2))),
          lines = maybeLines.toList.flatMap(_.flatMap(mkLine))
        )
    }
  }

  def mkHeader(surrogate: Surrogate,
               header: SilverFacilityHeader,
               line: Option[SilverFacilityLine]): Header = {
    val claim: ParsedFacilityHeader = header.data
    Header(
      partnerClaimId = claim.Transaction_Control_Number,
      typeOfBill = for {
        frequencyCode: String <- claim.Claim_Frequency_Code
        facilityTypeCode: String <- claim.Facility_Type_Code
      } yield "0" + frequencyCode + facilityTypeCode,
      admissionType = claim.Admission_Type_Code,
      admissionSource = claim.Admission_Source_Code,
      dischargeStatus = claim.Patient_Status_Code,
      lineOfBusiness = line.flatMap(_.data.Health_Plan),
      subLineOfBusiness = line.flatMap(_.data.Benefit_Plan),
      drg = DRG(None, None, claim.Diagnosis_Related_Group_DRG_Code),
      provider = HeaderProvider(
        billing = claim.Billing_Provider_Identifier.map(ProviderIdentifier(_, None)),
        referring = claim.Referring_Provider_Identifier.map(ProviderIdentifier(_, None)),
        servicing = claim.Attending_Provider_Primary_Identifier.map(ProviderIdentifier(_, None)),
        operating = claim.Operating_Physician_Primary_Identifier.map(ProviderIdentifier(_, None))
      ),
      diagnoses = mkDiagnoses(claim, surrogate),
      procedures = mkHeaderProcedures(claim, surrogate),
      date = Date(
        from = claim.Statement_From_Date,
        to = claim.Statement_To_Date,
        admit = claim.Admission_Date,
        discharge = claim.Discharge_Time.flatMap(discharge =>
          Conversions.safeParse(discharge, new LocalDate(_))),
        paid = claim.Date_of_Payment
      )
    )
  }

  def mkDiagnoses(header: ParsedFacilityHeader, surrogate: Surrogate): List[Diagnosis] = {
    def mkDiagnosis(tier: String = DiagnosisTier.Secondary.name, diagnosisCode: String): Diagnosis =
      Diagnosis(
        surrogate = surrogate,
        tier = tier,
        codeset = Transform.determineDiagnosisCodeset(diagnosisCode),
        code = diagnosisCode
      )

    val principalDiagnosis: Option[Diagnosis] =
      header.Principal_Diagnosis_Code.map(code => mkDiagnosis(DiagnosisTier.Principal.name, code))
    val admitDiagnosis: Option[Diagnosis] =
      header.Admitting_Diagnosis_Code.map(code => mkDiagnosis(DiagnosisTier.Admit.name, code))
    val otherDiagnoses: List[Diagnosis] = {
      val others: List[String] = List(
        header.Other_Diagnosis_01,
        header.Other_Diagnosis_02,
        header.Other_Diagnosis_03,
        header.Other_Diagnosis_04,
        header.Other_Diagnosis_05,
        header.Other_Diagnosis_06,
        header.Other_Diagnosis_07,
        header.Other_Diagnosis_08,
        header.Other_Diagnosis_09,
        header.Other_Diagnosis_10,
        header.Other_Diagnosis_11,
        header.Other_Diagnosis_12,
        header.Other_Diagnosis_13,
        header.Other_Diagnosis_14,
        header.Other_Diagnosis_15,
        header.Other_Diagnosis_16,
        header.Other_Diagnosis_17,
        header.Other_Diagnosis_18,
        header.Other_Diagnosis_19,
        header.Other_Diagnosis_20,
        header.Other_Diagnosis_21,
        header.Other_Diagnosis_22,
        header.Other_Diagnosis_23,
        header.Other_Diagnosis_24
      ).flatten

      others.map(other => mkDiagnosis(diagnosisCode = other))
    }

    otherDiagnoses ++ principalDiagnosis.toList ++ admitDiagnosis.toList
  }

  def mkHeaderProcedures(header: ParsedFacilityHeader,
                         surrogate: Surrogate): List[HeaderProcedure] = {
    def mkProcedure(tier: String, procedureCode: String): HeaderProcedure =
      HeaderProcedure(
        surrogate = surrogate,
        tier = tier,
        codeset = Transform.determineProcedureCodeset(procedureCode),
        code = procedureCode
      )

    val principalProcedure: Option[HeaderProcedure] =
      header.Principal_Procedure_Code.map(code => mkProcedure(DiagnosisTier.Principal.name, code))
    val otherProcedures: List[HeaderProcedure] = {
      val others: List[String] = List(
        header.Procedure_Code_01,
        header.Procedure_Code_02,
        header.Procedure_Code_03,
        header.Procedure_Code_04,
        header.Procedure_Code_05,
        header.Procedure_Code_06,
        header.Procedure_Code_07,
        header.Procedure_Code_08,
        header.Procedure_Code_09,
        header.Procedure_Code_10,
        header.Procedure_Code_11,
        header.Procedure_Code_12,
        header.Procedure_Code_13,
        header.Procedure_Code_14,
        header.Procedure_Code_15,
        header.Procedure_Code_16,
        header.Procedure_Code_17,
        header.Procedure_Code_18,
        header.Procedure_Code_19,
        header.Procedure_Code_20,
        header.Procedure_Code_21,
        header.Procedure_Code_22,
        header.Procedure_Code_23,
        header.Procedure_Code_24
      ).flatten

      others.map(other => mkProcedure(DiagnosisTier.Secondary.name, procedureCode = other))
    }

    otherProcedures ++ principalProcedure.toList
  }

  def mkLine(line: (Surrogate, SilverFacilityLine)): Option[Line] = {
    val surrogate: Surrogate = line._1
    val claim: ParsedFacilityLine = line._2.data
    for {
      lineNumberStr <- claim.Line_Number
      lineNum <- Conversions.safeParse(lineNumberStr, _.toInt)
    } yield {
      Line(
        surrogate = surrogate,
        lineNumber = lineNum,
        revenueCode = claim.Service_Line_Revenue_Code,
        cobFlag = None,
        capitatedFlag = None,
        claimLineStatus = claim.Line_Status_Code,
        inNetworkFlag = claim.In_or_Out_Network.flatMap(Conversions.safeParse(_, _.toBoolean)),
        serviceQuantity = claim.Service_Unit_Count.flatMap(Conversions.safeParse(_, _.toInt)),
        typesOfService = List.empty,
        procedure = claim.Procedure_Code.map(
          code =>
            Procedure(
              surrogate = surrogate,
              tier = ProcedureTier.Secondary.name,
              codeset = Transform.determineProcedureCodeset(code),
              code = code,
              modifiers = List(
                claim.Procedure_Modifier_01,
                claim.Procedure_Modifier_02,
                claim.Procedure_Modifier_03,
                claim.Procedure_Modifier_04
              ).flatten
          )),
        amount = Constructors.mkAmount(
          allowed = claim.Claim_Line_Allowed_Amount,
          billed = claim.Total_Claim_Line_Charge_Amount,
          COB = None,
          copay = None,
          deductible = None,
          coinsurance = None,
          planPaid = claim.Payers_Claim_Line_Payment_Amount
        )
      )
    }
  }

}
