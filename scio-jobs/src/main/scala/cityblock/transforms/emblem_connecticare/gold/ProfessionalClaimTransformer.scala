package cityblock.transforms.emblem_connecticare.gold

import cityblock.models.EmblemConnecticareSilverClaims.{
  SilverProfessionalClaim,
  SilverProfessionalExtension
}
import cityblock.models.Surrogate
import cityblock.models.gold.Claims._
import cityblock.models.gold.ProfessionalClaim._
import cityblock.models.gold._
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.transforms.Transform
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat
import java.util.UUID

object ProfessionalClaimTransformer {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val bigqueryProject: String = args.required("bigqueryProject")

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val deliveryDate: LocalDate = LocalDate.parse(args.required("deliveryDate"), dtf)

    val silverDataset = args.required("silverDataset")
    val silverTable = args.required("silverTable")

    val goldDataset = args.required("goldDataset")
    val goldTable = args.required("goldTable")

    val professionalClaims: SCollection[SilverProfessionalClaim] =
      Transform.fetchFromBigQuery[SilverProfessionalClaim](
        sc,
        bigqueryProject,
        silverDataset,
        silverTable,
        deliveryDate
      )

    val professionalExtensions: SCollection[SilverProfessionalExtension] =
      Transform.fetchFromBigQuery[SilverProfessionalExtension](
        sc,
        bigqueryProject,
        silverDataset,
        "professional_ext_v2",
        deliveryDate
      )

    val professionalClaimsWithSurrogates: SCollection[(Surrogate, SilverProfessionalClaim)] =
      professionalClaims
        .withName("Add surrogates")
        .map {
          Transform.addSurrogate(
            bigqueryProject,
            silverDataset,
            silverTable,
            _
          )(_.identifier.surrogateId)
        }

    val joinedProfessionals = {
      val groupedProfessionals
        : SCollection[(Option[String], (Surrogate, SilverProfessionalClaim))] =
        professionalClaimsWithSurrogates.keyBy(_._2.data.L_CLAIMNUMBER)

      val groupedExtensions: SCollection[(Option[String], SilverProfessionalExtension)] =
        professionalExtensions.keyBy(extension => {
          for {
            lineNumber: String <- extension.data.CLM_LN_SEQ_NBR
          } yield {
            extension.data.CLAIM_ID + lineNumber
          }
        })

      val joined = groupedProfessionals.leftOuterJoin(groupedExtensions).values
      joined.groupBy(_._1._2.data.CLAIM_ID).values
    }

    val goldProfessionalClaims = joinedProfessionals.flatMap(profLines => {
      val headerLine = profLines.find(_._1._2.data.CLM_LN_SEQ_NBR == Option(1)).map(_._1)

      val lines: Iterable[Line] = profLines.map {
        case ((surrogate, line), diagnosis) => mkLine(surrogate, line, diagnosis)
      }

      professionalClaim(headerLine, lines.toList)
    })

    Transform.persist(
      goldProfessionalClaims,
      bigqueryProject,
      goldDataset,
      goldTable,
      deliveryDate,
      WRITE_TRUNCATE,
      CREATE_IF_NEEDED
    )

    sc.close().waitUntilFinish()
  }

  private def professionalClaim(header: Option[(Surrogate, SilverProfessionalClaim)],
                                lines: List[Line]): Option[Professional] =
    for {
      (surrogate, headerLine) <- header
    } yield {
      val headerDiagnosis = mkHeaderDiagnoses(surrogate, headerLine)

      Professional(
        claimId = UUID.nameUUIDFromBytes(headerLine.data.CLAIM_ID.getBytes()).toString,
        memberIdentifier = MemberIdentifier(
          patientId = headerLine.patient.patientId,
          partnerMemberId = headerLine.patient.externalId,
          commonId = headerLine.patient.source.commonId,
          partner = "emblem"
        ),
        header = mkHeader(headerLine, Transform.uniqueDiagnoses(headerDiagnosis, lines)),
        lines = lines
      )
    }

  private def mkHeader(header: SilverProfessionalClaim, diagnoses: Iterable[Diagnosis]): Header =
    Header(
      partnerClaimId = header.data.CLAIM_ID,
      lineOfBusiness = None, // TODO: fill in using Jordan's mapping
      subLineOfBusiness = None,
      provider = HeaderProvider(
        billing = mkProvider(header.data.BLNG_PROV_PRPRID, None),
        referring = mkProvider(header.data.REFR_PROV_PRPRID, None)
      ),
      diagnoses = Transform.uniqueDiagnoses(diagnoses).toList
    )

  private def mkProvider(
    providerIdField: Option[String],
    providerSpecialtyField: Option[String]
  ): Option[ProviderIdentifier] =
    for (id <- providerIdField)
      yield
        ProviderIdentifier(
          id = mkProviderId(id),
          specialty = providerSpecialtyField
        )

  private def mkLine(surrogate: Surrogate,
                     silver: SilverProfessionalClaim,
                     diagnoses: Option[SilverProfessionalExtension]): Line =
    Line(
      surrogate = surrogate,
      lineNumber = silver.data.CLM_LN_SEQ_NBR.getOrElse(0),
      cobFlag = mkCobFlag(silver.data.COB_IND),
      capitatedFlag = mkCapitatedFlag(silver.data.FFS_CAP_IND),
      claimLineStatus = silver.data.LN_PAYMENT_STATUS.flatMap(mkClaimLineStatus),
      inNetworkFlag = mkInNetworkFlag(silver.data.IN_OUT_NTWRK_IND),
      serviceQuantity = silver.data.UNITS,
      placeOfService = silver.data.PLACE_OF_SERVICE,
      date = mkDate(silver),
      provider = LineProvider(
        servicing = mkProvider(silver.data.SERV_PROV_PRPRID, silver.data.SERV_PROV_FSPECLTY)
      ),
      procedure = mkProcedure(surrogate, silver),
      amount = mkAmount(silver),
      diagnoses = mkLineDiagnoses(surrogate, diagnoses).getOrElse(List.empty),
      typesOfService = silver.data.TYPE_OF_SERVICE.map(TypeOfService(1, _)).toList
    )

  private def mkCobFlag(COB_IND: Option[String]): Option[Boolean] =
    COB_IND.flatMap {
      case "Y" => Some(true)
      case "N" => Some(false)
      case _   => None
    }

  private def mkCapitatedFlag(FFS_CAP_IND: Option[String]): Option[Boolean] =
    FFS_CAP_IND.flatMap {
      case "F" => Some(false)
      case "C" => Some(true)
      case _   => None
    }

  private def mkClaimLineStatus(LN_PAYMENT_STATUS: String): Option[String] = {
    val status = LN_PAYMENT_STATUS match {
      case "PD" => Some(ClaimLineStatus.Paid)
      case "DN" => Some(ClaimLineStatus.Denied)
      case "RV" => Some(ClaimLineStatus.Reversed)
      case _    => None
    }
    status.map(_.toString)
  }

  private def mkInNetworkFlag(IN_OUT_NTWRK_IND: Option[String]): Option[Boolean] =
    IN_OUT_NTWRK_IND.flatMap {
      case "I" => Some(true)
      case "O" => Some(false)
      case _   => None
    }

  private def mkDate(silver: SilverProfessionalClaim): ProfessionalDate =
    ProfessionalDate(
      from = silver.data.SERV_START_DATE,
      to = silver.data.SERV_END_DATE,
      paid = silver.data.PAID_DATE
    )

  private def mkProcedure(surrogate: Surrogate,
                          silver: SilverProfessionalClaim): Option[Procedure] = {
    val tier = if (silver.data.CLM_LN_SEQ_NBR.contains(1)) {
      ProcedureTier.Principal
    } else {
      ProcedureTier.Secondary
    }

    Transform.mkProcedure(
      surrogate,
      silver,
      tier,
      (p: SilverProfessionalClaim) => p.data.HCPCS_CPT_PROC_CD,
      (p: SilverProfessionalClaim) => (p.data.PROC_MOD_1 ++ p.data.PROC_MOD_2).toList
    )
  }

  private def mkAmount(silver: SilverProfessionalClaim): Amount =
    Constructors.mkAmount(
      allowed = silver.data.ALLOWED_AMT,
      billed = silver.data.BILLED_AMT,
      COB = silver.data.COB_PAID_AMT,
      copay = silver.data.COPAY_AMT,
      deductible = silver.data.DEDUCTIBLE_AMT,
      coinsurance = silver.data.COINS_AMT,
      planPaid = silver.data.PAID_AMOUNT
    )

  private def mkHeaderDiagnoses(surrogate: Surrogate,
                                silver: SilverProfessionalClaim): List[Diagnosis] =
    silver.data.PRIN_DIAG_CD.map { code =>
      Diagnosis(
        surrogate = surrogate,
        tier = DiagnosisTier.Principal.name,
        code = code,
        codeset = Transform.CodeSet.ICD10Cm.toString
      )
    }.toList

  private def mkLineDiagnoses(
    surrogate: Surrogate,
    professionalExtension: Option[SilverProfessionalExtension]
  ): Option[List[Diagnosis]] =
    professionalExtension.map(extension => {
      val diagCodes: List[String] = List(
        extension.data.DIAG_CD_PRIMARY,
        extension.data.DIAG_CD_02_ADTNL,
        extension.data.DIAG_CD_03_ADTNL,
        extension.data.DIAG_CD_04_ADTNL,
        extension.data.DIAG_CD_05_ADTNL,
        extension.data.DIAG_CD_06_ADTNL,
        extension.data.DIAG_CD_07_ADTNL,
        extension.data.DIAG_CD_08_ADTNL,
        extension.data.DIAG_CD_09_ADTNL,
        extension.data.DIAG_CD_10_ADTNL,
        extension.data.DIAG_CD_11_ADTNL,
        extension.data.DIAG_CD_12_ADTNL
      ).flatten

      diagCodes.map(diagCode => {
        Diagnosis(
          surrogate = surrogate,
          tier = DiagnosisTier.Secondary.name,
          code = diagCode,
          codeset = Transform.CodeSet.ICD10Cm.toString
        )
      })
    })
}
