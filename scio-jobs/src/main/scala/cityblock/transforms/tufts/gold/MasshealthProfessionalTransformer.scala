package cityblock.transforms.tufts.gold
import cityblock.models.gold.Claims.{Diagnosis, MemberIdentifier, Procedure}
import cityblock.models.gold.enums.DiagnosisTier
import cityblock.models.gold.ProfessionalClaim.{
  Header,
  HeaderProvider,
  Line,
  LineProvider,
  Professional,
  ProfessionalDate
}
import cityblock.models.gold.{Constructors, ProviderIdentifier}
import cityblock.models.Surrogate
import cityblock.models.TuftsSilverClaims.MassHealth
import cityblock.transforms.Transform.{
  determineDiagnosisCodeset,
  determineProcedureCodeset,
  Transformer
}
import cityblock.transforms.tufts.ClaimKey
import cityblock.utilities.Conversions
import cityblock.utilities.Insurance.LineOfBusiness
import cityblock.utilities.PartnerConfiguration
import com.spotify.scio.values.SCollection

case class MassHealthProfessionalTransformer(
  silver: SCollection[MassHealth]
) extends Transformer[Professional] {
  override def transform()(implicit pr: String): SCollection[Professional] =
    MassHealthProfessionalTransformer.pipeline(
      pr,
      "MassHealth",
      silver
    )
}

object MassHealthProfessionalTransformer extends MedicalMapping {
  // scalastyle:off method.length
  def pipeline(
    project: String,
    table: String,
    silver: SCollection[MassHealth]
  ): SCollection[Professional] =
    //  using https://docs.google.com/spreadsheets/d/1AZTM1Thj3Av1dfJxiNerZO6HLIGk5a0n4GYsjyXYgSo/edit#gid=330944883
    silver
      .groupBy(ClaimKey.apply)
      .filter { case (_, silverLines) => isMassHealthProfessional(silverLines) }
      .map {
        case (claimKey, silverProfessionalLines) => {
          val headerLine: MassHealth = silverProfessionalLines.minBy(_.data.NUM_DTL)
          val surrogate: Surrogate = Surrogate(
            id = headerLine.identifier.surrogateId,
            project = project,
            dataset = "silver_claims",
            table = table
          )
          Professional(
            claimId = claimKey.uuid.toString,
            memberIdentifier = MemberIdentifier(
              patientId = headerLine.patient.patientId,
              partnerMemberId = headerLine.data.Mem_Id,
              commonId = None,
              partner = PartnerConfiguration.tufts.indexName
            ),
            header = Header(
              partnerClaimId = headerLine.data.NUM_LOGICAL_CLAIM,
              lineOfBusiness = Some(LineOfBusiness.Medicaid.toString),
              subLineOfBusiness = None,
              provider = HeaderProvider(
                billing = headerLine.data.ID_PROV_BILLING.map { id =>
                  ProviderIdentifier(
                    id = id,
                    specialty = headerLine.data.CDE_PROV_TYPE_BILLING
                  )
                },
                referring = headerLine.data.ID_PROV_REFERRING.map { id =>
                  ProviderIdentifier(
                    id = id,
                    specialty = headerLine.data.CDE_PROV_TYPE_REFERRING
                  )
                }
              ),
              diagnoses = (headerLine.data.CDE_DIAG_1.map { code =>
                Diagnosis(
                  surrogate = surrogate,
                  tier = DiagnosisTier.Principal.name,
                  codeset = determineDiagnosisCodeset(code),
                  code = code
                )
              }.toList ++ List(
                headerLine.data.CDE_DIAG_2,
                headerLine.data.CDE_DIAG_3,
                headerLine.data.CDE_DIAG_4,
                headerLine.data.CDE_DIAG_5,
                headerLine.data.CDE_DIAG_6,
                headerLine.data.CDE_DIAG_7,
                headerLine.data.CDE_DIAG_8,
                headerLine.data.CDE_DIAG_9,
                headerLine.data.CDE_DIAG_10,
                headerLine.data.CDE_DIAG_11,
                headerLine.data.CDE_DIAG_12,
                headerLine.data.CDE_DIAG_13,
                headerLine.data.CDE_DIAG_14,
                headerLine.data.CDE_DIAG_15,
                headerLine.data.CDE_DIAG_16,
                headerLine.data.CDE_DIAG_17,
                headerLine.data.CDE_DIAG_18,
                headerLine.data.CDE_DIAG_19,
                headerLine.data.CDE_DIAG_20,
                headerLine.data.CDE_DIAG_21,
                headerLine.data.CDE_DIAG_22,
                headerLine.data.CDE_DIAG_23,
                headerLine.data.CDE_DIAG_24,
                headerLine.data.CDE_DIAG_25,
                headerLine.data.CDE_DIAG_26
              ).flatten.map { code =>
                Diagnosis(
                  surrogate = surrogate,
                  tier = DiagnosisTier.Secondary.name,
                  codeset = determineDiagnosisCodeset(code),
                  code = code
                )
              })
            ),
            lines = silverProfessionalLines.toList
              .sortBy { _.data.NUM_DTL }
              .flatMap { silverLine =>
                for {
                  num_dtl_str <- silverLine.data.NUM_DTL
                  lineNumber <- Conversions
                    .safeParse(num_dtl_str, _.toInt)
                } yield
                  Line(
                    surrogate = surrogate,
                    lineNumber = lineNumber,
                    cobFlag = None,
                    capitatedFlag = None,
                    claimLineStatus = None,
                    inNetworkFlag = None,
                    serviceQuantity = silverLine.data.QTY_UNITS_BILLED.flatMap { qty =>
                      Conversions.safeParse(qty, _.toInt)
                    },
                    placeOfService = silverLine.data.CDE_PLACE_OF_SERVICE,
                    date = ProfessionalDate(
                      from = silverLine.data.DOS_FROM_DT,
                      to = silverLine.data.DOS_TO_DT,
                      paid = None
                    ),
                    provider = LineProvider(
                      servicing = silverLine.data.ID_PROV_SERVICING.map { id =>
                        ProviderIdentifier(
                          id = id,
                          specialty = silverLine.data.CDE_PROV_TYPE_SERVICING
                        )
                      }
                    ),
                    procedure = silverLine.data.CDE_PROC.map { code =>
                      Procedure(
                        surrogate = surrogate,
                        tier = "", // line procedures should not have tiers
                        codeset = determineProcedureCodeset(code),
                        code = code,
                        modifiers = List(
                          silverLine.data.CDE_PROC_MOD,
                          silverLine.data.CDE_PROC_MOD_2,
                          silverLine.data.CDE_PROC_MOD_3,
                          silverLine.data.CDE_PROC_MOD_4
                        ).flatten
                      )
                    },
                    amount = Constructors
                      .mkAmount(
                        allowed = None,
                        billed = silverLine.data.AMT_BILLED,
                        COB = silverLine.data.AMT_PAID_MCARE,
                        copay = None,
                        deductible = None,
                        coinsurance = None,
                        planPaid = silverLine.data.AMT_PAID
                      ),
                    diagnoses = List.empty,
                    typesOfService = List.empty
                  )
              }
          )
        }
      }
}
