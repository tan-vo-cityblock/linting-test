package cityblock.transforms.tufts.gold

import cityblock.models.gold.Claims.{Diagnosis, MemberIdentifier, Procedure}
import cityblock.models.gold.enums.DiagnosisTier
import cityblock.models.gold.enums.ProcedureTier
import cityblock.models.gold.FacilityClaim.{
  DRG,
  Date,
  Facility,
  Header,
  HeaderProcedure,
  HeaderProvider,
  Line
}
import cityblock.models.gold.{Constructors, ProviderIdentifier, TypeOfService}
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

case class MassHealthFacilityTransformer(
  silver: SCollection[MassHealth]
) extends Transformer[Facility] {
  override def transform()(implicit pr: String): SCollection[Facility] =
    MassHealthFacilityTransformer.pipeline(
      pr,
      "MassHealth",
      silver
    )
}

object MassHealthFacilityTransformer extends MedicalMapping {
  // scalastyle:off method.length
  def pipeline(
    project: String,
    table: String,
    silver: SCollection[MassHealth]
  ): SCollection[Facility] =
    //  using https://docs.google.com/spreadsheets/d/1AZTM1Thj3Av1dfJxiNerZO6HLIGk5a0n4GYsjyXYgSo/edit#gid=595728578
    silver
      .groupBy(ClaimKey.apply)
      .filter { case (_, silverLines) => isMassHealthFacility(silverLines) }
      .map {
        case (claimKey, silverFacilityLines) => {
          val headerLine: MassHealth = silverFacilityLines.minBy(_.data.NUM_DTL)
          val surrogate: Surrogate = Surrogate(
            id = headerLine.identifier.surrogateId,
            project = project,
            dataset = "silver_claims",
            table = table
          )
          Facility(
            claimId = claimKey.uuid.toString,
            memberIdentifier = MemberIdentifier(
              patientId = headerLine.patient.patientId,
              partnerMemberId = headerLine.data.Mem_Id,
              commonId = None,
              partner = PartnerConfiguration.tufts.indexName
            ),
            header = Header(
              partnerClaimId = headerLine.data.NUM_LOGICAL_CLAIM,
              // as of 2021-07-07, masshealth sends 3 digit codes without ignored leading zero
              // using https://med.noridianmedicare.com/web/jea/topics/claim-submission/bill-types
              // takeRight in case they start sending the leading 0 in the future
              typeOfBill = headerLine.data.CDE_TYPE_OF_BILL.map { code =>
                ("0" + code).takeRight(4)
              },
              admissionType = headerLine.data.CDE_ADMIT_TYPE,
              admissionSource = headerLine.data.CDE_ADMIT_SOURCE,
              dischargeStatus = headerLine.data.CDE_PATIENT_STATUS,
              lineOfBusiness = Some(LineOfBusiness.Medicaid.toString),
              subLineOfBusiness = None,
              drg = DRG(
                version = None,
                codeset = None,
                code = headerLine.data.CDE_DRG
              ),
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
                },
                servicing = headerLine.data.ID_PROV_SERVICING.map { id =>
                  ProviderIdentifier(
                    id = id,
                    specialty = headerLine.data.CDE_PROV_TYPE_SERVICING
                  )
                },
                operating = None
              ),
              diagnoses = headerLine.data.CDE_DIAG_1.map { code =>
                Diagnosis(
                  surrogate = surrogate,
                  tier = DiagnosisTier.Principal.name,
                  codeset = determineDiagnosisCodeset(code),
                  code = code
                )
              }.toList ++ List(
                headerLine.data.CDE_DIAG_2, // secondary
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
              },
              procedures =
                headerLine.data.CDE_PROC.map { code =>
                  HeaderProcedure(
                    surrogate = surrogate,
                    tier = ProcedureTier.Principal.name,
                    codeset = determineProcedureCodeset(code),
                    code = code
                  )
                }.toList ++
                  List(
                    headerLine.data.CDE_PROC_ICD9_1,
                    headerLine.data.CDE_PROC_ICD9_2,
                    headerLine.data.CDE_PROC_ICD9_3,
                    headerLine.data.CDE_PROC_ICD9_4,
                    headerLine.data.CDE_PROC_ICD9_5,
                    headerLine.data.CDE_PROC_ICD9_6,
                    headerLine.data.CDE_PROC_ICD9_7,
                    headerLine.data.CDE_PROC_ICD9_8,
                    headerLine.data.CDE_PROC_ICD9_9
                  ).flatten.map { code =>
                    HeaderProcedure(
                      surrogate = surrogate,
                      tier = ProcedureTier.Secondary.name,
                      codeset = determineProcedureCodeset(code),
                      code = code
                    )
                  },
              date = Date(
                from = headerLine.data.DOS_FROM_DT,
                to = headerLine.data.DOS_TO_DT,
                admit = None,
                discharge = None,
                paid = None
              )
            ),
            lines = silverFacilityLines.toList
              .sortBy { case silverLine => silverLine.data.NUM_DTL }
              .flatMap { silverLine =>
                for {
                  num_dtl_str <- silverLine.data.NUM_DTL
                  lineNumber <- Conversions
                    .safeParse(num_dtl_str, _.toInt)
                } yield
                  Line(
                    surrogate = surrogate,
                    lineNumber = lineNumber,
                    revenueCode = silverLine.data.CDE_REVENUE,
                    cobFlag = None,
                    capitatedFlag = None,
                    claimLineStatus = silverLine.data.CLAIM_STATUS
                      .map(Map("P" -> "Paid", "D" -> "Denied")),
                    inNetworkFlag = None,
                    serviceQuantity = silverLine.data.QTY_UNITS_BILLED.flatMap { qty =>
                      Conversions
                        .safeParse(qty, _.toInt)
                    },
                    typesOfService = List(TypeOfService(tier = 1, code = "SERVICE")),
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
                    amount = Constructors.mkAmount(
                      allowed = None,
                      billed = silverLine.data.AMT_BILLED,
                      COB = None,
                      copay = None,
                      deductible = None,
                      coinsurance = None,
                      planPaid = silverLine.data.AMT_PAID
                    )
                  )
              }
          )
        }
      }
}
