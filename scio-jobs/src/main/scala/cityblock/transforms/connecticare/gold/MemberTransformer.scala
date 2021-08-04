package cityblock.transforms.connecticare.gold

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.ConnecticareSilverClaims.Member
import cityblock.models.gold.Claims
import cityblock.models.gold.Claims.Gender.Gender
import cityblock.models.gold.Claims._
import cityblock.models.{Identifier, Surrogate}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.{Key, Transformer}
import cityblock.utilities.connecticare.Insurance.MemberMapping
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate

case class MemberTransformer(commercial: SCollection[Member]) extends Transformer[Claims.Member] {
  override def transform()(implicit pr: String): SCollection[Claims.Member] =
    MemberTransformer.pipeline(pr, "Member", commercial)
}

object MemberTransformer {
  import scala.language.implicitConversions

  def pipeline(
    project: String,
    table: String,
    members: SCollection[Member]
  ): SCollection[Claims.Member] =
    members
      .map(Transform.addSurrogate(project, "silver_claims", table, _)(_.identifier.surrogateId))
      .groupBy {
        case (_, member) =>
          MemberKey(member.patient.externalId, member.patient.patientId)
      }
      .withName("Construct members")
      .map {
        case (_, tuples) =>
          val (latestSurrogate, latestSilver) = tuples.maxBy {
            case (_, silver) => silver.member.ApplyMo
          }(Transform.NoneMinOptionLocalDateOrdering)

          Claims.Member(
            identifier = MemberIdentifier(
              partnerMemberId = latestSilver.patient.externalId,
              patientId = latestSilver.patient.patientId,
              commonId = latestSilver.patient.source.commonId,
              partner = partner
            ),
            demographics = memberDemographics(latestSurrogate, latestSilver),
            attributions = tuples.map {
              case (surrogate, silver) =>
                MemberTransformer.memberAttribution(surrogate, silver)
            }.toList,
            eligibilities = tuples.map {
              case (surrogate, silver) =>
                MemberTransformer.memberEligibility(surrogate, silver)
            }.toList
          )
      }

  /**
   * Key for CCI members that groups rows with equal patientId's but unequal NMI's.
   */
  sealed case class MemberKey(key: Either[String, String]) extends Key

  object MemberKey {
    def apply(nmi: String, patientId: Option[String]): MemberKey =
      patientId.fold(MemberKey(Left(nmi)))(id => MemberKey(Right(id)))
    def apply(patient: Patient): MemberKey =
      MemberKey(patient.externalId, patient.patientId)
  }

  implicit private def memberGetSurrogateId(member: Member): String =
    member.identifier.surrogateId

  private def toGoldClaimsGender(gender: Option[String]): Gender =
    gender match {
      case Some("M") => Gender.M
      case Some("F") => Gender.F
      case _         => Gender.U
    }

  private def identifier(surrogate: Surrogate) = Identifier(
    id = Transform.generateUUID(),
    partner = partner,
    surrogate = surrogate
  )

  sealed case class Dates(from: Option[LocalDate], to: Option[LocalDate])

  private object Dates {
    def apply(member: Member): Dates =
      Dates(
        from = member.member.ApplyMo,
        to = member.member.ApplyMo match {
          // Get the last day of `month` (ApplyMo is always the first day of the month)
          case Some(month) => Some(month.plusMonths(1).minusDays(1))
          case _           => None
        }
      )
  }

  private def memberAttribution(surrogate: Surrogate, member: Member): MemberAttribution =
    MemberAttribution(
      identifier = identifier(surrogate),
      date = MemberAttributionDates(
        from = Dates(member).from,
        to = Dates(member).to
      ),
      PCPId = mkPCPKey(member).map(_.uuid.toString)
    )

  private def memberDemographics(surrogate: Surrogate, member: Member): MemberDemographics =
    MemberDemographics(
      identifier = identifier(surrogate),
      date = MemberDemographicsDates(
        from = Dates(member).from,
        to = Dates(member).to,
        birth = member.member.DateBirth,
        death = None
      ),
      identity = MemberDemographicsIdentity(
        lastName = member.member.Last_Name,
        firstName = member.member.First_Name,
        middleName = None,
        nameSuffix = None,
        SSN = None, // CCI doesn't send SSN's
        gender = Some(toGoldClaimsGender(member.member.Gender).toString),
        ethnicity = None,
        race = None,
        primaryLanguage = None,
        NMI = member.member.NMI,
        maritalStatus = None,
        relation = member.member.ContractClass
      ),
      location = MemberDemographicsLocation(
        address1 = member.member.Address1,
        address2 = member.member.Address2,
        city = member.member.City,
        state = member.member.State,
        county = member.member.County,
        zip = member.member.Zip,
        email = None,
        phone = member.member.Phone1
      )
    )

  private def memberEligibility(surrogate: Surrogate, member: Member): MemberEligibility =
    MemberEligibility(
      identifier = identifier(surrogate),
      date = MemberEligibilityDates(
        from = Dates(member).from,
        to = Dates(member).to
      ),
      detail = MemberEligibilityDetail(
        lineOfBusiness = MemberMapping.getLineOfBusiness(member.member).map(_.toString),
        subLineOfBusiness = MemberMapping.getSubLineOfBusiness(member.member).map(_.toString),
        partnerPlanDescription = MemberMapping.getPlanDescription(member.member),
        partnerBenefitPlanId = None,
        partnerEmployerGroupId = None
      )
    )

  private def mkPCPKey(silver: Member): Option[ProviderKey] =
    for (id <- silver.member.PCP_Num) yield ProviderKey(id)
}
