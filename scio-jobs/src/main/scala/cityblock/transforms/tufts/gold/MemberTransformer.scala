package cityblock.transforms.tufts.gold

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.TuftsSilverClaims.SilverMember
import cityblock.models.gold.Claims.MemberIdentifier
import cityblock.models.gold.NewMember._
import cityblock.models.gold.{Address, Date}
import cityblock.models.tufts.silver.Member.ParsedMember
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.transforms.tufts.ProviderKey
import cityblock.utilities.Insurance.{LineOfBusiness, SubLineOfBusiness}
import com.spotify.scio.values.SCollection

case class MemberTransformer(
  members: SCollection[SilverMember]
) extends Transformer[Member] {
  override def transform()(implicit pr: String): SCollection[Member] =
    MemberTransformer.mkMembers(members, pr)
}

object MemberTransformer {
  def mkMemberIdentifer(member: SilverMember): MemberIdentifier = {
    val patient: Patient = member.patient

    MemberIdentifier(
      commonId = patient.source.commonId,
      partnerMemberId = member.data.CarrierSpecificMemID,
      patientId = patient.patientId,
      partner = partner
    )
  }

  def mkDate(member: ParsedMember): Date = {
    val startDate = List(member.ProductEnrStartDate, member.PCPEffectiveDate)
      .max(Transform.NoneMinOptionLocalDateOrdering)
    val endDate = List(member.ProductEnrEndDate, member.PCPTerminationDate)
      .min(Transform.NoneMaxOptionLocalDateOrdering)

    Date(
      from = startDate,
      to = endDate
    )
  }

  def mkLocation(member: ParsedMember): Address =
    Address(
      address1 = member.StreetAddress,
      address2 = member.Address2,
      city = member.City,
      state = member.State,
      county = None,
      zip = member.Zip,
      country = None,
      email = None,
      phone = member.PrimaryPhone
    )

  def mkMemberName(member: ParsedMember): MemberName =
    MemberName(
      last = member.MemberLastName,
      first = member.MemberFirstName,
      middle = member.MemberMiddleInit,
      suffix = None
    )

  def mkDemographics(member: ParsedMember): MemberDemographics =
    MemberDemographics(
      dateBirth = member.DOB,
      dateDeath = member.DateOfDeath,
      SSN = member.MemberSSN,
      name = mkMemberName(member),
      gender = member.Gender,
      ethnicity = member.PrimaryEthnicity,
      race = member.PrimaryRace,
      primaryLanguage = member.LanguagePreference,
      maritalStatus = member.MaritalStatus,
      subscriberRelationship = member.MemberToSubscriberRelationship
    )

  def mkPcp(member: ParsedMember): MemberPCP =
    MemberPCP(
      id = member.PCP_ID.map(ProviderKey(_).uuid.toString)
    )

  def mkEligibility(member: ParsedMember): MemberEligibility =
    MemberEligibility(
      lineOfBusiness = Some(LineOfBusiness.Medicare.toString),
      subLineOfBusiness = Some(SubLineOfBusiness.DSNP.toString),
      partnerBenefitPlanId = member.ProductID,
      partnerEmployerGroupId = None
    )

  def mkMembers(silverMembers: SCollection[SilverMember], project: String): SCollection[Member] =
    silverMembers.map { silverMember =>
      val (surrogate, _) =
        Transform.addSurrogate(project, "silver_claims", "Member", silverMember)(
          _.identifier.surrogateId
        )

      val member: ParsedMember = silverMember.data

      Member(
        surrogate = surrogate,
        memberIdentifier = mkMemberIdentifer(silverMember),
        dateEffective = mkDate(member),
        demographic = mkDemographics(member),
        location = mkLocation(member),
        pcp = mkPcp(member),
        eligibility = mkEligibility(member)
      )
    }
}
