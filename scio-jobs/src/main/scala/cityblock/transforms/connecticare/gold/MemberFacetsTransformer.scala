package cityblock.transforms.connecticare.gold

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.ConnecticareSilverClaims.{
  AttributedPCP,
  FacetsMember,
  FacetsProvider,
  HealthCoverageBenefits
}
import cityblock.models.{Identifier, Surrogate}
import cityblock.models.gold.Claims
import cityblock.models.gold.Claims.{
  MemberAttribution,
  MemberAttributionDates,
  MemberDemographics,
  MemberDemographicsDates,
  MemberDemographicsIdentity,
  MemberDemographicsLocation,
  MemberEligibility,
  MemberEligibilityDates,
  MemberEligibilityDetail,
  MemberIdentifier
}
import cityblock.models.connecticare.silver.AttributedPCP.ParsedAttributedPCP
import cityblock.models.connecticare.silver.FacetsMember.ParsedFacetsMember
import cityblock.models.connecticare.silver.HealthCoverageBenefits.ParsedHealthCoverageBenefits
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.utilities.Conversions
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.util.control.NonFatal

case class MemberFacetsTransformer(
  medicareMember: SCollection[FacetsMember],
  medicareHealthCoverageBenefits: SCollection[HealthCoverageBenefits],
  medicareAttributedPCP: SCollection[AttributedPCP],
  provider: SCollection[FacetsProvider]
) extends Transformer[Claims.Member] {
  override def transform()(implicit pr: String): SCollection[Claims.Member] =
    MemberFacetsTransformer.pipeline(
      pr,
      medicareMember,
      medicareHealthCoverageBenefits,
      medicareAttributedPCP,
      provider
    )
}

object MemberFacetsTransformer extends MedicalFacetsMapping {
  def joinPCPWithProvider(
    attributedPCP: SCollection[AttributedPCP],
    provider: SCollection[FacetsProvider]
  ): SCollection[(AttributedPCP, Option[FacetsProvider])] =
    attributedPCP
      .keyBy(_.data.Provider_ID)
      .leftOuterJoin(provider.keyBy(_.data.Facets_provider_nbr))
      .values

  def pipeline(
    project: String,
    members: SCollection[FacetsMember],
    healthCoverage: SCollection[HealthCoverageBenefits],
    attributedPCP: SCollection[AttributedPCP],
    provider: SCollection[FacetsProvider]
  ): SCollection[Claims.Member] = {
    val membersMapped = {
      members
        .withName("Construct members")
        .map(
          Transform.addSurrogate(project, "silver_claims_facets", "FacetsMember_med", _)(
            _.identifier.surrogateId
          )
        )
        .groupBy({
          case (_, member) => member.data.Member_ID
        })
    }

    val pcpWithProvider: SCollection[(AttributedPCP, Option[FacetsProvider])] =
      joinPCPWithProvider(attributedPCP, provider)

    val pcpMapped = pcpWithProvider
      .groupBy(_._1.data.Member_ID)
      .withName("Construct pcp")

    val benefitsMapped = healthCoverage
      .groupBy(_.data.Member_ID)
      .withName("Construct benefits")

    MultiJoin
      .left(membersMapped, pcpMapped, benefitsMapped)
      .map {
        case (_, (surrogateMembers, maybePcp, maybeBenefits)) =>
          val latestSilver: (Surrogate, FacetsMember) = {
            surrogateMembers.maxBy(
              _._2.data.Original_Effective_Date
                .flatMap(Conversions.safeParse(_, LocalDate.parse))
            )(Transform.NoneMinOptionLocalDateOrdering)
          }

          val latestPCP: Option[(ParsedAttributedPCP, Option[FacetsProvider])] = {
            maybePcp
              .map(
                _.maxBy(
                  _._1.data.Primary_Care_Provider_Effective_Date
                    .flatMap(Conversions.safeParse(_, LocalDate.parse))
                )(Transform.NoneMinOptionLocalDateOrdering))
              .map(pcp => (pcp._1.data, pcp._2))
          }

          val latestBenefits: Option[ParsedHealthCoverageBenefits] = {
            maybeBenefits
              .map(
                _.maxBy(
                  _.data.Coverage_effective_date
                    .flatMap(Conversions.safeParse(_, LocalDate.parse))
                )(Transform.NoneMinOptionLocalDateOrdering)
              )
              .map(_.data)
          }

          val latestMemberData: ParsedFacetsMember = latestSilver._2.data
          val latestPatient: Patient = latestSilver._2.patient

          Claims.Member(
            identifier = MemberIdentifier(
              partnerMemberId = latestMemberData.Member_ID,
              patientId = latestPatient.patientId,
              commonId = latestPatient.source.commonId,
              partner = partner
            ),
            demographics = memberDemographics(latestSilver._1,
                                              latestMemberData,
                                              latestPCP.map(_._1),
                                              latestBenefits),
            attributions = surrogateMembers
              .map(silver => {
                memberAttribution(silver._1, silver._2.data, latestPCP, latestBenefits)
              })
              .toList,
            eligibilities = surrogateMembers
              .map(silver => {
                memberEligibility(silver._1, silver._2.data, latestPCP.map(_._1), latestBenefits)
              })
              .toList
          )
      }
  }

  private def identifier(surrogate: Surrogate): Identifier = Identifier(
    id = Transform.generateUUID(),
    partner = partner,
    surrogate = surrogate
  )

  def toLocalDate(memberDate: Option[String]): Option[LocalDate] =
    memberDate.flatMap(date => {
      try {
        Some(LocalDate.parse(date, DateTimeFormat.forPattern("MM/dd/yyyy")))
      } catch {
        case NonFatal(_) => None
      }
    })

  def effectiveDate(
    member: ParsedFacetsMember,
    pcp: Option[ParsedAttributedPCP],
    benefits: Option[ParsedHealthCoverageBenefits]
  ): Option[LocalDate] =
    if (member.Original_Effective_Date.isDefined) {
      toLocalDate(member.Original_Effective_Date)
    } else if (pcp.exists(_.Primary_Care_Provider_Effective_Date.isDefined)) {
      toLocalDate(pcp.get.Primary_Care_Provider_Effective_Date)
    } else {
      benefits.flatMap(benefit => toLocalDate(benefit.Coverage_effective_date))
    }

  def terminationDate(
    member: ParsedFacetsMember,
    pcp: Option[ParsedAttributedPCP],
    benefits: Option[ParsedHealthCoverageBenefits]
  ): Option[LocalDate] =
    if (member.Termination_Date.isDefined) {
      toLocalDate(member.Termination_Date)
    } else if (pcp.exists(_.Primary_Care_Provider_Termination_Date.isDefined)) {
      toLocalDate(pcp.get.Primary_Care_Provider_Termination_Date)
    } else {
      benefits.flatMap(benefit => toLocalDate(benefit.Coverage_termination_date))
    }

  private def memberDemographics(
    surrogate: Surrogate,
    member: ParsedFacetsMember,
    pcp: Option[ParsedAttributedPCP],
    benefits: Option[ParsedHealthCoverageBenefits]
  ): MemberDemographics = {
    val deathDate: Option[LocalDate] = {
      val death: Option[LocalDate] = toLocalDate(member.Date_of_death)
      if (death.exists(_.isBefore(LocalDate.now()))) death else None
    }

    val phoneNumber: Option[String] = {
      member.Residential_Phone.map(_ + member.Residential_Phone_Extension.getOrElse(""))
    }

    MemberDemographics(
      identifier = identifier(surrogate),
      date = MemberDemographicsDates(
        from = effectiveDate(member, pcp, benefits),
        to = terminationDate(member, pcp, benefits),
        birth = toLocalDate(member.Birth_Date),
        death = deathDate
      ),
      identity = MemberDemographicsIdentity(
        lastName = member.Last_Name,
        firstName = member.First_Name,
        middleName = member.Middle_Initial,
        nameSuffix = member.Name_Suffix,
        SSN = None,
        gender = member.Gender,
        ethnicity = None,
        race = None,
        primaryLanguage = member.Language,
        NMI = member.Member_ID,
        maritalStatus = None,
        relation = member.Relationship
      ),
      location = MemberDemographicsLocation(
        address1 = member.Residential_Address_Line_1,
        address2 = member.Residential_Address_Line_2,
        city = member.Residential_City,
        state = member.Residential_State,
        county = None,
        zip = member.Residential_Zip,
        email = member.Residential_Email,
        phone = phoneNumber
      )
    )
  }

  private def memberAttribution(
    surrogate: Surrogate,
    member: ParsedFacetsMember,
    pcp: Option[(ParsedAttributedPCP, Option[FacetsProvider])],
    benefits: Option[ParsedHealthCoverageBenefits]
  ): MemberAttribution = {
    // We join the Attributed PCP file and the Provider file above and use the Provider ID from the Provider here
    // because CCI uses a different ID for the join from Provider to Member and Provider to Claims. To use one ID to
    // join Provider against both tables, we pick the ID used to join on Claims and fill that in here.
    val pcpIdToUse: Option[String] =
      pcp.flatMap(_._2).map(prov => FacetsProviderKey(prov.data)).map(_.uuid.toString)

    MemberAttribution(
      identifier = identifier(surrogate),
      date = MemberAttributionDates(
        from = effectiveDate(member, pcp.map(_._1), benefits),
        to = terminationDate(member, pcp.map(_._1), benefits)
      ),
      PCPId = pcpIdToUse
    )
  }

  private def memberEligibility(
    surrogate: Surrogate,
    member: ParsedFacetsMember,
    pcp: Option[ParsedAttributedPCP],
    benefits: Option[ParsedHealthCoverageBenefits]
  ): MemberEligibility = {
    val partnerEmployerGroupId: Option[String] = {
      val groupId = (benefits.flatMap(_.Group_Level_1_ID)
        ++ benefits.flatMap(_.Group_Level_2_ID)
        ++ benefits.flatMap(_.Group_Level_3_ID)).mkString
      if (groupId == "") None else Some(groupId)
    }

    MemberEligibility(
      identifier = identifier(surrogate),
      date = MemberEligibilityDates(
        from = effectiveDate(member, pcp, benefits),
        to = terminationDate(member, pcp, benefits)
      ),
      detail = MemberEligibilityDetail(
        lineOfBusiness = mkLineOfBusiness(surrogate, benefits.flatMap(_.Line_of_Business)),
        subLineOfBusiness = mkSubLineOfBusiness(surrogate, benefits.flatMap(_.Line_of_Business)),
        partnerPlanDescription = None,
        partnerBenefitPlanId = benefits.flatMap(_.Benefit_ID),
        partnerEmployerGroupId = partnerEmployerGroupId
      )
    )
  }
}
