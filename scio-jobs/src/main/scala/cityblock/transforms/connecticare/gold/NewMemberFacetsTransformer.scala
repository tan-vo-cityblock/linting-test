package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.{
  AttributedPCP,
  FacetsMember,
  HealthCoverageBenefits
}
import cityblock.models.Surrogate
import cityblock.models.connecticare.silver.AttributedPCP.ParsedAttributedPCP
import cityblock.models.connecticare.silver.FacetsMember.ParsedFacetsMember
import cityblock.models.connecticare.silver.HealthCoverageBenefits.ParsedHealthCoverageBenefits
import cityblock.models.gold.Claims.MemberIdentifier
import cityblock.models.gold.NewMember._
import cityblock.models.gold.{Address, Date}
import cityblock.transforms.Transform
import cityblock.utilities.Conversions
import cityblock.utilities.Insurance.LineOfBusiness
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

import scala.util.control.NonFatal

object NewMemberFacetsTransformer {
  def transform(
    sourceProject: String,
    sourceDataset: String,
    sourceTable: String,
    shard: LocalDate,
    members: SCollection[FacetsMember],
    healthCoverage: SCollection[HealthCoverageBenefits],
    attributedPCP: SCollection[AttributedPCP]
  ): SCollection[Member] =
    MultiJoin
      .left(
        members.groupBy(_.data.Member_ID),
        healthCoverage.groupBy(_.data.Member_ID),
        attributedPCP.groupBy(_.data.Member_ID)
      )
      .map {
        /* N.B. We only choose the most recent row of from each of Member, HealthCoverageBenefits, and AttributedPCP.
         * While this ensures that MemberV2_* contains the most up-to-date information, it also means that MemberV2_*
         * is missing information for any member whose insurance info or PCP attribution changes mid-month.
         *
         * Luckily this only affects a minority of members each month:
         * https://console.cloud.google.com/bigquery?sq=1000726452182:b487fdbd5b03466c83018427340837af
         *
         * However, we'll eventually need to emit multiple rows for these members to support Cost & Use and actuary
         * use cases. */
        case (_, (member, maybeBenefits, maybeAttributions)) =>
          val latestMember = member.maxBy {
            _.data.Original_Effective_Date.flatMap(Conversions.safeParse(_, LocalDate.parse))
          }(Transform.NoneMinOptionLocalDateOrdering)

          val latestBenefit = maybeBenefits.map(_.maxBy {
            _.data.Coverage_effective_date.flatMap(Conversions.safeParse(_, LocalDate.parse))
          }(Transform.NoneMinOptionLocalDateOrdering))

          val latestAttribution = maybeAttributions.map(_.maxBy {
            _.data.Primary_Care_Provider_Effective_Date
              .flatMap(Conversions.safeParse(_, LocalDate.parse))
          }(Transform.NoneMinOptionLocalDateOrdering))

          val surrogate = Surrogate(
            id = latestMember.identifier.surrogateId,
            project = sourceProject,
            dataset = sourceDataset,
            table = Transform.shardName(sourceTable, shard)
          )

          mkMember(surrogate, latestMember, latestBenefit, latestAttribution)
      }

  private def toLocalDate(memberDate: Option[String]): Option[LocalDate] =
    memberDate.flatMap(date => {
      try {
        Some(LocalDate.parse(date, DateTimeFormat.forPattern("MM/dd/yyyy")))
      } catch {
        case NonFatal(_) => None
      }
    })

  private def effectiveDate(
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

  private def terminationDate(
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

  private def mkMember(
    surrogate: Surrogate,
    member: FacetsMember,
    maybeBenefit: Option[HealthCoverageBenefits],
    maybeAttribution: Option[AttributedPCP]
  ): Member = {
    val deathDate: Option[LocalDate] = {
      val death: Option[LocalDate] = toLocalDate(member.data.Date_of_death)
      if (death.exists(_.isBefore(LocalDate.now()))) death else None
    }

    val phoneNumber: Option[String] =
      member.data.Residential_Phone.map(_ + member.data.Residential_Phone_Extension.getOrElse(""))

    val date = Date(
      from = effectiveDate(member.data, maybeAttribution.map(_.data), maybeBenefit.map(_.data)),
      to = terminationDate(member.data, maybeAttribution.map(_.data), maybeBenefit.map(_.data))
    )

    val demographic = MemberDemographics(
      dateBirth = toLocalDate(member.data.Birth_Date),
      dateDeath = deathDate,
      name = MemberName(
        last = member.data.Last_Name,
        first = member.data.First_Name,
        middle = member.data.Middle_Initial,
        suffix = member.data.Name_Suffix
      ),
      SSN = None,
      gender = member.data.Gender,
      ethnicity = None,
      race = None,
      primaryLanguage = member.data.Language,
      maritalStatus = None,
      subscriberRelationship = member.data.Relationship
    )

    val location = Address(
      address1 = member.data.Residential_Address_Line_1,
      address2 = member.data.Residential_Address_Line_2,
      city = member.data.Residential_City,
      state = member.data.Residential_State,
      county = None,
      zip = member.data.Residential_Zip,
      country = None,
      email = member.data.Residential_Email,
      phone = phoneNumber
    )

    Member(
      surrogate = surrogate,
      memberIdentifier = MemberIdentifier(
        patientId = member.patient.patientId,
        partnerMemberId = member.data.Member_ID,
        commonId = member.patient.source.commonId,
        partner = "cci_facets"
      ),
      dateEffective = date,
      demographic = demographic,
      location = location,
      pcp = mkMemberPCP(maybeAttribution.map(_.data)),
      eligibility = mkEligibility(maybeBenefit.map(_.data))
    )
  }

  private def mkMemberPCP(maybeAttribution: Option[ParsedAttributedPCP]): MemberPCP =
    MemberPCP(
      id = maybeAttribution.flatMap(_.Provider_ID.map(ProviderKey.apply)).map(_.uuid.toString)
    )

  private def mkEligibility(
    maybeBenefits: Option[ParsedHealthCoverageBenefits]
  ): MemberEligibility = maybeBenefits match {
    case Some(benefits) =>
      val partnerEmployerGroupId = {
        val groupId = (benefits.Group_Level_1_ID ++ benefits.Group_Level_2_ID
          ++ benefits.Group_Level_3_ID).mkString
        if (groupId == "") None else Some(groupId)
      }
      MemberEligibility(
        lineOfBusiness = Some(LineOfBusiness.Medicare.toString),
        subLineOfBusiness = None,
        partnerBenefitPlanId = benefits.Benefit_ID,
        partnerEmployerGroupId = partnerEmployerGroupId
      )
    case _ =>
      MemberEligibility(
        lineOfBusiness = Some(LineOfBusiness.Medicare.toString),
        subLineOfBusiness = None,
        partnerBenefitPlanId = None,
        partnerEmployerGroupId = None
      )
  }
}
