package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.{SilverMemberDemographic, SilverMemberMonth}
import cityblock.models.gold.Claims.Gender
import cityblock.models.gold.Claims._
import cityblock.models.{Identifier, Surrogate}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.Transformer
import cityblock.utilities.emblem.Insurance
import cityblock.utilities.reference.tables
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.{SCollection, SideInput}

sealed case class MemberTransformer(member_demographics: SCollection[SilverMemberDemographic],
                                    mappedLOB: SideInput[Map[String, tables.LineOfBusiness]],
                                    member_months: SCollection[SilverMemberMonth])
    extends Transformer[Member] {
  def transform()(implicit pr: String): SCollection[Member] =
    MemberTransformer.pipeline(pr, mappedLOB, member_demographics, member_months)
}

object MemberTransformer {
  def pipeline(pr: String,
               mappedLOB: SideInput[Map[String, tables.LineOfBusiness]],
               silverMemberDemographic: SCollection[SilverMemberDemographic],
               silverMemberMonth: SCollection[SilverMemberMonth]): SCollection[Member] = {
    val memberMonthByMemberKey = silverMemberMonth
      .withName("Index member_month by patient key")
      .keyBy(silver => silver.patient.patientId.getOrElse(silver.month.MEMBERID))

    val memberDemographicsByMemberKey = silverMemberDemographic
      .withName("Index member_demographics by patient key")
      .keyBy(silver => silver.patient.patientId.getOrElse(silver.demographic.MEMBER_ID))

    val attributionsByMemberKey = memberMonthByMemberKey
      .withName("Create gold memberAttribution")
      .flatMapValues {
        MemberTransformer.memberAttribution(pr, _)
      }
      .withName("Group by patient key")
      .groupByKey

    val eligibilityByMemberKey = memberMonthByMemberKey
      .withName("Create gold memberEligibility")
      .withSideInputs(mappedLOB)
      .map {
        case ((key, silver), ctx) =>
          (key, MemberTransformer.memberEligibility(pr, silver, ctx(mappedLOB)))
      }
      .toSCollection
      .groupByKey

    val demographicsAndIdentifierByMemberKey = memberDemographicsByMemberKey
      .withName("Group silver member demographics by patient key")
      .groupByKey
      .map {
        case (key, list) =>
          val latest = list.maxBy { silver =>
            silver.demographic.SERVICEYEARMONTH
              .flatMap(YearMonthDateParser.parse)
          }(Transform.NoneMinOptionLocalDateOrdering)

          (key,
           (MemberIdentifier(
              commonId = latest.patient.source.commonId,
              patientId = latest.patient.patientId,
              partnerMemberId = latest.patient.externalId,
              partner = partner
            ),
            MemberTransformer.memberDemographics(pr, latest)))
      }

    MultiJoin
      .withName("Join demographics, attributions, and eligibilites")
      .left(demographicsAndIdentifierByMemberKey, attributionsByMemberKey, eligibilityByMemberKey)
      .map {
        case (_, ((identifier, demographics), attributions, eligibilities)) =>
          Member(
            identifier = identifier,
            demographics = demographics,
            attributions = attributions.getOrElse(List()).toList,
            eligibilities = eligibilities.getOrElse(List()).toList
          )
      }
  }

  private def memberAttribution(project: String,
                                silver: SilverMemberMonth): Option[MemberAttribution] =
    for (pcpId <- silver.month.PROVIDERID)
      yield
        MemberAttribution(
          identifier = Identifier(
            id = Transform.generateUUID(),
            partner = partner,
            surrogate = Surrogate(
              id = silver.identifier.surrogateId,
              project = project,
              dataset = "silver_claims",
              table = "member_month"
            )
          ),
          date = MemberAttributionDates(
            from = silver.month.SPANFROMDATE,
            to = silver.month.SPANTODATE
          ),
          PCPId = Some(ProviderKey(pcpId, silver.month.PROVIDERLOCATIONSUFFIX).uuid.toString)
        )

  def memberDemographics(project: String, silver: SilverMemberDemographic): MemberDemographics =
    MemberDemographics(
      identifier = Identifier(
        id = Transform.generateUUID(),
        partner = partner,
        surrogate = Surrogate(
          id = silver.identifier.surrogateId,
          project = project,
          dataset = "silver_claims",
          table = "member_month"
        )
      ),
      date = MemberDemographicsDates(
        from = silver.demographic.SERVICEYEARMONTH
          .flatMap(YearMonthDateParser.parse),
        to = None,
        birth = silver.demographic.MEM_DOB,
        death = silver.demographic.MEM_DOD
      ),
      identity = MemberDemographicsIdentity(
        lastName = silver.demographic.MEM_LNAME,
        firstName = silver.demographic.MEM_FNAME,
        middleName = silver.demographic.MEM_MNAME,
        nameSuffix = None,
        SSN = silver.demographic.MEM_SSN,
        gender = Some(toGoldClaimsGender(silver.demographic.MEM_GENDER).toString),
        ethnicity = silver.demographic.MEM_ETHNICITY,
        race = silver.demographic.MEM_RACE,
        primaryLanguage = silver.demographic.MEM_LANGUAGE,
        maritalStatus = silver.demographic.MARITALSTATUS,
        relation = None,
        NMI = silver.demographic.NMI
      ),
      location = MemberDemographicsLocation(
        address1 = silver.demographic.MEM_ADDR1,
        address2 = silver.demographic.MEM_ADDR2,
        city = silver.demographic.MEM_CITY,
        state = silver.demographic.MEM_STATE,
        county = silver.demographic.COUNTY,
        zip = silver.demographic.MEM_ZIP,
        email = silver.demographic.MEM_EMAIL,
        phone = silver.demographic.MEM_PHONE
      )
    )

  private def memberEligibility(project: String,
                                silver: SilverMemberMonth,
                                lobMap: Map[String, tables.LineOfBusiness]): MemberEligibility =
    MemberEligibility(
      identifier = Identifier(
        id = Transform.generateUUID(),
        partner = partner,
        surrogate = Surrogate(
          id = silver.identifier.surrogateId,
          project = project,
          dataset = "silver_claims",
          table = "member_month"
        )
      ),
      date = MemberEligibilityDates(
        from = silver.month.ELIGIBILITYSTARTDATE,
        to = silver.month.ELIGIBILITYENDDATE
      ),
      detail = MemberEligibilityDetail(
        lineOfBusiness = Insurance.MemberMonthMapping
          .getLineOfBusiness(silver.month, lobMap),
        subLineOfBusiness = Insurance.MemberMonthMapping
          .getSubLineOfBusiness(silver.month, lobMap),
        partnerPlanDescription = Insurance.MemberMonthMapping.getPlanDescription(silver.month),
        partnerBenefitPlanId = None,
        partnerEmployerGroupId = None
      )
    )

  def toGoldClaimsGender(gender: Option[String]): Gender.Value = gender match {
    case Some("M") => Gender.M
    case Some("F") => Gender.F
    case _         => Gender.U
  }
}
