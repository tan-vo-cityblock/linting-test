package cityblock.utilities

import cityblock.models.gold.Claims._
import cityblock.models.gold.FacilityClaim.{Facility, HeaderProcedure}
import cityblock.models.gold.NewProvider.{
  Location,
  Provider,
  ProviderIdentifier => ProviderIdentifierV2,
  Specialty,
  Taxonomy
}
import cityblock.models.gold.ProfessionalClaim.Professional
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.models.gold.{
  Address,
  Amount,
  Date,
  FacilityClaim,
  ProfessionalClaim,
  ProviderIdentifier,
  TypeOfService
}
import cityblock.models.{Identifier, Surrogate}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet.CodeSet
import org.joda.time.LocalDate

/**
 * Default GoldClaimsModel objects for use in tests.
 *
 * The member, provider, professional, and facility functions take in a minimal set of
 * identifiers and fill out the rest of the object with default values. If it is necessary
 * to modify these default valuers, use the copy constructor.
 */
object GoldClaimsDefaults {
  private val emptyMemberIdentifier = MemberIdentifier(None, "", None, "")

  private def id: String = Transform.generateUUID()
  private def surrogate = Surrogate(id, "", "", "")
  private def identifier(id: String, partner: String) =
    Identifier(id = id, partner = partner, surrogate = surrogate)
  private def providerIdentifier(surrogate: Surrogate,
                                 partnerProviderId: String,
                                 partner: String,
                                 id: String) =
    ProviderIdentifierV2(surrogate = surrogate,
                         partnerProviderId = partnerProviderId,
                         partner = partner,
                         id = id)

  def member(partnerId: String,
             commonId: Option[String],
             patientId: Option[String],
             partner: String,
             pcpId: Option[String] = None): Member = Member(
    identifier = MemberIdentifier(
      commonId = commonId,
      patientId = patientId,
      partnerMemberId = partnerId,
      partner = partner
    ),
    demographics = MemberDemographics(
      identifier = identifier("demographics id", "connecticare"),
      date = MemberDemographicsDates(
        from = Some(LocalDate.parse("2018-04-01")),
        to = Some(LocalDate.parse("2018-04-30")),
        birth = Some(LocalDate.parse("1984-10-01")),
        death = None
      ),
      identity = MemberDemographicsIdentity(
        firstName = Some("Joan"),
        middleName = Some("of"),
        lastName = Some("Arc"),
        nameSuffix = None,
        gender = Option("F"),
        ethnicity = None,
        race = None,
        primaryLanguage = Some("French"),
        maritalStatus = None,
        relation = Some("self"),
        SSN = Some("100000123"),
        NMI = partnerId
      ),
      location = MemberDemographicsLocation(
        address1 = Some("123 Paris St."),
        address2 = Some("Apt 3F"),
        city = Some("Brooklyn"),
        state = Some("NY"),
        county = Some("Kings"),
        zip = Some("11214"),
        phone = None,
        email = Some("joanofarc@yahoo.com")
      )
    ),
    attributions = List(
      MemberAttribution(
        identifier = identifier("attribution id", "connecticare"),
        date = MemberAttributionDates(
          from = Some(LocalDate.parse("2018-04-01")),
          to = Some(LocalDate.parse("2018-04-30"))
        ),
        PCPId = pcpId
      )),
    eligibilities = List(
      MemberEligibility(
        identifier = identifier("eligibility id", "connecticare"),
        date = MemberEligibilityDates(
          from = Some(LocalDate.parse("2018-04-01")),
          to = Some(LocalDate.parse("2018-04-30"))
        ),
        detail = MemberEligibilityDetail(
          lineOfBusiness = Some(Insurance.LineOfBusiness.Medicare.toString),
          subLineOfBusiness = Some(Insurance.SubLineOfBusiness.MedicareAdvantage.toString),
          partnerPlanDescription = None,
          partnerBenefitPlanId = None,
          partnerEmployerGroupId = None
        )
      ))
  )

  def member(id: MemberIdentifier): Member =
    member(id.partnerMemberId, id.commonId, id.patientId, id.partner)

  def provider(kind: Option[String], partner: String, id: String = id): Provider =
    Provider(
      providerIdentifier = providerIdentifier(surrogate, "partnerProviderId", id, partner),
      dateEffective = Date(
        from = Some(LocalDate.parse("2018-04-01")),
        to = Some(LocalDate.parse("2018-04-30"))
      ),
      specialties = List(
        Specialty(
          code = "code",
          codeset = Some("codeset"),
          tier = Some("tier")
        )),
      taxonomies = List(
        Taxonomy(
          code = "code",
          tier = Some("tier")
        )),
      pcpFlag = Some(true),
      npi = Some("npi"),
      name = Some("Betty Davis"),
      affiliatedGroupName = None,
      entityType = Some("entityType"),
      inNetworkFlag = Some(true),
      locations = List(
        Location(
          mailing = None,
          clinic = Some(Address(
            address1 = Some("321 Doctor Dr."),
            address2 = None,
            city = Some("Healthville"),
            zip = Some("32059"),
            county = Some("Santa Clara"),
            state = Some("CT"),
            country = Some("USA"),
            email = Some("bettydavis@gmail.com"),
            phone = Some("555-555-5555")
          ))
        ))
    )

  def professional(memberIdentifier: MemberIdentifier = emptyMemberIdentifier,
                   id: String = id): Professional =
    Professional(
      claimId = id,
      memberIdentifier = memberIdentifier,
      header = ProfessionalClaim.Header(
        partnerClaimId = "partner claim id",
        lineOfBusiness = None,
        subLineOfBusiness = None,
        provider = ProfessionalClaim.HeaderProvider(
          billing = None,
          referring = None
        ),
        diagnoses = List()
      ),
      lines = List()
    )

  def professionalLine: ProfessionalClaim.Line = ProfessionalClaim.Line(
    surrogate = surrogate,
    lineNumber = 0,
    cobFlag = None,
    capitatedFlag = None,
    claimLineStatus = None,
    inNetworkFlag = None,
    serviceQuantity = None,
    placeOfService = None,
    date = ProfessionalClaim.ProfessionalDate(
      from = None,
      to = None,
      paid = None
    ),
    provider = ProfessionalClaim.LineProvider(
      servicing = None
    ),
    procedure = None,
    amount = Amount(
      allowed = None,
      billed = None,
      cob = None,
      copay = None,
      deductible = None,
      coinsurance = None,
      planPaid = None
    ),
    diagnoses = List(),
    typesOfService = List()
  )

  def facility(memberIdentifier: MemberIdentifier = emptyMemberIdentifier,
               id: String = id): Facility = Facility(
    claimId = id,
    memberIdentifier = memberIdentifier,
    header = FacilityClaim.Header(
      partnerClaimId = "partner claim id",
      typeOfBill = None,
      admissionType = None,
      admissionSource = None,
      dischargeStatus = None,
      lineOfBusiness = None,
      subLineOfBusiness = None,
      drg = FacilityClaim.DRG(
        version = None,
        codeset = None,
        code = None
      ),
      provider = FacilityClaim.HeaderProvider(
        billing = None,
        referring = None,
        servicing = None,
        operating = None
      ),
      diagnoses = List(),
      procedures = List(),
      date = FacilityClaim.Date(
        from = None,
        to = None,
        admit = None,
        discharge = None,
        paid = None
      )
    ),
    lines = List()
  )

  def facilityLine: FacilityClaim.Line = FacilityClaim.Line(
    surrogate = surrogate,
    lineNumber = 0,
    revenueCode = None,
    cobFlag = None,
    capitatedFlag = None,
    claimLineStatus = None,
    inNetworkFlag = None,
    serviceQuantity = None,
    typesOfService = List(),
    procedure = None,
    amount = Amount(
      allowed = None,
      billed = None,
      cob = None,
      copay = None,
      deductible = None,
      coinsurance = None,
      planPaid = None
    )
  )

  object Builders {
    sealed class FacilityBuilder(state: Facility) {
      def build: Facility = state

      def setHeader(fn: FacilityClaim.Header => FacilityClaim.Header): FacilityBuilder = {
        val newState = state.copy(header = fn(state.header))
        new FacilityBuilder(newState)
      }

      def addDates(fn: FacilityClaim.Date => FacilityClaim.Date): FacilityBuilder = {
        val newState =
          state.copy(header = state.header.copy(date = fn(state.header.date)))
        new FacilityBuilder(newState)
      }

      def addBillingProvider(id: String): FacilityBuilder = {
        val newState = state.copy(
          header = state.header.copy(provider = state.header.provider
            .copy(billing = Some(ProviderIdentifier(id = id, None)))))
        new FacilityBuilder(newState)
      }

      def addDiagnosis(tier: DiagnosisTier, codeset: CodeSet, code: String): FacilityBuilder = {
        val newState = state.copy(
          header = state.header.copy(diagnoses = state.header.diagnoses :+
            Diagnosis(surrogate, tier.name, codeset.toString, code)))
        new FacilityBuilder(newState)
      }

      def addProcedure(tier: ProcedureTier, codeset: CodeSet, code: String): FacilityBuilder = {
        val newState = state.copy(
          header = state.header.copy(
            procedures = state.header.procedures :+ HeaderProcedure(surrogate,
                                                                    tier.toString,
                                                                    codeset.toString,
                                                                    code)
          )
        )
        new FacilityBuilder(newState)
      }

      def addLineBuilder(builder: FacilityLineBuilder): FacilityBuilder = {
        val newState = state.copy(lines = state.lines :+ builder.build)
        new FacilityBuilder(newState)
      }
    }

    object FacilityBuilder {
      def mk(memberIdentifier: MemberIdentifier = emptyMemberIdentifier,
             id: String = id): FacilityBuilder =
        new FacilityBuilder(facility(memberIdentifier, id))
    }

    sealed class FacilityLineBuilder(state: FacilityClaim.Line) {
      def build: FacilityClaim.Line = state

      def addTypeOfService(tos: TypeOfService): FacilityLineBuilder = {
        val newState = state.copy(typesOfService = state.typesOfService :+ tos)
        new FacilityLineBuilder(newState)
      }
    }

    object FacilityLineBuilder {
      def mk: FacilityLineBuilder = new FacilityLineBuilder(facilityLine)
    }

    sealed class ProfessionalBuilder(state: Professional) {
      def build: Professional = state

      def setMember(identifier: MemberIdentifier): ProfessionalBuilder = {
        val newState = state.copy(
          memberIdentifier = identifier
        )
        new ProfessionalBuilder(newState)
      }

      def setProviders(fn: ProfessionalClaim.HeaderProvider => ProfessionalClaim.HeaderProvider)
        : ProfessionalBuilder = {
        val newState = state.copy(header = state.header.copy(provider = fn(state.header.provider)))
        new ProfessionalBuilder(newState)
      }

      def addDiagnosis(tier: DiagnosisTier, codeset: CodeSet, code: String): ProfessionalBuilder = {
        val dx =
          Diagnosis(surrogate, tier.toString, codeset.toString, code)
        val newState =
          state.copy(header = state.header.copy(diagnoses = state.header.diagnoses :+ dx))
        new ProfessionalBuilder(newState)
      }

      def addLineBuilder(line: ProfessionalLineBuilder): ProfessionalBuilder = {
        val newState = state.copy(lines = state.lines :+ line.build)
        new ProfessionalBuilder(newState)
      }
    }

    object ProfessionalBuilder {
      def mk(memberIdentifier: MemberIdentifier, id: String = id): ProfessionalBuilder = {
        val state =
          professional(memberIdentifier = memberIdentifier)
        new ProfessionalBuilder(state)
      }
    }

    sealed class ProfessionalLineBuilder(state: ProfessionalClaim.Line) {
      def build: ProfessionalClaim.Line = state

      def addDates(fn: ProfessionalClaim.ProfessionalDate => ProfessionalClaim.ProfessionalDate)
        : ProfessionalLineBuilder = {
        val newState = state.copy(date = fn(state.date))
        new ProfessionalLineBuilder(newState)
      }

      def addServicingProvider(id: String): ProfessionalLineBuilder = {
        val newState = state.copy(
          provider =
            state.provider.copy(servicing = Some(ProviderIdentifier(id = id, specialty = None))))
        new ProfessionalLineBuilder(newState)
      }

      def addProcedure(tier: ProcedureTier,
                       codeset: CodeSet,
                       code: String): ProfessionalLineBuilder = {
        val newState = state.copy(
          procedure = Some(Procedure(surrogate, tier.toString, codeset.toString, code, List()))
        )
        new ProfessionalLineBuilder(newState)
      }

      def edit(fn: ProfessionalClaim.Line => ProfessionalClaim.Line): ProfessionalLineBuilder =
        new ProfessionalLineBuilder(fn(state))

    }

    object ProfessionalLineBuilder {
      def mk: ProfessionalLineBuilder =
        new ProfessionalLineBuilder(professionalLine)
    }
  }
}
