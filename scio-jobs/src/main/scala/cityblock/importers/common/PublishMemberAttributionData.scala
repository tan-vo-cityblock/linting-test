package cityblock.importers.common

import cityblock.member.service.io._
import cityblock.member.service.api.{
  Address,
  Demographics,
  Detail,
  Email,
  Insurance,
  MemberCreateAndPublishRequest,
  MemberInsuranceRecord,
  Phone,
  Plan
}
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.models.gold.Claims.Member
import cityblock.models.gold.NewProvider.Provider
import cityblock.utilities._
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import org.joda.time.format.ISODateTimeFormat

/**
 * Job for writing and publishing member attribution data to BigQuery (BQ) and Pub/Sub (latter if specified)
 *
 * Usage:
 *  {{{
 *       sbt "runMain PublishMemberAttributionData \\
 *          --environment=prod \\
 *          --project=cityblock-data \\
 *          --runner=DataflowRunner \\
 *          --deliveryDate=<YYYYMMDD> \\
 *          --sourceProject=emblemproduction \\
 *          --bigqueryProject=emblem-data \\
 *          --dataset=gold_claims \\
 *          --destinationProject=cityblock-data \\
 *          --workerMachineType=n1-standard-16"
 *  }}}
 *
 */
@deprecated(
  "This class will be removed in favor of the new pipeline that is being built in cityblock.member.service.attribution")
object PublishMemberAttributionData extends Loggable {
  private val usage =
    s"""
      |usage: "runMain ${this.getClass.toString.split('$')(0)} \\
      |         --environment=prod \\
      |         --project=cityblock-data \\
      |         --runner=DataflowRunner \\
      |         --sourceProject=emblemproduction \\
      |         --bigqueryProject=emblem-data \\
      |         --dataset=gold_claims \\
      |         --deliveryDate=<YYYYMMDD> \\
      |         --destinationProject=cityblock-data \\
      |         --workerMachineType=n1-standard-16"
      |
      |When `publish' is true, messages are published to `destinationProject' and written to BigQuery
      |in the respective `sourceProject'. When `publish is false, messages are not published, but
      |they are written to BigQuery under `destinationProject`.
    """.stripMargin

  /**
   * Runs job
   *
   * @param argv arguments provided to run the Scio job, see object Scaladoc for format
    **/
  def main(argv: Array[String]): Unit = {
    if (argv.contains("--help")) {
      logger.error(usage)
      System.exit(0)
    }

    val (sc, args) = ContextAndArgs(argv)
    implicit val environment: Environment = Environment(argv)

    val sourceProject: String = args.required("sourceProject")
    val bigqueryProject: String = args.required("bigqueryProject")
    val dataset: String = args.required("dataset")
    val deliveryDate: String = args.required("deliveryDate")

    publishMemberAttributionData(sc, sourceProject, bigqueryProject, deliveryDate, dataset)

    sc.close().waitUntilFinish()
  }

  /**
   *   1. produce Tuple for downstream processing (contains 2 `SCollections` retrieved from BQ)
   *   2. produce Member Attribution data
   *   3. save data to BQ and publish to Pub/Sub if flagged
   *
   * @param sc                  ScioContext
   * @param sourceProject       Partner name to retrieve partner IDs, comes from an enum in MultiPartnerIds.scala
   * @param bigqueryProject     BigQuery project in which partner member and provider files are
   */
  def publishMemberAttributionData(
    sc: ScioContext,
    sourceProject: String,
    bigqueryProject: String,
    deliveryDate: String,
    dataset: String
  )(implicit environment: Environment): Unit = {
    val ids: MultiPartnerIds = MultiPartnerIds.getByProject(sourceProject)
    val normalizedPartner: Option[Partner] =
      PartnerConfiguration.getPartnerByProject(bigqueryProject)

    if (normalizedPartner.isDefined) {
      val memberQuery =
        s"SELECT * FROM `$bigqueryProject.$dataset.Member_$deliveryDate` WHERE identifier.patientId IS NOT NULL"
      val providerQuery = s"SELECT * FROM `$bigqueryProject.$dataset.Provider`"
      val members: SCollection[Member] = sc.typedBigQuery[Member](memberQuery)
      val providers: SCollection[Provider] = sc.typedBigQuery[Provider](providerQuery)

      val patientIndex: SCollection[(String, MemberInsuranceRecord)] =
        sc.readMemberInsuranceRecords(normalizedPartner.map(_.configuration.get.indexName))
          .keyBy(_.memberId)

      val attributionData: SCollection[(String, MemberCreateAndPublishRequest)] =
        goldClaimsMembersToPublishRequests(ids, patientIndex, members, providers)

      attributionData.updateAndPublishToMemberService

    } else {
      logger.error(
        s"Skipping as no partner information found for partner [partner: $bigqueryProject]"
      )
    }
  }

  /**
   * Returns Member Attribution data using the following procedure:
   *   1. begin iterating through gold members and ensure they are valid
   *   2. join on patient data (key: patient/member id)
   *   3. left outer join on cohort assignment data (key: patient/member id)
   *   4. get latest member attribution records along with their PCP id and set as key; provider id
   *   5. left outer join on providers (key: provider id)
   *   6. construct the (patientId, MemberCreateAndPublishRequest) tuple given all the data obtained
   *
   * @param ids              identifiers associated with source project
   * @param patientIndex     patient/member data
   * @param members          gold member data
   * @param providers        gold provider data
   */
  def goldClaimsMembersToPublishRequests(
    ids: MultiPartnerIds,
    patientIndex: SCollection[(String, MemberInsuranceRecord)],
    members: SCollection[Member],
    providers: SCollection[Provider]
  ): SCollection[(String, MemberCreateAndPublishRequest)] = {
    import cityblock.transforms.Transform.NoneMinOptionLocalDateOrdering

    val providerIndexOption: SCollection[(Option[String], Provider)] = providers
      .withName("Convert provider key to Option")
      .map { provider =>
        (Some(provider.providerIdentifier.id), provider)
      }

    members
      .withName("Extract patientId from Option")
      .flatMap { member =>
        for (patientId <- member.identifier.patientId) yield (patientId, member)
      }
      .withName("Join with patient index")
      .join(patientIndex)
      .withName("Index by PCP id")
      .map {
        case (_, (member, patient)) =>
          val key = Option(member.attributions)
            .filter(_.nonEmpty)
            .flatMap(_.maxBy(_.date.from)(NoneMinOptionLocalDateOrdering).PCPId)

          (key, (member, patient))
      }
      .withName("Join with providers") // TODO we should really inline PCP in member
      .leftOuterJoin(providerIndexOption)
      .withName("Construct member attribution messages")
      .flatMap {
        case (_, ((member, patient), pcp)) =>
          Option(member.eligibilities)
            .filter(_.nonEmpty)
            .map { eligibilities =>
              val demographics: Demographics = Demographics(
                firstName = member.demographics.identity.firstName,
                middleName = member.demographics.identity.middleName,
                lastName = member.demographics.identity.lastName,
                dateOfBirth =
                  member.demographics.date.birth.map(ISODateTimeFormat.yearMonthDay().print),
                dateOfDemise =
                  member.demographics.date.death.map(ISODateTimeFormat.yearMonthDay().print),
                sex = member.demographics.identity.gender,
                gender = member.demographics.identity.gender,
                ethnicity = None,
                race = None,
                language = member.demographics.identity.primaryLanguage,
                maritalStatus = member.demographics.identity.maritalStatus,
                ssn = member.demographics.identity.SSN,
                ssnLastFour = member.demographics.identity.SSN.map(_.takeRight(4)),
                addresses = List(Address(
                  id = None,
                  addressType = None,
                  street1 = member.demographics.location.address1,
                  street2 = member.demographics.location.address2,
                  city = member.demographics.location.city,
                  zip = member.demographics.location.zip,
                  county = member.demographics.location.county,
                  state = member.demographics.location.state,
                  spanDateStart = None,
                  spanDateEnd = None
                )),
                phones = member.demographics.location.phone
                  .map(phone => Phone(id = None, phone = phone, phoneType = None))
                  .toList,
                emails = member.demographics.location.email
                  .map(email => Email(id = None, email = email))
                  .toList,
                updatedBy = None
              )

              val latestEligibility =
                eligibilities.maxBy(_.date.from)(NoneMinOptionLocalDateOrdering)
              val plan = Plan(
                externalId = member.identifier.partnerMemberId,
                rank = None,
                current = None,
                details = List(Detail(
                  lineOfBusiness = latestEligibility.detail.lineOfBusiness.map(_.toLowerCase),
                  subLineOfBusiness = latestEligibility.detail.subLineOfBusiness,
                  spanDateStart =
                    latestEligibility.date.from.map(ISODateTimeFormat.yearMonthDay().print),
                  spanDateEnd =
                    latestEligibility.date.to.map(ISODateTimeFormat.yearMonthDay().print),
                ))
              )
              val insurance = Insurance(carrier = member.identifier.partner, plans = List(plan))

              val medicaidInsurance = List.empty[Insurance]

              val (pcpName, pcpAddress, pcpPhone) = getPcpInfo(pcp)

              val request = MemberCreateAndPublishRequest(
                demographics = demographics,
                partner = Option(member.identifier.partner),
                insurances = List(insurance),
                medicaid = medicaidInsurance,
                medicare = List.empty[Insurance],
                pcpAddress = pcpAddress,
                pcpName = pcpName,
                pcpPhone = pcpPhone,
                pcpPractice = None,
                cohortId = None,
                categoryId = None,
                primaryPhysician = None,
                caregiverPractice = None,
                elationPlan = None,
                createElationPatient = None,
                marketId = ids.market,
                partnerId = ids.partner,
                clinicId = ids.clinic
              )

              (patient.memberId, request)
            }
      }
  }

  private def getPcpInfo(pcp: Option[Provider]): (Option[String], Option[String], Option[String]) =
    pcp.fold[(Option[String], Option[String], Option[String])]((None, None, None)) { provider =>
      val name = provider.name
      val clinicAddress = provider.locations.map(_.clinic).head
      val address = clinicAddress.map { addr =>
        Seq(addr.address1, addr.address2, addr.city, addr.state, addr.zip).flatten.mkString(" ")
      }
      val phone = clinicAddress.flatMap(_.phone)
      (name, address, phone)
    }
}
