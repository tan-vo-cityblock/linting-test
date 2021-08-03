package cityblock.importers.common

import cityblock.member.service.io._
import cityblock.member.service.api.MemberCreateRequest
import cityblock.models.gold.Claims.{MemberDemographics, MemberEligibility}
import cityblock.utilities.{Environment, Loggable, PartnerConfiguration}
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions

object PatientIndexImporter extends Loggable {

  @BigQueryType.toTable
  case class CohortIngestionRequest(memberId: String,
                                    categoryId: Option[Int],
                                    demographics: Option[MemberDemographics],
                                    eligibilities: List[MemberEligibility])

  //scalastyle:off
  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[GcpOptions](cmdlineArgs)

    val requestLocation = args.required("requestLocation")
    val partner = args.required("partner")
    val cohortId = args.required("cohortId").toInt
    val persist = args.boolean("persist", false)

    implicit val environment: Environment = Environment(cmdlineArgs)

    val partnerConfig = PartnerConfiguration.partners.get(partner) match {
      case Some(pc) => pc
      case _ =>
        logger.error(s"Error fetching partner configuration for $partner")
        sys.exit(255)
    }

    val partnerIndexName = partnerConfig.indexName
    val memberId =
      if (partnerConfig.indexName == "emblem") "Member_ID" else "NMI"

    val query = s"""|#standardsql
                    |SELECT
                    |  cohortLoad.${memberId} as memberId,
                    |  cohortLoad.category as categoryId,
                    |  m.*
                    |FROM
                    |`$requestLocation` AS cohortLoad
                    |left JOIN
                    |  `${partnerConfig.productionProject}.gold_claims.Member` AS m
                    |ON
                    |  cohortLoad.${memberId} = m.identifier.partnerMemberId
                    |""".stripMargin

    val sc = ScioContext(opts)

    val fullMembers = sc
      .typedBigQuery[CohortIngestionRequest](query)
      .collect {
        case CohortIngestionRequest(
            memberId,
            categoryId,
            Some(demographics),
            _
            ) =>
          MemberCreateRequest(
            partner = partnerIndexName,
            insuranceId = memberId,
            cohortId = Option(cohortId),
            categoryId = categoryId,
            demographics = demographics,
          )
      }

    // Filter out the existing external ids that are already stored, to make this job idempotent when rerun.
    val existingMembers = sc
      .readInsurancePatients(Option(partnerIndexName))
      .map { case (_, patient) => (patient.externalId, patient) }

    // Currently, we only persist a single insurance ID at the time of load. Although the member service supports multiple IDs at ingestion time,
    // we still have yet to change our underlying structure in BQ to reflect this requestz.
    val persistMembers = fullMembers
      .map { fullMember =>
        (fullMember.insurances.head.plans.head.externalId, fullMember)
      }
      .leftOuterJoin(existingMembers)
      .collect {
        case (_, (createdMember, None)) => createdMember
      }

    if (persist) {
      persistMembers.writeToMemberIndex
    } else {
      logger.info("--persist wasn't specified, printing debugging information")
      persistMembers.debug(prefix = "Member Index Entry: ")
    }
    sc.close.waitUntilDone()
  }
}
