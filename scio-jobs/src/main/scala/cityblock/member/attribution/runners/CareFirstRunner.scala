package cityblock.member.attribution.runners

import cityblock.member.attribution.transforms.CareFirstTransformer
import cityblock.member.service.io.{CreateAndPublishMemberSCollection, PublishMemberSCollection}
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.member.silver.CareFirst.CareFirstMember
import cityblock.utilities.Environment
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}

class CareFirstRunner extends GenericRunner {
  def query(
    bigQueryProject: String,
    bigQueryDataset: String
  ): String = {
    val silverMemberMonthTable = s"member_month"
    val silverMemberDemoTable = s"member_demographics"
    val silverProviderTable = s"provider"

    s"""
       |#standardsql
       |WITH
       |
       |insurance AS (
       |  SELECT
       |    mm.data.MEM_ID AS externalId,
       |    ARRAY_AGG(STRUCT(
       |        mm.DATA.SPAN_FROM_DATE AS spanDateStart,
       |        mm.data.SPAN_TO_DATE AS spanDateEnd,
       |        CASE
       |          WHEN mm.DATA.LOB = "400" THEN "medicaid"
       |        END AS lineOfBusiness,
       |        CASE
       |          WHEN mm.DATA.LOB = "400" THEN "supplemental security income"
       |        END AS subLineOfBusiness
       |    )) AS insuranceDetails
       |  FROM
       |    `$bigQueryProject.$bigQueryDataset.$silverMemberMonthTable` mm
       |  GROUP BY
       |    externalId ),
       |
       |latestLine AS (
       |  SELECT
       |    mm.DATA.MEM_ID AS externalId,
       |    MAX(mm.DATA.SPAN_FROM_DATE) AS latestSpanStart,
       |  FROM
       |    `$bigQueryProject.$bigQueryDataset.$silverMemberMonthTable` mm
       |  GROUP BY
       |    externalId)
       |
       |SELECT
       |  md.patient,
       |  md.DATA.*,
       |  insurance.*,
       |  silver_provider_query.pcpName,
       |  silver_provider_query.pcpPhone,
       |  silver_provider_query.pcpPractice,
       |  silver_provider_query.pcpAddress
       |FROM
       |  `$bigQueryProject.$bigQueryDataset.$silverMemberMonthTable` mm
       |INNER JOIN
       |  `$bigQueryProject.$bigQueryDataset.$silverMemberDemoTable` md
       |ON
       |  mm.DATA.MEM_ID = md.DATA.MEM_ID
       |LEFT JOIN (
       |  SELECT
       |    DISTINCT silver_provider.DATA.PROV_ID,
       |    CONCAT(silver_provider.DATA.PROV_FNAME, " ", silver_provider.DATA.PROV_LNAME) AS pcpName,
       |    silver_provider.DATA.PROV_PHONE AS pcpPhone,
       |    silver_provider.DATA.PROV_CLINIC_NAME AS pcpPractice,
       |    CONCAT(silver_provider.DATA.PROV_CLINIC_ADDR, ", ", silver_provider.DATA.PROV_CLINIC_ADDR2, ", ", silver_provider.DATA.PROV_CLINIC_CITY, ", ", silver_provider.DATA.PROV_CLINIC_STATE, " ", silver_provider.DATA.PROV_CLINIC_ZIP) AS pcpAddress
       |  FROM
       |    `$bigQueryProject.$bigQueryDataset.$silverProviderTable` silver_provider
       |) silver_provider_query
       |ON
       |  mm.DATA.ATTRIBUTED_PCP_ID = silver_provider_query.PROV_ID
       |LEFT JOIN
       |  insurance
       |ON
       |  mm.DATA.MEM_ID = insurance.externalId
       |INNER JOIN
       |  latestLine
       |ON
       |  (mm.DATA.MEM_ID = latestLine.externalId AND mm.DATA.SPAN_FROM_DATE = latestLine.latestSpanStart)
       |ORDER BY
       |  mm.patient.patientId
       |""".stripMargin
  }

  def deployOnScio(
    bigQuerySrc: String,
    cohortId: Option[Int],
    env: Environment,
    multiPartnerIds: MultiPartnerIds
  ): ScioContext => ScioResult = { sc: ScioContext =>
    val careFirstMembers: SCollection[CareFirstMember] =
      sc.typedBigQuery[CareFirstMember](bigQuerySrc)

    val (existingMembers, newMembers) =
      careFirstMembers.partition(_.patient.patientId.isDefined)

    newMembers
      .map(
        careFirstMember =>
          CareFirstTransformer
            .createAndPublishRequest(careFirstMember, multiPartnerIds, "carefirst", cohortId)
      )
      .createAndPublishToMemberService(env)

    existingMembers
      .map(
        careFirstMember =>
          (
            careFirstMember.patient.patientId.get,
            CareFirstTransformer
              .createAndPublishRequest(careFirstMember, multiPartnerIds, "carefirst", None)
        )
      )
      .updateAndPublishToMemberService(env)

    sc.close().waitUntilDone()
  }
}
