package cityblock.member.attribution.runners

import cityblock.member.attribution.transforms.TuftsTransformer
import cityblock.member.service.io.{CreateAndPublishMemberSCollection, PublishMemberSCollection}
import cityblock.member.silver.Tufts.TuftsMember
import com.spotify.scio.{ScioContext, ScioResult}
import com.spotify.scio.bigquery._
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.utilities.Environment
import com.spotify.scio.values.SCollection

class TuftsRunner extends GenericRunner {
  def query(bigQueryProject: String, bigQueryDataset: String): String = {
    val silverMemberDailyTable = s"Member_Daily"
    val silverProviderTable = s"Provider"
    s"""
       |#standardsql
       |WITH
       |
       |insurance AS (
       |  SELECT
       |    md.DATA.CarrierSpecificMemID AS externalId,
       |    ARRAY_AGG(STRUCT( md.DATA.ProductEnrStartDate AS spanDateStart,
       |        md.DATA.ProductEnrEndDate AS spanDateEnd,
       |        CASE
       |          WHEN md.DATA.InsuranceTypeCode = "IC" THEN "medicare"
       |          WHEN md.DATA.InsuranceTypeCode = "MC" THEN "medicaid"
       |          WHEN md.DATA.InsuranceTypeCode = "HM" THEN "commercial"
       |      END
       |        AS lineOfBusiness,
       |        CASE
       |          WHEN md.DATA.InsuranceTypeCode = "IC" THEN "dual"
       |          WHEN md.DATA.InsuranceTypeCode = "MC" THEN "MCO"
       |          WHEN md.DATA.InsuranceTypeCode = "HM" THEN "direct"
       |      END
       |        AS subLineOfBusiness)) AS insuranceDetails
       |  FROM
       |    `tufts-data.silver_claims.$silverMemberDailyTable` md
       |  GROUP BY
       |    externalId ),
       |
       |latestLine AS (
       |  SELECT
       |    md.DATA.CarrierSpecificMemID AS externalId,
       |    MAX(md.DATA.ProductEnrStartDate) AS latestSpanStart,
       |  FROM
       |    `tufts-data.silver_claims.$silverMemberDailyTable` md
       |  GROUP BY
       |    externalId)
       |
       |SELECT
       |  md.patient,
       |  md.DATA.* EXCEPT (CarrierSpecificMemID,
       |    ProductEnrStartDate,
       |    ProductEnrEndDate),
       |  insurance.*,
       |  CONCAT(pr.DATA.First_Name, " ", pr.DATA.Last_Name) AS pcpName,
       |  pr.DATA.Provider_Telephone AS pcpPhone,
       |  pr.DATA.Street_Address1_Name AS pcpPractice,
       |  CONCAT(pr.DATA.Street_Address2_Name, " ", pr.DATA.City_Name, " ", pr.DATA.State_Code, " ", pr.DATA.Zip_Code) AS pcpAddress
       |FROM
       |  `tufts-data.silver_claims.$silverMemberDailyTable` md
       |LEFT JOIN
       |  `tufts-data.silver_claims.$silverProviderTable` pr
       |ON
       |  md.DATA.PCP_ID = pr.DATA.Plan_Provider_ID
       |LEFT JOIN
       |  insurance
       |ON
       |  md.DATA.CarrierSpecificMemID = insurance.externalId
       |INNER JOIN
       |  latestLine
       |ON
       |  (md.DATA.CarrierSpecificMemID = latestLine.externalId AND md.DATA.ProductEnrStartDate = latestLine.latestSpanStart)
       |ORDER BY
       |  md.patient.patientId
       |""".stripMargin
  }

  def deployOnScio(
    bigQuerySrc: String,
    cohortId: Option[Int],
    env: Environment,
    multiPartnerIds: MultiPartnerIds
  ): ScioContext => ScioResult = { sc: ScioContext =>
    val members: SCollection[TuftsMember] = sc.typedBigQuery[TuftsMember](bigQuerySrc)

    val (existingMembers, newMembers) = members.partition(_.patient.patientId.isDefined)

    newMembers
      .map(
        tuftsMember =>
          TuftsTransformer.createAndPublishRequest(tuftsMember, multiPartnerIds, "tufts", cohortId)
      )
      .createAndPublishToMemberService(env)

    existingMembers
      .map(
        tuftsMember =>
          (
            tuftsMember.patient.patientId.get,
            TuftsTransformer.createAndPublishRequest(tuftsMember, multiPartnerIds, "tufts", None)
        )
      )
      .updateAndPublishToMemberService(env)

    sc.close().waitUntilDone()
  }
}
