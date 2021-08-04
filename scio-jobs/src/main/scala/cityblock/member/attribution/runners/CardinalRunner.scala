package cityblock.member.attribution.runners

import cityblock.member.attribution.transforms.CardinalTransformer
import cityblock.member.attribution.types.Cardinal.InsuranceAndDate
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.member.silver.Cardinal.CardinalMember
import cityblock.utilities.Environment
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import com.spotify.scio.bigquery._
import cityblock.member.service.io.{CreateAndPublishMemberSCollection, PublishMemberSCollection}

class CardinalRunner extends GenericRunner {
  def query(bigQueryProject: String, bigQueryDataset: String): String =
    """SELECT
      |patient,
      |data.MemberID,
      |data.EnrollmentDate,
      |data.TerminationDate,
      |data.LineOfBusiness,
      |data.SubLineOfBusiness,
      |data.LastName,
      |data.FirstName,
      |data.DOB,
      |data.DOD,
      |data.SSN,
      |data.MBI,
      |data.MedicaidId,
      |data.AttributedProviderNPI,
      |data.LegalGuardian,
      |data.GenderId,
      |data.RaceCode,
      |data.EthnicityCode,
      |data.MaritialStatusCode,
      |data.PrimaryLanguageCode,
      |data.AddressLine1,
      |data.AddressLine2,
      |data.City,
      |data.State,
      |data.PostalCode,
      |data.County,
      |data.Email,
      |data.PrimaryPhone,
      |data.PrimaryPhoneType,
      |data.SecondaryPhone,
      |data.SecondaryPhoneType,
      |clinic.marketId as MarketId,
      |clinic.id as ClinicId,
      |duals.Dual_Elig as IsDualEligible
      |FROM
      |`cbh-cardinal-data.silver_claims.Member`
      |LEFT JOIN `cbh-cardinal-data.resources.Cardinal_Member_List_20210507` duals
      |ON CAST(duals.Cardinal_ID AS STRING) = data.MemberID
      |LEFT JOIN `cbh-cardinal-data.resources.zip_code_hub_mapping` mappings
      |ON CAST(mappings.Zip_Code AS STRING) = data.PostalCode
      |LEFT JOIN `cbh-db-mirror-prod.commons_mirror.clinic` clinic
      |ON mappings.clinic_id = id
      |WHERE mappings.clinic_id != '#N/A'""".stripMargin

  def deployOnScio(
    bqSrc: String,
    cohortId: Option[Int],
    env: Environment,
    multiPartnerIds: MultiPartnerIds
  ): ScioContext => ScioResult = { sc: ScioContext =>
    val members: SCollection[CardinalMember] = sc.typedBigQuery[CardinalMember](bqSrc)
    val (existingMembers, newMembers) = members.partition(_.patient.patientId.isDefined)

    def withInsuranceAndDates(
      members: SCollection[CardinalMember]
    ): SCollection[(CardinalMember, Iterable[InsuranceAndDate])] =
      members
        .groupBy(_.MemberID)
        .values
        .map(memberRows => {
          val insuranceAndDates: Iterable[InsuranceAndDate] = memberRows.map(row => {
            InsuranceAndDate(row.SubLineOfBusiness, row.EnrollmentDate, row.TerminationDate)
          })
          (memberRows.head, insuranceAndDates)
        })

    withInsuranceAndDates(newMembers)
      .map(
        cardinalMemberAndInsuranceDates =>
          CardinalTransformer
            .createAndPublishRequest(
              cardinalMemberAndInsuranceDates,
              multiPartnerIds,
              "cardinal",
              cohortId
          )
      )
      .createAndPublishToMemberService(env)

    withInsuranceAndDates(existingMembers)
      .map(
        cardinalMembers =>
          (
            cardinalMembers._1.patient.patientId.get,
            CardinalTransformer
              .createAndPublishRequest(cardinalMembers, multiPartnerIds, "cardinal", None)
        )
      )
      .updateAndPublishToMemberService(env)

    sc.close().waitUntilDone()
  }
}
