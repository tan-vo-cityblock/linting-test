package cityblock.member.attribution.runners

import cityblock.member.attribution.transforms.EmblemVirtualTransformer
import cityblock.member.service.io.{CreateAndPublishMemberSCollection, PublishMemberSCollection}
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.member.silver.EmblemVirtual.EmblemVirtualMember
import cityblock.utilities.Environment
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}
import org.joda.time.LocalDate

class EmblemVirtualRunner extends GenericRunner {

  def query(
    bigQueryProject: String,
    bigQueryDataset: String
  ): String = {
    val silverMemberEnrollTable = s"member_enroll"
    val silverMemberDemoTable = s"member_demographics"
    val currentDate = new LocalDate().toString

    // TODO: We need to modify this query to aggregate historical insurance context from member month file
    s"""
       |#standardsql
       |SELECT
       |  silver_member_enroll.patient,
       |  silver_member_demo.demographic.*
       |FROM `$bigQueryProject.$bigQueryDataset.$silverMemberEnrollTable` silver_member_enroll
       |JOIN `$bigQueryProject.$bigQueryDataset.$silverMemberDemoTable` silver_member_demo
       |ON silver_member_demo.patient.externalId = silver_member_enroll.patient.externalId
       |WHERE silver_member_enroll.data.TERM_DATE > "$currentDate"
     """.stripMargin
  }

  def deployOnScio(
    bigQuerySrc: String,
    cohortId: Option[Int],
    env: Environment,
    multiPartnerIds: MultiPartnerIds
  ): ScioContext => ScioResult = { sc: ScioContext =>
    val members: SCollection[EmblemVirtualMember] =
      sc.typedBigQuery[EmblemVirtualMember](bigQuerySrc)

    val (existingMembers, newMembers) = members.partition(_.patient.patientId.isDefined)

    newMembers
      .map(
        emblemVirtualMember =>
          EmblemVirtualTransformer
            .createAndPublishRequest(emblemVirtualMember, multiPartnerIds, "emblem", cohortId)
      )
      .createAndPublishToMemberService(env)

    existingMembers
      .map(
        emblemVirtualMember =>
          (
            emblemVirtualMember.patient.patientId.get,
            EmblemVirtualTransformer
              .createAndPublishRequest(emblemVirtualMember, multiPartnerIds, "emblem", None)
        ))
      .updateAndPublishToMemberService(env)

    sc.close().waitUntilDone()
  }
}
