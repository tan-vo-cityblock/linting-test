package cityblock.member.resolution.emblem.io

import java.util.UUID

import cityblock.member.resolution.models.NodeEntity
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.BigQueryType

object EmblemMembers {

  val queryMemberServiceMirror: String =
    """
      |#standardsql
      |SELECT
      |  mdi.memberId as id,
      |  mdi.externalId,
      |  ds.name as datasource
      |FROM
      |  `cbh-db-mirror-prod.member_index_mirror.member_insurance` mdi
      |LEFT JOIN
      |  `cbh-db-mirror-prod.member_index_mirror.datasource` ds
      |ON
      |  mdi.datasourceId = ds.id
      |WHERE
      |  mdi.deletedAt is null AND
      |  ds.id in (1, 8, 10, 11, 12, 13, 14, 15) AND
      |  mdi.memberId NOT IN ('bc6faebc-58e1-11eb-a1c5-42010a8e0140')
      |""".stripMargin

  def fetchData(project: String): Iterator[Member] = {
    val bq = BigQuery(project)
    bq.getTypedRows[Member](queryMemberServiceMirror)
  }

  @BigQueryType.toTable
  case class Member(id: String, datasource: String, externalId: String) {
    def toNodeEntity: (NodeEntity[String], UUID) = {
      val id = NodeEntity(groupName = this.datasource, this.externalId)
      val label = UUID.fromString(this.id)
      (id, label)
    }
  }

  def toNodeEntity(members: List[Member]): Iterator[(NodeEntity[String], UUID)] =
    members.toIterator.map { member =>
      val id = NodeEntity(groupName = member.datasource, member.externalId)
      val label = UUID.fromString(member.id)
      (id, label)
    }
}
