package cityblock.member.resolution.emblem.io

import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.BigQueryType
import com.spotify.scio.bigquery.{CREATE_IF_NEEDED, WRITE_TRUNCATE}
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}

object EmblemMemberDiscovery {

  @BigQueryType.toTable
  case class MemberDiscovery(memberId: String, externalId: String, carrier: String)

  implicit final class MemberDiscoveryIO(@transient private val self: List[MemberDiscovery]) {

    def writeToBQ(project: Option[String] = None,
                  dataset: Option[String] = None,
                  table: Option[String] = None,
                  shard: Option[String] = None,
                  writeDisposition: WriteDisposition = WRITE_TRUNCATE,
                  createDisposition: CreateDisposition = CREATE_IF_NEEDED): Unit = {

      val p: String = project.getOrElse("emblem-data")
      val d: String = dataset.getOrElse("qa")
      val t: String = table.getOrElse("member_discovery")

      val tableShard = shard.map(date => s"${table}_$date").getOrElse(t)

      val bq = BigQuery(p)
      bq.writeTypedRows[MemberDiscovery](s"$p:$d.$tableShard",
                                         self,
                                         writeDisposition = writeDisposition,
                                         createDisposition = createDisposition)
    }
  }
}
