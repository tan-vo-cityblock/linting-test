package cityblock.member.resolution.emblem.io

import cityblock.member.resolution.models.NodeEntity
import com.spotify.scio.bigquery.client.BigQuery
import com.spotify.scio.bigquery.types.BigQueryType

object EmblemCrosswalk {

  @BigQueryType.toTable
  case class EmblemCrosswalkEntry(
    CIN: Option[String],
    LEGACY_MBR_ID: Option[String],
    FACETS_MBR_ID: Option[String],
  ) {

    def toNodeEntity: Seq[NodeEntity[String]] =
      Seq(this.CIN.map(new NodeEntity("medicaidNY", _)),
          this.LEGACY_MBR_ID.map(new NodeEntity("emblem", _)),
          this.FACETS_MBR_ID.map(new NodeEntity("emblem", _))).flatten

  }

  private def queryCrosswalkIds(dataset: String, tableShard: String): String =
    s"""
      |#standardsql
      |SELECT distinct
      |  CASE
      |    WHEN data.CIN = 'MS00000P' THEN null
      |    ELSE data.CIN
      |  END as CIN,
      |  data.LEGACY_MBR_ID as LEGACY_MBR_ID,
      |  data.FACETS_MBR_ID as FACETS_MBR_ID
      |FROM
      |  `emblem-data.$dataset.$tableShard`
      |""".stripMargin

  def fetchData(project: String,
                dataset: String,
                tableShard: String): Iterator[EmblemCrosswalkEntry] = {
    val bq = BigQuery(project)
    bq.getTypedRows[EmblemCrosswalkEntry](queryCrosswalkIds(dataset, tableShard))
  }
}
