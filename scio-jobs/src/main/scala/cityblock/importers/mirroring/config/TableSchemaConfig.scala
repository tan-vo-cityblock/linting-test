package cityblock.importers.mirroring.config

import cityblock.importers.generic.FailedParseContainer.FailedParse
import cityblock.importers.generic.config.BigQueryField
import com.google.api.services.bigquery.model.TableSchema
import org.joda.time.LocalDate

trait TableSchemaConfig {
  val sourceBucket: String
  val databaseName: String
  val tableNames: List[String]
  val shardDate: LocalDate

  def parseSchemaLine(schemaLine: String): BigQueryField
  def parseLine(
    tableName: String,
    line: String,
    schemaFields: List[BigQueryField]
  ): Either[FailedParse, Map[String, String]]
  def getTableSchema(schemaFields: List[BigQueryField]): TableSchema

  val DATE_FORMAT = "yyyyMMdd"

  def dataFileLocation(tableName: String): String =
    s"gs://$sourceBucket/${databaseName}_export_${tableName}_data_${shardDate.toString(DATE_FORMAT)}"
  def schemaFileLocation(tableName: String): String =
    s"gs://$sourceBucket/${databaseName}_export_${tableName}_schema_${shardDate.toString(DATE_FORMAT)}"
}
