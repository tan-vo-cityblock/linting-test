package cityblock.importers.mirroring.config

import cityblock.importers.generic.config.{BigQueryField, BigQueryFieldType}
import cityblock.importers.generic.FailedParseContainer.FailedParse
import cityblock.importers.generic.config.BigQueryFieldType.BigQueryFieldType
import com.google.api.services.bigquery.model.TableSchema
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.Storage
import org.joda.time.LocalDate
import kantan.csv.rfc
import kantan.csv.ops._

import collection.JavaConverters._
import scala.util.{Failure, Success, Try}

case class CsvPostgresTableConfig(
  sourceBucket: String,
  databaseName: String,
  tableNames: List[String],
  shardDate: LocalDate,
  delimiter: Char = ','
) extends TableSchemaConfig {
  import CsvPostgresTableConfig._
  private val csvParseConfig = rfc.withCellSeparator(delimiter).withoutHeader

  @throws[IllegalStateException]
  def parseSchemaLine(schemaLine: String): BigQueryField =
    schemaLine.asCsvReader[List[String]](csvParseConfig).next match {
      // Successful parsing case
      case Right(line) if line.length == SCHEMA_LINE_LENGTH =>
        val fieldName :: fieldType :: isNullable :: _ = line
        val bqFieldType = postgresToBigQueryTypes.getOrElse(fieldType, BigQueryFieldType.STRING)
        val isRequired = isNullable.equalsIgnoreCase("no")
        BigQueryField(fieldName, None, bqFieldType, isRequired)

      // Unexpected number of fields
      case Right(line) =>
        throw new IllegalStateException(
          s"Unexpected number of fields in schema file [example line: $line]")

      // Parsing error
      case Left(readError) =>
        throw new IllegalStateException(
          s"Parsing error on schema file [error: ${readError.getMessage}]")
    }

  def parseLine(
    tableName: String,
    line: String,
    schemaFields: List[BigQueryField]
  ): PostgresParseResult =
    Try {
      line.asCsvReader[List[String]](csvParseConfig).next match {
        case Right(split) if split.length == schemaFields.length =>
          val parsedMap: Map[BigQueryField, String] = (schemaFields zip split).toMap
          val normalizedFields: Map[String, String] = parsedMap
            .map { case (field, value) => (field, field.normalize(value, trimWhitespace = false)) }
            .collect { case (key, Some(value)) => key.name -> value }

          validateNormalizedFields(tableName, line, schemaFields, normalizedFields)

        case _ =>
          val parsingErrorMsg =
            s"Unexpected number of parsed elements [schema elements: ${schemaFields.length}]"
          Left(FailedParse(tableName, parsingErrorMsg, line))
      }
    } match {
      case Success(postgresParseResult) => postgresParseResult
      case Failure(ex)                  => Left(FailedParse(tableName, ex.getMessage, line))
    }

  def getTableSchema(schemaFields: List[BigQueryField]): TableSchema = {
    val dataFields = schemaFields.map(_.tableFieldSchema).asJava
    new TableSchema().setFields(dataFields)
  }

  private def validateNormalizedFields(
    tableName: String,
    line: String,
    schemaFields: List[BigQueryField],
    normalizedFields: Map[String, String]
  ): PostgresParseResult = {
    val requiredFieldNames: Seq[String] = schemaFields.collect {
      case field if field.required => field.name
    }
    val missingRequiredFields: Seq[String] = requiredFieldNames.diff(normalizedFields.keys.toList)
    val errorMsg = s"Parsed line missing required fields: ${missingRequiredFields.mkString(", ")}"

    Either.cond(
      missingRequiredFields.isEmpty,
      normalizedFields,
      FailedParse(tableName, errorMsg, line)
    )
  }

}

object CsvPostgresTableConfig {
  type PostgresParseResult = Either[FailedParse, Map[String, String]]

  private val SCHEMA_LINE_LENGTH = 3

  private lazy val postgresToBigQueryTypes: Map[String, BigQueryFieldType] =
    Map(
      "boolean" -> BigQueryFieldType.BOOLEAN,
      "uuid" -> BigQueryFieldType.STRING,
      "text" -> BigQueryFieldType.STRING,
      "integer" -> BigQueryFieldType.INTEGER,
      "bigint" -> BigQueryFieldType.INTEGER,
      "smallint" -> BigQueryFieldType.INTEGER,
      "numeric" -> BigQueryFieldType.NUMERIC,
      "date" -> BigQueryFieldType.DATE,
      "timestamp with time zone" -> BigQueryFieldType.TIMESTAMP
    )

  def verifyAllFilesPresent(
    config: CsvPostgresTableConfig,
    gcsStorageService: Storage,
  ): Unit =
    config.tableNames.foreach { tableName: String =>
      val bucketName = config.sourceBucket
      val dataFile = config.dataFileLocation(tableName)
      val schemaFile = config.schemaFileLocation(tableName)
      val bucket = gcsStorageService.get(bucketName)
      val schemaBlob = bucket.list(BlobListOption.prefix(schemaFile))
      val dataBlob = bucket.list(BlobListOption.prefix(dataFile))
      require(schemaBlob.iterateAll.iterator.hasNext,
              s"Missing schema file: [bucket: $bucket, file: $schemaFile]")
      require(dataBlob.iterateAll.iterator.hasNext,
              s"Missing data file: [bucket: $bucket, file: $dataFile]")
    }
}
