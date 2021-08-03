package cityblock.importers.generic.config

import cityblock.importers.generic.FailedParseContainer.FailedParse
import io.circe.Decoder
import io.circe.generic.extras.Configuration
import io.circe.generic.extras.semiauto.deriveDecoder
import kantan.csv.rfc
import kantan.csv.ops._

case class CsvFileConfig(
  sourceBucket: String,
  prefixDir: String,
  fileNamePattern: String,
  partnerName: String,
  fileType: String,
  tableName: String,
  fields: List[BigQueryField],
  // The header line of the CSV. This has to match up with the fields above.
  header: String,
  // The delimiter that separates values in a row. Defined as a string here and transformed to a character below to
  // account for partners who use multi-character delimiters.
  delimiter: String,
  // Fields in the CSV file that should be ignored entirely.
  ignoreFields: List[String] = List(),
  override val indexJoinField: Option[String] = None,
  override val datasource: Option[String] = None,
  override val dataFieldName: String = "data",
  override val createIds: Boolean = true,
  override val dataType: Option[String] = None
) extends PartnerInputFileConfig {
  require(fileType.equalsIgnoreCase("csv"))

  // Needed since Emblem sometimes uses multi character delimiters.
  val delimiterChar: Char = {
    if (delimiter.length == 1) delimiter.toCharArray.head else '|'
  }

  private val headerFieldNames = {
    val maybeFixedHeader: String = if (delimiter.toCharArray.head == delimiterChar) {
      header
    } else {
      header.replaceAll(delimiter, delimiterChar.toString)
    }

    maybeFixedHeader
      .split(delimiterChar)
      .toList
      .map(normalizeHeaderField)
      .map(modifyFieldForBackwardCompatiblity)
  }

  private val csvParseConfig = rfc.withCellSeparator(delimiterChar)

  override def validate(): Unit = {
    super.validate()

    val allFieldNames =
      (fieldNamesToFields.keys ++ ignoreFields).toList

    val missingFieldNames = headerFieldNames.diff(allFieldNames)
    val unexpectedFieldNames = allFieldNames.diff(headerFieldNames)

    require(
      missingFieldNames.isEmpty,
      s"Missing fields defined in header:\n${missingFieldNames.mkString("\n")}"
    )

    require(
      unexpectedFieldNames.isEmpty,
      s"Unexpected fields not defined in header:\n${unexpectedFieldNames.mkString("\n")}"
    )

    val intersection =
      ignoreFields.intersect(fieldNamesToFields.keys.toList)
    require(
      intersection.isEmpty,
      s"Ignored fields are also defined as BigQuery schema fields:\n${intersection
        .mkString("\n")}"
    )
  }

  def parseLine(line: String): Either[FailedParse, Map[String, String]] = {
    val fixedLine = if (delimiter.toCharArray.head == delimiterChar) {
      line
    } else {
      line.replaceAll(delimiter, delimiterChar.toString)
    }

    fixedLine
      .asCsvReader[List[String]](csvParseConfig)
      .next match {
      case Right(split) if split.length == headerFieldNames.length =>
        // Join up the parsed pieces to their respective field names from the header, filter out the ones that should be
        // ignored, and convert the result to a map.
        val parsedMap = (headerFieldNames zip split).filter {
          case (fieldName, _) =>
            ignoreFields.isEmpty || !ignoreFields.contains(fieldName)
        }.toMap
        // The fields are normalized again when creating a TableRow, but we do it first here because it makes it easier
        // to validate the row.
        val normalizedMap = normalizeValues(parsedMap)

        val missingRequiredFields = getMissingRequiredFields(normalizedMap)
        if (missingRequiredFields.isEmpty) {
          Right(normalizedMap)
        } else {
          Left(
            FailedParse(
              tableName,
              s"Parsed line missing required fields: ${missingRequiredFields.mkString(", ")}",
              line
            )
          )
        }
      case _ => Left(FailedParse(tableName, "Failed to parse input", line))
    }
  }
}

object CsvFileConfig {
  implicit val customConfig: Configuration = Configuration.default.withDefaults
  implicit val csvFileConfigDecoder: Decoder[CsvFileConfig] =
    deriveDecoder[CsvFileConfig]
}
