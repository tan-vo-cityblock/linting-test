package cityblock.importers.generic.config

import cats.syntax.functor._
import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.SilverIdentifier
import cityblock.transforms.Transform
import cityblock.utilities.Loggable
import com.google.api.services.bigquery.model.{TableFieldSchema, TableRow, TableSchema}
import com.spotify.scio.bigquery.types.BigQueryType
import com.google.cloud.storage.Storage.BlobListOption
import com.google.cloud.storage.{Bucket, Storage, StorageOptions}

import collection.JavaConverters._
import io.circe.Decoder

trait PartnerInputFileConfig extends Loggable {
  // The GCS bucket that contains the source file(s)
  val sourceBucket: String
  // The "directory" containing the source file(s) in the GCS bucket
  val prefixDir: String
  // The source filename. This may have one string placeholder (`%s`), for the delivery date
  val fileNamePattern: String
  // The canonical name of the partner that this data belongs to. If doing a join against the member
  // index, this must correspond to a defined partner there.
  val partnerName: String
  // The type of the file to process
  val fileType: String
  // The (unsharded) name in BigQuery of the table to store the parsed data into
  val tableName: String
  // The list of fields that the data will correspond to in each BigQuery row
  val fields: List[BigQueryField]
  // If specified, this is the field on which to join against the member index. When this is
  // done, the resulting record will have a `patient` subrecord containing the Patient from
  // the member index.
  val indexJoinField: Option[String] = None
  // If specified, this is the field that tells us which datasource to use, in case there are multiple IDs
  val datasource: Option[String] = None
  // Whether or not each row that is created should get an id minted for it. This will result
  // in the actual data being nested in a subrecord of the row.
  val createIds: Boolean = true
  // The name of subrecord to store the actual parsed data in, when `createIds` is true.
  // This primarily exists for legacy reasons, new imports should not set this.
  val dataFieldName: String = "data"
  val dataType: Option[String] = None

  val fieldNamesToFields: Map[String, BigQueryField] =
    fields.map(f => (f.name, f)).toMap
  protected val requiredFieldNames: List[String] =
    fields.filter(_.required).map(_.name)

  import PartnerInputFileConfig._

  def validate(): Unit = {
    logger.info(s"Validating config for table: $tableName")
    if (indexJoinField.isDefined) {
      require(
        fieldNamesToFields.keySet.contains(indexJoinField.get),
        "indexJoinField is not a defined field"
      )
      require(createIds, "indexJoinField can't be specified if createIds is false")
    }

    require(fields.nonEmpty, "At least one field must be defined")

    fields.foreach(_.validate())
  }

  def getMissingRequiredFields(fieldMap: Map[String, String]): List[String] =
    requiredFieldNames.diff(fieldMap.keys.toList)

  def normalizeValues(fieldMap: Map[String, String]): Map[String, String] =
    fieldMap
      .map {
        case (fieldName, value) =>
          (fieldName, fieldNamesToFields(fieldName).normalize(value))
      }
      .filter { case (_, value) => value.isDefined }
      .mapValues(_.get)

  // BigQuery only accepts header names with letters, numbers, or underscores
  def normalizeHeaderField(field: String): String =
    field
      .replaceAll(" ", "_")
      .replaceAll("/", "_or_")
      .replaceAll("-", "_")
      .replaceAll("#", "Number")
      .replaceAll("\\(", "")
      .replaceAll("\\)", "")
      .replaceAll("\"", "")
      .replaceAll("$", "")

  // Use when partner changes field name but we want consistency in the name of the field in our BigQuery tables.
  def modifyFieldForBackwardCompatiblity(field: String): String =
    if (partnerName == "emblem") {
      field.replaceAll("MEM_COUNTY", "COUNTY")
    } else {
      field
    }

  def tableSchema: TableSchema = {
    val dataFields = fields.map(_.tableFieldSchema).asJava

    if (createIds) {
      new TableSchema().setFields(
        (List(identifierSchema)
          ++ (if (indexJoinField.isDefined) {
                List(patientSchema)
              } else {
                Nil
              })
          ++ List(
            new TableFieldSchema()
              .setName(dataFieldName)
              .setType(BigQueryFieldType.RECORD)
              .setMode(BigQueryFieldMode.REQUIRED)
              .setFields(dataFields)
          )).asJava
      )
    } else {
      new TableSchema().setFields(dataFields)
    }
  }

  def matchPrefixForFiles(
    files: Iterable[String],
    filePrefix: String,
    sourceBucket: String
  ): String = {
    val filePath: String = {
      val matchedFiles: Iterable[String] = files.filter(
        fileName =>
          fileName.endsWith(".txt") || fileName.endsWith(".csv") || fileName.endsWith(".TXT")
      )
      if (matchedFiles.size > 1) {
        throw new IllegalStateException(s"Multiple files matched for prefix $filePrefix.")
      }
      if (matchedFiles.size < 1) {
        throw new NoSuchElementException(s"No file on GCS with prefix $filePrefix.")
      }
      matchedFiles.head
    }

    s"gs://$sourceBucket/$filePath"
  }

  def getInputFilePath(deliveryDate: String): String = {
    val filePrefix: String = prefixDir + "/" + fileNamePattern.format(deliveryDate)
    val storage: Storage = StorageOptions.getDefaultInstance.getService
    val bucket: Bucket = storage.get(sourceBucket)
    val blobs = bucket.list(BlobListOption.prefix(filePrefix)).iterateAll().asScala.map(_.getName)

    matchPrefixForFiles(blobs, filePrefix, sourceBucket)
  }

  def toTableRow(fieldMap: Map[String, String], member: Option[Patient]): TableRow = {
    val normalizedValues: Map[String, String] = normalizeValues(fieldMap)
    val dataRow = normalizedValues.foldLeft(new TableRow()) {
      case (table, (fieldName, value)) => table.set(fieldName, value)
    }

    if (createIds) {
      val row =
        new TableRow()
          .set("identifier", makeIdentifierRow())
          .set(dataFieldName, dataRow)

      if (member.isDefined) {
        row.set("patient", BigQueryType.toTableRow[Patient](member.get))
      } else {
        row
      }
    } else {
      dataRow
    }
  }
}

object PartnerInputFileConfig {
  implicit val decodeEvent: Decoder[PartnerInputFileConfig] =
    List[Decoder[PartnerInputFileConfig]](
      Decoder[CsvFileConfig].widen
    ).reduceLeft(_ or _)

  private val identifierSchema: TableFieldSchema = new TableFieldSchema()
    .setName("identifier")
    .setType(BigQueryFieldType.RECORD)
    .setMode(BigQueryFieldMode.REQUIRED)
    .setFields(BigQueryType.schemaOf[SilverIdentifier].getFields)

  private val patientSchema: TableFieldSchema =
    new TableFieldSchema()
      .setName("patient")
      .setType(BigQueryFieldType.RECORD)
      .setMode(BigQueryFieldMode.REQUIRED)
      .setFields(BigQueryType.schemaOf[Patient].getFields)

  private def makeIdentifierRow(): TableRow =
    new TableRow().set("surrogateId", Transform.generateUUID())
}
