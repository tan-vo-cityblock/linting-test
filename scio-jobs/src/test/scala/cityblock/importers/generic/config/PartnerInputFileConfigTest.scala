package cityblock.importers.generic.config

import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import com.google.api.services.bigquery.model.TableSchema
import com.spotify.scio.bigquery.TableRow
import org.scalatest.{FlatSpec, Matchers}

import collection.JavaConverters._

class PartnerInputFileConfigTest extends FlatSpec with Matchers {
  case class PartnerInputFileConfigForTest(
    sourceBucket: String = "sourceBucket",
    prefixDir: String = "prefixDix",
    fileNamePattern: String = "fileNamePattern_%s.txt",
    partnerName: String = "ConnectiCare",
    fileType: String = "csv",
    tableName: String = "myTable",
    fields: List[BigQueryField] = List(BigQueryField("stringField", None)),
    override val indexJoinField: Option[String] = None,
    override val createIds: Boolean = true
  ) extends PartnerInputFileConfig {}

  def ensureFieldName(tableSchema: TableSchema, fieldName: String, expectExists: Boolean): Unit = {
    val fieldExists =
      tableSchema.getFields.asScala.exists(_.getName === fieldName)
    fieldExists should be(expectExists)
  }

  "PartnerInputFileConfigForTest" should "validate by default" in {
    val config = PartnerInputFileConfigForTest()
    config.validate()
  }

  "PartnerInputFileConfig.validate" should "validate indexJoinField" in {
    val invalidConfig =
      PartnerInputFileConfigForTest(indexJoinField = Some("fakeField"))
    an[IllegalArgumentException] should be thrownBy invalidConfig.validate()

    val validConfig =
      PartnerInputFileConfigForTest(indexJoinField = Some("stringField"))
    validConfig.validate()
  }

  "PartnerInputFileConfig.validate" should "prevent indexJoinField when createIds is false" in {
    val invalidConfig =
      PartnerInputFileConfigForTest(indexJoinField = Some("stringField"), createIds = false)
    an[IllegalArgumentException] should be thrownBy invalidConfig.validate()
  }

  "PartnerInputFileConfig.validate" should "require at least one field" in {
    val config = PartnerInputFileConfigForTest(fields = List())
    an[IllegalArgumentException] should be thrownBy config.validate()
  }

  "PartnerInputFileConfig.validate" should "validate fields" in {
    val badField = new BigQueryField("badField", None) {
      override def validate(): Unit =
        throw new Error("This is a success!")
    }

    val config = PartnerInputFileConfigForTest(fields = List(badField))
    an[Error] should be thrownBy config.validate()
  }

  "PartnerInputFileConfig.containsAllRequiredFields" should "return field names that are missing" in {
    val config = PartnerInputFileConfigForTest(
      fields = List(
        BigQueryField("first", required = true),
        BigQueryField("second"),
        BigQueryField("third", required = true)
      )
    )

    val fieldMap = Map(("first", "firstVal"), ("second", "secondVal"))
    val expected = List("third")
    config.getMissingRequiredFields(fieldMap) should be(expected)
  }

  "PartnerInputFileConfig.containsAllRequiredFields" should "return empty list when all required fields are there" in {
    val config = PartnerInputFileConfigForTest(
      fields = List(
        BigQueryField("first", required = true),
        BigQueryField("second"),
        BigQueryField("third", required = true)
      )
    )

    val fieldMap = Map(("first", "firstVal"), ("third", "thirdVal"))
    val expected = List()
    config.getMissingRequiredFields(fieldMap) should be(expected)
  }

  "PartnerInputFileConfig.normalizeValues" should "normalize fields" in {
    val config =
      PartnerInputFileConfigForTest(fields = List(BigQueryField("first"), BigQueryField("second")))
    val fieldMap = Map(("first", "     a    "), ("second", "b "))
    val expected = Map(("first", "a"), ("second", "b"))
    config.normalizeValues(fieldMap) should be(expected)
  }

  "PartnerInputFileConfig.normalizeValues" should "filter out empty values" in {
    val config =
      PartnerInputFileConfigForTest(fields = List(BigQueryField("first"), BigQueryField("second")))
    val fieldMap = Map(("first", "  "), ("second", "b"))
    val expected = Map(("second", "b"))
    config.normalizeValues(fieldMap) should be(expected)
  }

  "PartnerInputFileConfig.tableSchema" should "include patient if indexJoinField is specified" in {
    val config =
      PartnerInputFileConfigForTest(indexJoinField = Some("stringField"))
    ensureFieldName(config.tableSchema, "patient", expectExists = true)
  }

  "PartnerInputFileConfig.tableSchema" should "omit patient if indexJoinField is not specified" in {
    val config = PartnerInputFileConfigForTest()
    ensureFieldName(config.tableSchema, "patient", expectExists = false)
  }

  "PartnerInputFileConfig.getInputFilePaths" should "get file with requested exact prefix and date" in {
    val config = PartnerInputFileConfigForTest()
    val filePrefix = config.prefixDir + "/" + config.fileNamePattern.format("20190819")
    val testFiles = Iterable("prefixDix/fileNamePattern_20190819.txt")
    val inputFilePaths = config.matchPrefixForFiles(testFiles, filePrefix, config.sourceBucket)
    inputFilePaths should be("gs://sourceBucket/prefixDix/fileNamePattern_20190819.txt")
  }

  "PartnerInputFileConfig.getInputFilePaths" should "get file with requested prefix and date" in {
    val config = PartnerInputFileConfigForTest()
    val filePrefix = config.prefixDir + "/" + config.fileNamePattern.format("20190819")
    val testFiles = Iterable("prefixDix/fileNamePattern_20190819121200.txt")
    val inputFilePaths = config.matchPrefixForFiles(testFiles, filePrefix, config.sourceBucket)
    inputFilePaths should be("gs://sourceBucket/prefixDix/fileNamePattern_20190819121200.txt")
  }

  "PartnerInputFileConfig.getInputFilePaths" should "throw exception where no file matches" in {
    val config = PartnerInputFileConfigForTest()
    val filePrefix = config.prefixDir + "/" + config.fileNamePattern.format("20190810")
    val testFiles = Iterable.empty
    a[NoSuchElementException] should be thrownBy config.matchPrefixForFiles(
      testFiles,
      filePrefix,
      config.sourceBucket
    )
  }

  "PartnerInputFileConfig.getInputFilePaths" should "throw an exception for multiple file matches" in {
    val config = PartnerInputFileConfigForTest()
    val filePrefix = config.prefixDir + "/" + config.fileNamePattern.format("20190819")
    val testFiles = Iterable(
      "prefixDix/fileNamePattern_20190819.txt",
      "prefixDix/fileNamePattern_20190819121200.txt"
    )
    an[IllegalStateException] should be thrownBy config.matchPrefixForFiles(
      testFiles,
      filePrefix,
      config.sourceBucket
    )
  }

  "PartnerInputFileConfig.toTableRow" should "include fields" in {
    val config = PartnerInputFileConfigForTest()
    val rowMap = Map(("stringField", "stringValue"))
    val dataField =
      config.toTableRow(rowMap, None).get("data").asInstanceOf[TableRow]
    dataField shouldNot be(null)
    dataField.get("stringField").asInstanceOf[String] should be("stringValue")
  }

  "PartnerInputFileConfig.toTableRow" should "include member when provided" in {
    val config = PartnerInputFileConfigForTest()
    val rowMap = Map(("stringField", "stringValue"))
    val patient = Patient(None, "externalId", Datasource("connecticare", None))
    val dataField =
      config
        .toTableRow(rowMap, Some(patient))
        .get("patient")
        .asInstanceOf[TableRow]
    dataField shouldNot be(null)
  }
}
