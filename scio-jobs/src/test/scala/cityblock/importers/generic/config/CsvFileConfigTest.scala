package cityblock.importers.generic.config

import org.scalatest.{FlatSpec, Matchers}

class CsvFileConfigTest extends FlatSpec with Matchers {

  // scalastyle:off
  private def makeTestConfig(sourceBucket: String = "sourceBucket",
                             prefixDir: String = "prefixDir",
                             fileNamePattern: String = "fileNamePattern_%s.txt",
                             partnerName: String = "ConnectiCare",
                             fileType: String = "csv",
                             tableName: String = "myTable",
                             indexJoinField: Option[String] = None,
                             fields: List[BigQueryField] = List(BigQueryField("stringField")),
                             header: String = "stringField",
                             delimiter: String = "~|~",
                             ignoreFields: List[String] = List(),
                             dataType: Option[String] = None) =
    CsvFileConfig(sourceBucket,
                  prefixDir,
                  fileNamePattern,
                  partnerName,
                  fileType,
                  tableName,
                  fields,
                  header,
                  delimiter,
                  ignoreFields,
                  indexJoinField,
                  dataType = dataType)
  // scalastyle:on

  "CsvFileConfigForTest" should "validate by default" in {
    val config = makeTestConfig()
    config.validate()
  }

  "CsvFileConfig" should "require fileType to be csv" in {
    an[IllegalArgumentException] should be thrownBy makeTestConfig(fileType = "other")
  }

  "CsvFileConfig.validate" should "call super.validate()" in {
    // The check that indexJoinField be mentioned in the fields is in PartnerInputFileConfig
    val config = makeTestConfig(indexJoinField = Some("other"))
    an[IllegalArgumentException] should be thrownBy config.validate()
  }

  "CsvFileConfig.validate" should "count ignoreFields as mentioned" in {
    val config = makeTestConfig(header = "stringField|other",
                                fields = List(BigQueryField("stringField")),
                                ignoreFields = List("other"))
    config.validate()
  }

  "CsvFileConfig.validate" should "check for missing header fields" in {
    val config = makeTestConfig(header = "stringField|other")
    an[IllegalArgumentException] should be thrownBy config.validate()
  }

  "CsvFileConfig.validate" should "check for unexpected fields" in {
    val config = makeTestConfig(fields = List(BigQueryField("stringField"), BigQueryField("other")))
    an[IllegalArgumentException] should be thrownBy config.validate()
  }

  "CsvFileConfig.validate" should "check that ignored fields and defined fields don't overlap" in {
    val config = makeTestConfig(header = "stringField|other",
                                fields = List(BigQueryField("stringField"), BigQueryField("other")),
                                ignoreFields = List("other"))
    an[IllegalArgumentException] should be thrownBy config.validate()
  }

  "CsvFileConfig.parseLine" should "successfully parse valid line" in {
    val config = makeTestConfig(
      header = "first|second|third",
      fields = List(BigQueryField("first"), BigQueryField("second"), BigQueryField("third")))
    val expected =
      Right(Map(("first", "12"), ("second", "words!"), ("third", "something")))
    config.parseLine("12|words!|something") should be(expected)
  }

  "CsvFileConfig.parseLine" should "skip ignored fields in result" in {
    val config =
      makeTestConfig(header = "first|second|third",
                     fields = List(BigQueryField("first"), BigQueryField("third")),
                     ignoreFields = List("second"))
    val expected = Right(Map(("first", "12"), ("third", "something")))
    config.parseLine("12|words!|something") should be(expected)
  }

  "CsvFileConfig.parseLine" should "reject malformed line" in {
    val config = makeTestConfig(
      header = "first|second|third",
      fields = List(BigQueryField("first"), BigQueryField("second"), BigQueryField("third")))
    config.parseLine("aslnfasjbf3,43,m43") shouldBe a[Left[_, _]]
  }

  "CsvFileConfig.parseLine" should "reject line if it doesn't match up with the header" in {
    val config = makeTestConfig(
      header = "first|second|third",
      fields = List(BigQueryField("first"), BigQueryField("second"), BigQueryField("third")))
    config.parseLine("12|words!|something|another one?") shouldBe a[Left[_, _]]
  }

  "CsvFileConfig.parseLine" should "reject line if required field is normalized to empty" in {
    val config = makeTestConfig(header = "first|second|third",
                                fields = List(BigQueryField("first"),
                                              BigQueryField("second", required = true),
                                              BigQueryField("third")))

    config.parseLine("12|  |something") shouldBe a[Left[_, _]]
  }

  "CsvFileConfig.parseLine" should "skip empty fields" in {
    val config = makeTestConfig(
      header = "first|second|third",
      fields = List(BigQueryField("first"), BigQueryField("second"), BigQueryField("third")))
    val expected = Right(Map(("first", "12"), ("third", "something")))
    config.parseLine("12||something") should be(expected)
  }
}
