package cityblock.importers.mirroring.config

import cityblock.importers.generic.FailedParseContainer.FailedParse
import cityblock.importers.generic.config.{BigQueryField, BigQueryFieldType}
import org.joda.time.LocalDate
import org.scalatest.Inside._
import org.scalatest.{FlatSpec, Matchers}
import org.scalatestplus.mockito.MockitoSugar
import resource._

import scala.io.Source

class CsvTableSchemaConfigTest extends FlatSpec with Matchers with MockitoSugar {

  var source: Source = _
  val (tableName1, tableName2) = ("testTable1", "testTable2")

  private def makeCsvTableSchemaConfig(
    sourceBucket: String = "source-bucket",
    databaseName: String = "testDatabase",
    tableNames: List[String] = List(tableName1, tableName2),
    shard: LocalDate = LocalDate.now(),
    delimiter: Char = ','
  ) =
    CsvPostgresTableConfig(
      sourceBucket,
      databaseName,
      tableNames,
      shard,
      delimiter
    )

  private def mkStringL(file: String): List[String] =
    managed(Source.fromString(file)).acquireAndGet(_.getLines().toList)

  "CsvTableSchemaConfig.parseSchemaLine" should "parse a valid schema" in {
    val config = makeCsvTableSchemaConfig()

    val schema =
      """id,uuid,NO
        |field1,bigint,YES
        |field2,integer,NO
        |field3,text,YES
        |field4,date,NO
        |field5,timestamp with time zone,YES
        |field6,boolean,NO""".stripMargin

    val expectedOutput = List(
      BigQueryField("id", None, BigQueryFieldType.STRING, required = true),
      BigQueryField("field1", None, BigQueryFieldType.INTEGER),
      BigQueryField("field2", None, BigQueryFieldType.INTEGER, required = true),
      BigQueryField("field3", None, BigQueryFieldType.STRING),
      BigQueryField("field4", None, BigQueryFieldType.DATE, required = true),
      BigQueryField("field5", None, BigQueryFieldType.TIMESTAMP),
      BigQueryField("field6", None, BigQueryFieldType.BOOLEAN, required = true)
    )

    mkStringL(schema).map(config.parseSchemaLine) shouldBe expectedOutput
  }

  "CsvTableSchemaConfig.parseSchemaLine" should "error when not enough fields" in {
    val config = makeCsvTableSchemaConfig()
    val schema =
      """id,uuid,NO
        |field1,bigint""".stripMargin

    val errorMsg =
      "Unexpected number of fields in schema file [example line: List(field1, bigint)]"
    the[IllegalStateException] thrownBy {
      mkStringL(schema).map(config.parseSchemaLine)
    } should have message errorMsg
  }

  "CsvTableSchemaConfig.parseLine" should "parse a valid line given a schema" in {
    val config = makeCsvTableSchemaConfig()

    val schemaFields = List(
      BigQueryField("id", None, BigQueryFieldType.STRING, required = true),
      BigQueryField("field1", None, BigQueryFieldType.INTEGER),
      BigQueryField("field2", None, BigQueryFieldType.BOOLEAN, required = true)
    )

    val line = "d6c6b9cc-4596-4ff7-b802-eb8a67d3801c,1,true"
    val expectedOutput = Map[String, String](
      "id" -> "d6c6b9cc-4596-4ff7-b802-eb8a67d3801c",
      "field1" -> "1",
      "field2" -> "true"
    )

    config.parseLine(tableName1, line, schemaFields) shouldBe Right(expectedOutput)
  }

  "CsvTableSchemaConfig.parseLine" should "parse a line with whitespace correctly" in {
    val config = makeCsvTableSchemaConfig()

    val schemaFields = List(
      BigQueryField("id", None, BigQueryFieldType.STRING, required = true),
      BigQueryField("field1", None, BigQueryFieldType.STRING, required = true),
      BigQueryField("field2", None, BigQueryFieldType.BOOLEAN)
    )

    val line = "d6c6b9cc-4596-4ff7-b802-eb8a67d3801c,  , false"
    val expectedOutput = Map[String, String](
      "id" -> "d6c6b9cc-4596-4ff7-b802-eb8a67d3801c",
      "field1" -> "  ",
      "field2" -> "false"
    )

    config.parseLine(tableName1, line, schemaFields) shouldBe Right(expectedOutput)
  }

  "CsvTableSchemaConfig.parseLine" should "nullify a bad date" in {
    val config = makeCsvTableSchemaConfig()

    val schemaFields = List(
      BigQueryField("id", None, BigQueryFieldType.STRING, required = true),
      BigQueryField("field1", None, BigQueryFieldType.DATE),
      BigQueryField("field2", None, BigQueryFieldType.BOOLEAN, required = true)
    )

    val line = "d6c6b9cc-4596-4ff7-b802-eb8a67d3801c,This is not a valid date.,true"
    val expectedOutput = Map[String, String](
      "id" -> "d6c6b9cc-4596-4ff7-b802-eb8a67d3801c",
      "field2" -> "true"
    )

    config.parseLine(tableName1, line, schemaFields) shouldBe Right(expectedOutput)
  }

  "CsvTableSchemaConfig.parseLine" should "parse a line with non-required fields missing and a bad date" in {
    val config = makeCsvTableSchemaConfig()

    val schemaFields = List(
      BigQueryField("id", None, BigQueryFieldType.STRING, required = true),
      BigQueryField("field1", None, BigQueryFieldType.DATE),
      BigQueryField("field2", None, BigQueryFieldType.BOOLEAN)
    )

    val line = "d6c6b9cc-4596-4ff7-b802-eb8a67d3801c,This is not a valid date.,"
    val expectedOutput = Map[String, String](
      "id" -> "d6c6b9cc-4596-4ff7-b802-eb8a67d3801c",
    )

    config.parseLine(tableName1, line, schemaFields) shouldBe Right(expectedOutput)
  }

  "CsvTableSchemaConfig.parseLine" should "fail parsing if missing a required field" in {
    val config = makeCsvTableSchemaConfig()

    val schemaFields = List(
      BigQueryField("id", None, BigQueryFieldType.STRING, required = true),
      BigQueryField("field1", None, BigQueryFieldType.INTEGER),
      BigQueryField("field2", None, BigQueryFieldType.BOOLEAN, required = true)
    )

    val line = "d6c6b9cc-4596-4ff7-b802-eb8a67d3801c,1,"
    val errorMsg = s"Parsed line missing required fields: field2"

    inside(config.parseLine(tableName2, line, schemaFields)) {
      case Left(FailedParse(_tableName, _errorMsg, _line, _)) =>
        _tableName shouldEqual tableName2
        _errorMsg shouldEqual errorMsg
        _line shouldEqual line
      case Right(_) =>
        fail("test should not be passing when missing required fields")
    }
  }

  "CsvTableSchemaConfig.parseLine" should "fail parsing if discrepancy in number of fields" in {
    val config = makeCsvTableSchemaConfig()

    val schemaFields = List(
      BigQueryField("id", None, BigQueryFieldType.STRING, required = true),
      BigQueryField("field1", None, BigQueryFieldType.INTEGER),
      BigQueryField("field2", None, BigQueryFieldType.BOOLEAN, required = true)
    )

    val line = "d6c6b9cc-4596-4ff7-b802-eb8a67d3801c,1,,234,,434,2,4"
    val errorMsg = "Unexpected number of parsed elements [schema elements: 3]"

    inside(config.parseLine(tableName2, line, schemaFields)) {
      case Left(FailedParse(_tableName, _errorMsg, _line, _)) =>
        _tableName shouldEqual tableName2
        _errorMsg shouldEqual errorMsg
        _line shouldEqual line
      case Right(_) =>
        fail("test should not be passing with number of fields discrepancy")
    }
  }

}
