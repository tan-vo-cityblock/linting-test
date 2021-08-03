package cityblock.importers.generic.config

import org.scalatest.{FlatSpec, Matchers, OptionValues}

class BigQueryFieldTest extends FlatSpec with Matchers with OptionValues {

  "BigQueryField" should "disallow RECORD type" in {
    val field = BigQueryField("name", fieldType = BigQueryFieldType.RECORD)
    an[IllegalArgumentException] should be thrownBy field.validate()
  }

  "BigQueryField.normalize" should "normalize numeric types" in {
    val input = "43.675819"
    val bqField = BigQueryField("latitude", fieldType = BigQueryFieldType.NUMERIC)
    bqField.normalize(input).value shouldBe "43.675819"
  }

  "BigQueryField.normalizeBoolean" should "normalize true boolean values" in {
    val inputList = List("true", "t")
    val bqField = BigQueryField("isLegacy", fieldType = BigQueryFieldType.BOOLEAN)
    inputList.foreach { bqField.normalize(_).value shouldBe "true" }
  }

  "BigQueryField.normalizeBoolean" should "normalize false boolean values" in {
    val inputList = List("false", "f")
    val bqField = BigQueryField("isLegacy", fieldType = BigQueryFieldType.BOOLEAN)
    inputList.foreach { bqField.normalize(_).value shouldBe "false" }
  }

  "BigQueryField.normalizeBoolean" should "return None for non-booleans" in {
    val input = "not a boolean"
    val bqField = BigQueryField("isLegacy", fieldType = BigQueryFieldType.BOOLEAN)
    bqField.normalize(input) shouldBe None
  }

  "BigQueryField.normalize" should "handle yyyyMM" in {
    val input = "201908"
    BigQueryField("name", fieldType = BigQueryFieldType.DATE)
      .normalize(input)
      .value shouldBe "2019-08-01"
    BigQueryField("name", fieldType = BigQueryFieldType.DATETIME)
      .normalize(input)
      .value shouldBe "2019-08-01T00:00:00"
    BigQueryField("name", fieldType = BigQueryFieldType.TIME)
      .normalize(input)
      .value shouldBe "00:00:00"
    BigQueryField("name", fieldType = BigQueryFieldType.TIMESTAMP)
      .normalize(input)
      .value shouldBe "1564617600"
  }

  "BigQueryField.normalize" should "handle timestamp pattern" in {
    val input = "2019-08-19 18:14:03"
    BigQueryField("name", fieldType = BigQueryFieldType.DATE)
      .normalize(input)
      .value shouldBe "2019-08-19"
    BigQueryField("name", fieldType = BigQueryFieldType.DATETIME)
      .normalize(input)
      .value shouldBe "2019-08-19T18:14:03"
    BigQueryField("name", fieldType = BigQueryFieldType.TIME)
      .normalize(input)
      .value shouldBe "18:14:03"
    BigQueryField("name", fieldType = BigQueryFieldType.TIMESTAMP)
      .normalize(input)
      .value shouldBe "1566238443"
  }

  "BigQueryField.normalize" should "leave non-time field untouched" in {
    val input = "201908"
    BigQueryField("name", fieldType = BigQueryFieldType.STRING)
      .normalize(input)
      .value shouldBe input
  }

  "BigQueryField.normalize" should "return None for 'empty' inputs" in {
    val input = "    "
    BigQueryField("stringField", fieldType = BigQueryFieldType.STRING)
      .normalize(input) shouldBe None
    BigQueryField("dateField", fieldType = BigQueryFieldType.DATE)
      .normalize(input) shouldBe None
  }

  "BigQueryField.normalize" should "return leave whitespace untouched if flag set" in {
    val input = "    "
    BigQueryField("stringField", fieldType = BigQueryFieldType.STRING)
      .normalize(input, trimWhitespace = false)
      .value shouldBe "    "
  }

  "BigQueryField.normalizeDate" should "normalize Postgres dates with ms" in {
    val input = "2019-12-18 17:49:12.54345678+0500"
    BigQueryField("date", fieldType = BigQueryFieldType.DATE)
      .normalize(input)
      .value shouldBe "2019-12-18"
    BigQueryField("date", fieldType = BigQueryFieldType.DATETIME)
      .normalize(input)
      .value shouldBe "2019-12-18T17:49:12"
    BigQueryField("date", fieldType = BigQueryFieldType.TIME)
      .normalize(input)
      .value shouldBe "17:49:12"
    BigQueryField("date", fieldType = BigQueryFieldType.TIMESTAMP)
      .normalize(input)
      .value shouldBe "1576691352"
  }

  "BigQueryField.normalizeDate" should "normalize Postgres dates no ms" in {
    val input = "2019-12-18 17:49:12+0500"
    BigQueryField("date", fieldType = BigQueryFieldType.DATE)
      .normalize(input)
      .value shouldBe "2019-12-18"
    BigQueryField("date", fieldType = BigQueryFieldType.DATETIME)
      .normalize(input)
      .value shouldBe "2019-12-18T17:49:12"
    BigQueryField("date", fieldType = BigQueryFieldType.TIME)
      .normalize(input)
      .value shouldBe "17:49:12"
    BigQueryField("date", fieldType = BigQueryFieldType.TIMESTAMP)
      .normalize(input)
      .value shouldBe "1576691352"
  }

  "BigQueryField.normalizeDate" should "normalize Postgres truncated date" in {
    val input = "2019-12-18 17:49:12+00"
    BigQueryField("date", fieldType = BigQueryFieldType.DATE)
      .normalize(input)
      .value shouldBe "2019-12-18"
    BigQueryField("date", fieldType = BigQueryFieldType.DATETIME)
      .normalize(input)
      .value shouldBe "2019-12-18T17:49:12"
    BigQueryField("date", fieldType = BigQueryFieldType.TIME)
      .normalize(input)
      .value shouldBe "17:49:12"
    BigQueryField("date", fieldType = BigQueryFieldType.TIMESTAMP)
      .normalize(input)
      .value shouldBe "1576691352"
  }

  "BigQueryField.normalizeDate" should "be None for faulty dates" in {
    val input = "not a date"
    val bqField = BigQueryField("date", fieldType = BigQueryFieldType.DATE)
    bqField.normalize(input) shouldBe None
  }

}
