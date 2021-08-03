package cityblock.utilities.reference

import com.spotify.scio.bigquery.types.BigQueryType

package object tables {

  @BigQueryType.toTable
  case class ICD10Cm(code: String, name: String)
  object ICD10Cm extends MappingTable[ICD10Cm] with ValidCodesTable {
    override val dataset: String = "codesets"
    override val table: String = "icd10cm"
    override val codeField: String = "code"
  }

  @BigQueryType.toTable
  case class LineOfBusiness(BEN_PKG_ID: String, lineOfBusiness1: String, lineOfBusiness2: String)
  object LineOfBusiness extends MappingTable[LineOfBusiness] {
    override val dataset: String = "payers"
    override val table: String = "emblem_lob_packageId_map"
  }

  @BigQueryType.toTable
  case class HCPCS(code: String, summary: Option[String], description: Option[String])
  object HCPCS extends MappingTable[HCPCS] {
    override val dataset: String = "codesets"
    override val table: String = "hcpcs"
    override def queryAll: String = s"""
                                    |SELECT
                                    |  hcpcs.HCPCS AS code,
                                    |  hcpcs.DESCRIPTION AS summary,
                                    |  hcpcs_full.concept_name AS description
                                    |FROM
                                    |  `$project.$dataset.$table` hcpcs
                                    |JOIN
                                    |  `$project.$dataset.hcpcs_full` hcpcs_full
                                    |
                                    |ON
                                    |  hcpcs.HCPCS = hcpcs_full.concept_code
    """.stripMargin
  }

  @BigQueryType.toTable
  case class ProviderSpecialtyMappings(code: String, medicareSpecialty: Option[String])
  object ProviderSpecialtyMappings extends MappingTable[ProviderSpecialtyMappings] {
    override val dataset: String = "codesets"
    override val table: String = "taxonomy_medicare_specialties"
    override def queryAll: String =
      s"""
         | SELECT distinct code, medicareSpecialty
         | FROM `$project.$dataset.$table`
         |""".stripMargin
  }

  @BigQueryType.toTable
  case class PlaceOfService(name: String, code: String)
  object PlaceOfService extends MappingTable[PlaceOfService] with ValidCodesTable {
    override val dataset: String = "codesets"
    override val table: String = "place_of_service"
    override val codeField: String = "code"
    override def queryAll: String = s"${super.queryAll} WHERE name IS NOT NULL AND code IS NOT NULL"

    val padTo: Int = 2
  }

  @BigQueryType.toTable
  case class TypeOfService(code: String, description: String)
  object TypeOfService extends MappingTable[TypeOfService] with ValidCodesTable {
    override val dataset: String = "codesets"
    override val table: String = "type_of_service"
    override val codeField: String = "code"
  }

  object DiagnosisRelatedGroup extends ValidCodesTable {
    override val dataset: String = "claims_mappings"
    override val table: String = "cu_drg_codes"
    override val codeField: String = "drg_code"

    val padTo: Int = 3
  }

  object RevenueCode extends ValidCodesTable {
    override val dataset: String = "claims_mappings"
    override val table: String = "cu_rev_codes"
    override val codeField: String = "rev_code"

    val padTo: Int = 4
  }

  object TypeOfBill extends ValidCodesTable {
    override val dataset: String = "claims_mappings"
    override val table: String = "cu_type_of_bill"
    override val codeField: String = "type_of_bill_code"

    val padTo: Int = 4
  }
}
