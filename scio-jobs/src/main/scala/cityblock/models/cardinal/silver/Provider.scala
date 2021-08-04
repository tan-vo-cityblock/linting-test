package cityblock.models.cardinal.silver

import com.spotify.scio.bigquery.types.BigQueryType

object Provider {
  @BigQueryType.toTable
  case class ParsedProvider(
    ProviderID: Option[String],
    NPI: Option[String],
    DEA: Option[String],
    ProviderName: Option[String],
    FedTaxID: Option[String],
    HospitalAffiliation: Option[String],
    SpecialityCode: Option[String],
    Speciality: Option[String],
    TaxonomyCode: Option[String],
    Taxonomy: Option[String],
    FacilityPhone: Option[String],
    FacilityFax: Option[String],
    WebSite: Option[String],
    ContractStatusCode: Option[String],
    ContractStatus: Option[String],
    ContractTypeCode: Option[String],
    ContractType: Option[String],
    EntityTypeCode: Option[String],
    EntityType: Option[String],
    AvailabilityStatusCode: Option[String],
    AvailabilityStatus: Option[String],
    Address1: Option[String],
    Address2: Option[String],
    City: Option[String],
    State: Option[String],
    ZipCode: Option[String],
    County: Option[String],
    MailAddress1: Option[String],
    MailAddress2: Option[String],
    MailCity: Option[String],
    MailState: Option[String],
    MailZipCode: Option[String],
    MailCounty: Option[String],
    BillAddress1: Option[String],
    BillAddress2: Option[String],
    BillCity: Option[String],
    BillState: Option[String],
    BillZipCode: Option[String],
    BillCounty: Option[String],
  )
}
