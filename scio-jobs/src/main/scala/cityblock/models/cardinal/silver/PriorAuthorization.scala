package cityblock.models.cardinal.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object PriorAuthorization {
  @BigQueryType.toTable
  case class ParsedPriorAuthorization(
    AuthorizationID: String,
    MemberID: String,
    MemberName: Option[String],
    MemberDOB: Option[LocalDate],
    LineofBusiness: Option[String],
    SubLineofBusiness: Option[String],
    ContractNumber: Option[String],
    ProviderID: Option[String],
    ReferringProviderNPI: Option[String],
    ReferringProviderName: Option[String],
    ServicingProviderNPI: Option[String],
    ServicingProviderName: Option[String],
    ParStatus: Option[String],
    FacilityNPI: Option[String],
    FacilityName: Option[String],
    FacilityAddress1: Option[String],
    FacilityAddress2: Option[String],
    FacilityCity: Option[String],
    FacilityState: Option[String],
    FacilityZip: Option[String],
    DiagnosisCode: Option[String],
    DiagnosisDescription: Option[String],
    AuthorizationStatus: Option[String],
    StatusReason: Option[String],
    Priority: Option[String],
    AdmitDate: Option[LocalDate],
    DischargeDate: Option[LocalDate],
    ProcedureCode: Option[String],
    RequestDateTime: Option[String],
    InpatOutpat: Option[String],
    PlaceOfService: Option[String],
    TypeOfService: Option[String],
    ServiceStartDate: Option[LocalDate],
    ServiceEndDate: Option[LocalDate],
    RequestedUnits: Option[String],
    ApprovedUnits: Option[String],
    LastUpdateDate: Option[String],
    Cardinal_ID: Option[String]
  )
}
