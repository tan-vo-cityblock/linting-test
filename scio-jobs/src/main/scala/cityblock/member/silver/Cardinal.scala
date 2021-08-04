package cityblock.member.silver

import cityblock.member.service.models.PatientInfo.Patient
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object Cardinal {

  @BigQueryType.toTable
  case class CardinalMember(
    patient: Patient,
    MemberID: String,
    EnrollmentDate: Option[LocalDate],
    TerminationDate: Option[LocalDate],
    LineOfBusiness: Option[String],
    SubLineOfBusiness: Option[String],
    LastName: Option[String],
    FirstName: Option[String],
    DOB: Option[LocalDate],
    DOD: Option[LocalDate],
    SSN: Option[String],
    MBI: Option[String],
    MedicaidId: Option[String],
    AttributedProviderNPI: Option[String],
    GenderId: Option[String],
    RaceCode: Option[String],
    EthnicityCode: Option[String],
    MaritialStatusCode: Option[String], //[sic] from Cardinal
    PrimaryLanguageCode: Option[String],
    AddressLine1: Option[String],
    AddressLine2: Option[String],
    City: Option[String],
    State: Option[String],
    PostalCode: Option[String],
    County: Option[String],
    Email: Option[String],
    PrimaryPhone: Option[String],
    PrimaryPhoneType: Option[String],
    SecondaryPhone: Option[String],
    SecondaryPhoneType: Option[String],
    MarketId: String,
    ClinicId: String,
    IsDualEligible: Option[Int],
  )
}
