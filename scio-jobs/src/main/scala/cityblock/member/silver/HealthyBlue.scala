package cityblock.member.silver

import cityblock.member.service.models.PatientInfo.Patient
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object HealthyBlue {

  @BigQueryType.toTable
  case class HealthyBlueMember(
    patient: Patient,
    MemberId: String,
    MedicaidId: Option[String],
    EnrollmentDate: Option[LocalDate],
    TerminationDate: Option[LocalDate],
    LineOfBusiness: Option[String],
    SubLineOfBusiness: Option[String],
    LastName: Option[String],
    FirstName: Option[String],
    MiddleName: Option[String],
    DOB: Option[LocalDate],
    DOD: Option[LocalDate],
    MBI: Option[String],
    GenderCode: Option[String],
    RaceCode1: Option[String],
    RaceCode2: Option[String],
    EthnicityCode: Option[String],
    PrimaryLanguageCode: Option[String],
    AddressLine1: Option[String],
    AddressLine2: Option[String],
    City: Option[String],
    State: Option[String],
    PostalCode: Option[String],
    County: Option[String],
    PrimaryPhone: Option[String],
    PCPName: Option[String],
    PCPAddressLine1: Option[String],
    PCPAddressLine2: Option[String],
    PCPCity: Option[String],
    PCPState: Option[String],
    PCPPostalCode: Option[String],
    PCPPhone: Option[String],
    ClinicId: String
  )
}
