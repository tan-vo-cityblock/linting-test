package cityblock.member.silver
import cityblock.member.service.models.PatientInfo.Patient
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object Tufts {
  @BigQueryType.toTable
  case class TuftsInsuranceDetails(
    lineOfBusiness: Option[String],
    subLineOfBusiness: Option[String],
    spanDateStart: Option[LocalDate],
    spanDateEnd: Option[LocalDate],
  )

  @BigQueryType.toTable
  case class TuftsMember(
    patient: Patient,
    externalId: String,
    insuranceDetails: List[TuftsInsuranceDetails],
    InsuranceTypeCode: Option[String],
    MemberSSN: Option[String],
    Gender: Option[String],
    DOB: Option[LocalDate],
    City: Option[String],
    State: Option[String],
    Zip: Option[String],
    PrimaryRace: Option[String],
    PrimaryEthnicity: Option[String],
    LanguagePreference: Option[String],
    ProductID: Option[String],
    StreetAddress: Option[String],
    Address2: Option[String],
    DateOfDeath: Option[LocalDate],
    PrimaryPhone: Option[String],
    MaritalStatus: Option[String],
    MMIS: Option[String],
    MedicareCode: Option[String],
    MemberLastName: Option[String],
    MemberFirstName: Option[String],
    MemberMiddleInit: Option[String],
    pcpName: Option[String],
    pcpPhone: Option[String],
    pcpPractice: Option[String],
    pcpAddress: Option[String]
  ) {
    def getProductDescription: Option[String] =
      this.InsuranceTypeCode.map {
        case "IC" => "Tufts Health Unify"
        case "HM" => "Tufts Health Direct"
        case "MC" => "Tufts Healthy Together"
      }

    // TODO: Need to incorporate this into the member service
    def getRateCell: Option[String] =
      this.ProductID.map(s => s.replaceAll("Unify", ""))
  }
}
