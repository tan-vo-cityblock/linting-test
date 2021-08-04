package cityblock.member.silver
import cityblock.member.service.models.PatientInfo.Patient
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object CareFirst {
  @BigQueryType.toTable
  case class CareFirstInsuranceDetails(
    lineOfBusiness: Option[String],
    subLineOfBusiness: Option[String],
    spanDateStart: Option[LocalDate],
    spanDateEnd: Option[LocalDate],
  )

  @BigQueryType.toTable
  case class CareFirstMember(
    patient: Patient,
    insuranceDetails: List[CareFirstInsuranceDetails],
    MEM_ID: String,
    MEM_QUAL: Option[String],
    MEM_GENDER: Option[String],
    MEM_SSN: Option[String],
    MEM_LNAME: Option[String],
    MEM_FNAME: Option[String],
    MEM_MNAME: Option[String],
    MEM_DOB: Option[LocalDate],
    MEM_DOD: Option[LocalDate],
    MEM_MEDICARE: Option[String],
    MEM_MEDICAID: Option[String],
    MEM_ADDR1: Option[String],
    MEM_ADDR2: Option[String],
    MEM_CITY: Option[String],
    MEM_COUNTY: Option[String],
    MEM_STATE: Option[String],
    MEM_ZIP: Option[String],
    MEM_EMAIL: Option[String],
    MEM_PHONE_PRIMARY: Option[String],
    MEM_PHONE_SECONDARY: Option[String],
    MEM_PHONE_PRIMARY_TYPE: Option[String],
    MEM_SECONDARY_TYPE: Option[String],
    MEM_RACE: Option[String],
    MEM_ETHNICITY: Option[String],
    MEM_LANGUAGE: Option[String],
    MEM_MARITAL: Option[String],
    PROD_DESC: Option[String],
    pcpName: Option[String],
    pcpPhone: Option[String],
    pcpPractice: Option[String],
    pcpAddress: Option[String]
  ) {
    def getProductDescription: Option[String] =
      this.PROD_DESC.map { _.split(" ").filterNot(_.exists(_.isDigit)).mkString(" ") }
  }
}
