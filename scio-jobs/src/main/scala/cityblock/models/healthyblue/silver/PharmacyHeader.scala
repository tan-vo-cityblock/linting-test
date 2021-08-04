package cityblock.models.healthyblue.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object PharmacyHeader {
  @BigQueryType.toTable
  case class ParsedPharmacyHeader(
    Transaction_Control_Number: String,
    Transaction_Count: Option[String],
    Claim_Header_Status_Code: Option[String],
    Date_of_Service: Option[LocalDate],
    Transaction_Code: Option[String],
    Service_Provider_ID: Option[String],
    Patient_ID: String,
    Patient_Date_Of_Birth: Option[LocalDate],
    Patient_Gender_Code: Option[String],
    Patient_Pregnancy_Indicator: Option[String],
    Diagnosis_Code_01: Option[String],
    Diagnosis_Code_02: Option[String],
    Diagnosis_Code_03: Option[String],
    Diagnosis_Code_04: Option[String],
    Diagnosis_Code_05: Option[String],
    Date_of_Receipt: Option[LocalDate],
    Date_of_Adjudication: Option[LocalDate],
    Date_of_Payment: Option[LocalDate],
    Billing_Provider_In_or_Out_Network: Option[String],
    Living_Region_Code: Option[String],
    Health_Plan: Option[String],
    Benefit_Plan: Option[String],
    Administrative_County_Code: Option[String],
    Residence_County_Code: Option[String],
    Eligibility_Code: Option[String],
    Family_Planning_Indicator: Option[String],
    Sub_Program_01: Option[String],
    Sub_Program_02: Option[String],
    Sub_Program_03: Option[String],
    Sub_Program_04: Option[String],
    Living_Arrangement_Code: Option[String],
    Tribal_Code: Option[String],
    Total_Claim_Charge_Amount: Option[String],
    Claim_Allowed_Amount: Option[String],
    Payers_Claim_Payment_Amount: Option[String]
  )
}
