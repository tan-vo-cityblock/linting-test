package cityblock.models.healthyblue.silver

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object PharmacyLines {
  @BigQueryType.toTable
  case class ParsedPharmacyLine(
    Transaction_Control_Number: String,
    Line_Number: Option[String],
    Claim_Header_Status_Code: Option[String],
    Line_Status_Code: Option[String],
    Prescription_Number: Option[String],
    Product_Service_Or_NDC_Code: Option[String],
    Associated_Prescription_Service_Number: Option[String],
    Associated_Prescription_Service_Date: Option[String],
    Quantity_Dispensed: Option[String],
    Fill_Number: Option[String],
    Days_Supply: Option[String],
    Compound_Code: Option[String],
    Dispense_As_Written_or_Product_Selection_Code: Option[String],
    Prescription_Written_Date: Option[LocalDate],
    Unit_Dose_Indicator: Option[String],
    Originally_Prescribed_Product_Service_Code: Option[String],
    Level_Of_Service: Option[String],
    Priori_Authorization_Number_Submitted: Option[String],
    Dispensing_Status: Option[String],
    Compound_Type: Option[String],
    Prescribing_Provider_NPI: Option[String],
    Prescribing_Provider_Last_Name: Option[String],
    Prescribing_Provider_Phone_Number: Option[String],
    Prescribing_Provider_First_Name: Option[String],
    Prescribing_Provider_Address: Option[String],
    Prescribing_Provider_City: Option[String],
    Prescribing_Provider_State: Option[String],
    Prescribing_Provider_Zip_Or_Postal_Code: Option[String],
    Compound_Product_Or_NDC_Code: Option[String],
    Compound_Ingredient_Basis_Of_Cost_Determination: Option[String],
    Compound_Ingredient_Modifier_Code_Count: Option[String],
    Total_Claim_Charge_Amount: Option[String],
    Claim_Allowed_Amount: Option[String],
    Payers_Claim_Payment_Amount: Option[String]
  )
}
