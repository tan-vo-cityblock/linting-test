package cityblock.transforms.tufts.gold

import cityblock.member.service.models.PatientInfo.{Datasource, Patient}
import cityblock.models.SilverIdentifier
import cityblock.models.TuftsSilverClaims.SilverPharmacy
import cityblock.models.tufts.silver.Pharmacy.ParsedPharmacy
import com.spotify.scio.testing.PipelineSpec

class PharmacyTransformerTest extends PipelineSpec {
  val identifier: SilverIdentifier = SilverIdentifier("surrogateId", None)

  val patient: Patient = Patient(None, "externalId", Datasource("tufts"))

  def makeData(
    quantity: Option[String],
    allowed: Option[String],
    billed: Option[String],
    cob: Option[String],
    copay: Option[String],
    deduct: Option[String],
    coin: Option[String],
    planPaid: Option[String]
  ): ParsedPharmacy =
    ParsedPharmacy(
      Submitter = None,
      Plan_ID = None,
      Insurance_Type_Code = None,
      Payer_Claim_Control_Number = None,
      Line_Counter = None,
      Version_Number = None,
      Insured_Group_or_Policy_Number = None,
      Filler1 = None,
      Plan_Specific_Contract_Number = None,
      Member_Suffix_or_Sequence_Number = None,
      Filler2 = None,
      Individual_Relationship_Code = None,
      Member_Gender = None,
      Member_Date_of_Birth = None,
      Filler3 = None,
      Member_State = None,
      Member_ZIP_Code = None,
      Date_Service_Approved = None,
      Pharmacy_Number = None,
      Pharmacy_Tax_ID_Number = None,
      Pharmacy_Name = None,
      National_Pharmacy_ID_Number = None,
      Pharmacy_Location_City = None,
      Pharmacy_Location_State = None,
      Pharmacy_ZIP_Code = None,
      Pharmacy_Country_Code = None,
      Claim_Status = None,
      Drug_Code = None,
      Drug_Name = None,
      New_Prescription_or_Refill = None,
      Generic_Drug_Indicator = None,
      Dispense_as_Written_Code = None,
      Compound_Drug_Indicator = None,
      Date_Prescription_Filled = None,
      Quantity_Dispensed = quantity,
      Days_Supply = None,
      Charge_Amount = billed,
      Paid_Amount = planPaid,
      Ingredient_Cost = None,
      Postage_Amount_Claimed = None,
      Dispensing_Fee = None,
      Copay_Amount = copay,
      Coinsurance_Amount = coin,
      Deductible_Amount = deduct,
      Prescribing_Provider_ID = None,
      Prescribing_Physician_First_Name = None,
      Prescribing_Physician_Middle_Name = None,
      Prescribing_Physician_Last_Name = None,
      Prescribing_Physician_DEA_Number = None,
      Prescribing_Physician_NPI = None,
      Prescribing_Physician_Plan_Number = None,
      Prescribing_Physician_License_Number = None,
      Prescribing_Physician_Street_Address = None,
      Prescribing_Physician_Street_Address_2 = None,
      Prescribing_Physician_City = None,
      Prescribing_Physician_State = None,
      Prescribing_Physician_Zip = None,
      Product_ID_Number = None,
      Mail_Order_pharmacy = None,
      Script_number = "123",
      Recipient_PCP_ID = None,
      Single_or_Multiple_Source_Indicator = None,
      Filler5 = None,
      Billing_Provider_Tax_ID_Number = None,
      Paid_Date = None,
      Date_Prescription_Written = None,
      Coordination_of_Benefits_or_TPL_Liability_Amount = cob,
      Other_Insurance_Paid_Amount = None,
      Medicare_Paid_Amount = None,
      Allowed_amount = allowed,
      Member_Self_Pay_Amount = None,
      Rebate_Indicator = None,
      State_Sales_Tax = None,
      Delegated_Benefit_Administrator_Organization_ID = None,
      Formulary_Code = None,
      Route_of_Administration = None,
      Drug_Unit_of_Measure = None,
      Filler6 = None,
      Filler7 = None,
      Filler8 = None,
      Filler9 = None,
      Filler10 = None,
      Filler11 = None,
      Carrier_Specific_Unique_Member_ID = "MemberId",
      Carrier_Specific_Unique_Subscriber_ID = None,
      Filler12 = None,
      Claim_Line_Type = None,
      Former_Claim_Number = None,
      Medicare_Indicator = None,
      Pregnancy_Indicator = None,
      Diagnosis = None,
      ICD_Indicator = None,
      Denied_Flag = None,
      Denial_Reason = None,
      Payment_Arrangement_Type = None,
      Filler13 = None,
      APCD_ID_Code = None,
      Claim_Line_Paid_Flag = None,
      Record_Type = None
    )

  def makeSilverPharmacy(
    quantity: Option[String] = None,
    allowed: Option[String] = None,
    billed: Option[String] = None,
    cob: Option[String] = None,
    copay: Option[String] = None,
    deduct: Option[String] = None,
    coin: Option[String] = None,
    planPaid: Option[String] = None
  ): SilverPharmacy =
    SilverPharmacy(
      identifier = identifier,
      patient = patient,
      data = makeData(quantity, allowed, billed, cob, copay, deduct, coin, planPaid)
    )

  "Pharmacy Transformer" should "amounts zero" in {
    runWithContext { sc =>
      val silverPharmacy =
        sc.parallelize(
          List(makeSilverPharmacy(planPaid = Some("0"), quantity = Some("14"), coin = Some("0")))
        )
      val goldPharmacy =
        PharmacyTransformer.pipeline(silverPharmacy, "tufts")

      goldPharmacy.map(_.amount.planPaid) should containSingleValue[Option[BigDecimal]](
        Some(BigDecimal(0.0))
      )
      goldPharmacy.map(_.amount.allowed) should containSingleValue[Option[BigDecimal]](
        Some(BigDecimal(0.0))
      )
    }
  }

  "Pharmacy Transformer" should "amounts none" in {
    runWithContext { sc =>
      val silverPharmacy = sc.parallelize(List(makeSilverPharmacy()))
      val goldPharmacy = PharmacyTransformer.pipeline(silverPharmacy, "tufts")

      goldPharmacy.map(_.amount.allowed) should containSingleValue[Option[BigDecimal]](None)
      goldPharmacy.map(_.amount.planPaid) should containSingleValue[Option[BigDecimal]](None)
    }
  }

  "Pharmacy Transformer" should "amounts quantity 28" in {
    runWithContext { sc =>
      val silverPharmacy = sc.parallelize(
        List(
          makeSilverPharmacy(Some("-14")),
          makeSilverPharmacy(Some("14")),
          makeSilverPharmacy(Some("-14")),
          makeSilverPharmacy(Some("-14")),
          makeSilverPharmacy(Some("14")),
          makeSilverPharmacy(Some("14")),
          makeSilverPharmacy(Some("-14")),
          makeSilverPharmacy(Some("14")),
          makeSilverPharmacy(Some("14")),
          makeSilverPharmacy(Some("14"))
        )
      )
      val goldPharmacy =
        PharmacyTransformer.pipeline(silverPharmacy, "tufts")

      goldPharmacy.map(_.drug.quantityDispensed) should containSingleValue[Option[BigDecimal]](
        Some(BigDecimal(28.0))
      )

      goldPharmacy.map(_.amount.allowed) should containSingleValue[Option[BigDecimal]](None)
    }
  }
}
