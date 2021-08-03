package cityblock.models.connecticare.silver

import com.spotify.scio.bigquery.types.BigQueryType

object FacetsMember {
  @BigQueryType.toTable
  case class ParsedFacetsMember(
    Record_Type: Option[String],
    Source_System_ID: Option[String],
    Group_Level_1_ID: Option[String],
    Group_Level_2_ID: Option[String],
    Group_Level_3_ID: Option[String],
    Subscriber_ID: Option[String],
    Member_ID: String,
    Member_Suffix: Option[String],
    Type: Option[String],
    Relationship: Option[String],
    Last_Name: Option[String],
    First_Name: Option[String],
    Middle_Initial: Option[String],
    Name_Suffix: Option[String],
    Original_Effective_Date: Option[String],
    Termination_Date: Option[String],
    Gender: Option[String],
    Birth_Date: Option[String],
    Language: Option[String],
    Past_Member_ID: Option[String],
    Exchange_Member_ID: Option[String],
    Event_Type_Indicator: Option[String],
    COB_Indicator: Option[String],
    Group_Name: Option[String],
    Group_State: Option[String],
    Parent_Group: Option[String],
    Client_ID: Option[String],
    Date_of_death: Option[String],
    Subscriber_Employment_type: Option[String],
    Subgroup_Name: Option[String],
    Union_Welfare_Fund: Option[String],
    Health_Code: Option[String],
    Division_Code: Option[String],
    Opt_Out_Indicator: Option[String],
    Preferred_Contact_Indicator: Option[String],
    Member_Handicap_Effective_Date: Option[String],
    Member_Handicap_Termination_Date: Option[String],
    Member_Handicap_Type: Option[String],
    Handicap_Indicator: Option[String],
    Student_Effective_Date: Option[String],
    Student_Termination_Date: Option[String],
    School_Name: Option[String],
    Student_Type: Option[String],
    Responsible_key_person: Option[String],
    Responsible_Person_ID: Option[String],
    Responsible_Person_Last_Name: Option[String],
    Responsible_Person_First_Name: Option[String],
    Responsible_Person_Middle_Initial: Option[String],
    Responsible_Person_Address_Line_1: Option[String],
    Responsible_Person_Address_Line_2: Option[String],
    Responsible_Person_Address_Line_3: Option[String],
    Responsible_Person_City: Option[String],
    Responsible_Person_State: Option[String],
    Responsible_Person_Zip_Code: Option[String],
    Responsible_Person_County: Option[String],
    Responsible_Person_Country: Option[String],
    Residential_Address_Type: Option[String],
    Residential_Address_Line_1: Option[String],
    Residential_Address_Line_2: Option[String],
    Residential_Address_Line_3: Option[String],
    Residential_City: Option[String],
    Residential_State: Option[String],
    Residential_Zip: Option[String],
    Residential_Country: Option[String],
    Residential_Phone: Option[String],
    Residential_Phone_Extension: Option[String],
    Fax: Option[String],
    Residential_Fax_Extension: Option[String],
    Residential_Email: Option[String],
    Cell_Phone_Number: Option[String],
    Mail_Address_Type: Option[String],
    Mail_Address_Line_1: Option[String],
    Mail_Address_Line_2: Option[String],
    Mail_Address_Line_3: Option[String],
    Mail_City: Option[String],
    Mail_State: Option[String],
    Mail_Zip: Option[String],
    Mail_Country: Option[String],
    Billing_Address_Line_1: Option[String],
    Billing_Address_Line_2: Option[String],
    Billing_Address_Line_3: Option[String],
    Billing_City: Option[String],
    Billing_State: Option[String],
    Billing_Zip: Option[String],
    Billing_Country: Option[String],
    Confidential_indicator: Option[String],
    Confidential_Address_Line_1: Option[String],
    Confidential_Address_Line_2: Option[String],
    Confidential_Address_Line_3: Option[String],
    Confidential_City: Option[String],
    Confidential_County: Option[String],
    Confidential_Country_Code: Option[String],
    Confidential_State: Option[String],
    Confidential_Zip: Option[String],
    Social_Security_Number: Option[String],
    Custom_ID_1: Option[String],
    Custom_Val_1: Option[String],
    Custom_Desc_1: Option[String],
    Custom_ID_2: Option[String],
    Custom_Val_2: Option[String],
    Custom_Desc_2: Option[String],
    Custom_ID_3: Option[String],
    Custom_Val_3: Option[String],
    Custom_Desc_3: Option[String],
    Custom_ID_4: Option[String],
    Custom_Val_4: Option[String],
    Custom_Desc_4: Option[String],
    Custom_ID_5: Option[String],
    Custom_Val_5: Option[String],
    Custom_Desc_5: Option[String],
    Custom_ID_6: Option[String],
    Custom_Val_6: Option[String],
    Custom_Desc_6: Option[String],
    Custom_ID_7: Option[String],
    Custom_Val_7: Option[String],
    Custom_Desc_7: Option[String],
    Custom_ID_8: Option[String],
    Custom_Val_8: Option[String],
    Custom_Desc_8: Option[String],
    Additional_1: Option[String]
  )

}