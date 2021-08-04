package cityblock.member.attribution.runners

import cityblock.member.attribution.transforms.HealthyBlueTransformer
import cityblock.member.service.io.{CreateAndPublishMemberSCollection, PublishMemberSCollection}
import cityblock.member.service.utilities.MultiPartnerIds
import cityblock.member.silver.HealthyBlue.HealthyBlueMember
import cityblock.utilities.Environment
import com.spotify.scio.bigquery._
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}

class HealthyBlueRunner extends GenericRunner {
  def query(
    bigQueryProject: String,
    bigQueryDataset: String
  ): String =
    """SELECT
                |patient,
                |data.CNDS_ID as MemberId,
                |data.CNDS_ID as MedicaidId,
                |data.Enrollment_Start_Date as EnrollmentDate,
                |data.Enrollment_End_Date as TerminationDate,
                |data.Plan_Coverage_Description as LineOfBusiness,
                |data.Program_Category_Code as SubLineOfBusiness,
                |data.Member_Last_Name as LastName,
                |data.Member_First_Name as FirstName,
                |data.Member_Middle_Name as MiddleName,
                |data.Member_DOB as DOB,
                |data.Member_DOD as DOD,
                |data.MBI_HIC as MBI,
                |data.Member_Gender_Code as GenderCode,
                |data.Race_Code_01 as RaceCode1,
                |data.Race_Code_02 as RaceCode2,
                |data.Ethnicity_Code as EthnicityCode,
                |data.Language_Code as PrimaryLanguageCode,
                |data.Member_Address_Line1 as AddressLine1,
                |data.Member_Address_Line2 as AddressLine2,
                |data.Member_City_Name as City,
                |data.Member_State_Code as State,
                |data.Member_ZIP_Code as PostalCode,
                |data.Member_County_Code as County,
                |data.Member_Phone_Number as PrimaryPhone,
                |data.PCP_Last_Name as PCPName,
                |data.PCP_Address_Line1 as PCPAddressLine1,
                |data.PCP_Address_Line2 as PCPAddressLine2,
                |data.PCP_City as PCPCity,
                |data.PCP_State as PCPState,
                |data.PCP_ZIP_Code as PCPPostalCode,
                |data.PCP_Contact_Number as PCPPhone,
                |mapping.clinicId as ClinicId
                |FROM
                |`cbh-healthyblue-data.silver_claims.member_20210608`
                |LEFT JOIN 
                |`cbh-healthyblue-data.resources.city_clinic_mapping` mapping
                |ON mapping.PCP_City = data.PCP_City""".stripMargin

  def deployOnScio(
    bigQuerySrc: String,
    cohortId: Option[Int],
    env: Environment,
    multiPartnerIds: MultiPartnerIds
  ): ScioContext => ScioResult = { sc: ScioContext =>
    val healthyBlueMembers: SCollection[HealthyBlueMember] =
      sc.typedBigQuery[HealthyBlueMember](bigQuerySrc)

    val (existingMembers, newMembers) =
      healthyBlueMembers.partition(_.patient.patientId.isDefined)

    newMembers
      .map(
        healthyBlueMember =>
          HealthyBlueTransformer
            .createAndPublishRequest(healthyBlueMember, multiPartnerIds, "Healthy Blue", cohortId)
      )
      .createAndPublishToMemberService(env)

    existingMembers
      .map(
        healthyBlueMember =>
          (
            healthyBlueMember.patient.patientId.get,
            HealthyBlueTransformer
              .createAndPublishRequest(healthyBlueMember, multiPartnerIds, "Healthy Blue", None)
        )
      )
      .updateAndPublishToMemberService(env)

    sc.close().waitUntilDone()
  }
}
