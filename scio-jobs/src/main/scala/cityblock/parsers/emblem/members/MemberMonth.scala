package cityblock.parsers.emblem.members

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object MemberMonth {
  @BigQueryType.toTable
  case class ParsedMemberMonth(
    MEMBERID: String,
    MEMBERGENDER: Option[String],
    RELATIONSHIP: Option[String],
    MEMBERAGE: Option[String],
    MEMBERBIRTHDATE: Option[LocalDate],
    MEMBERSTATE: Option[String],
    MEMBERZIPCODE: Option[String],
    MEMBERCOUNTY: Option[String],
    SPANFROMDATE: Option[LocalDate],
    SPANTODATE: Option[LocalDate],
    ELIGIBILITYSTARTDATE: Option[LocalDate],
    ELIGIBILITYENDDATE: Option[LocalDate],
    EMPLOYERGROUPID: Option[String],
    EMPLOYERGROUPMODIFIER: Option[String],
    BENEFITPLANID: Option[String],
    BENEFITSEQUENCENUMBER: Option[String],
    LINEOFBUSINESS: Option[String],
    RIDEROPTION: Option[String],
    BENEFITTIER: Option[String],
    COMPANYCODE: Option[String],
    COMPANYNAME: Option[String],
    BASEPLANCODE: Option[String],
    BASEPLANDESCRIPTION: Option[String],
    PRODUCTCODE: Option[String],
    PRODUCTDESCRIPTION: Option[String],
    PRODUCTTYPECODE: Option[String],
    PRODUCTTYPECODEDEFINITION: Option[String],
    SEGMENTCODE: Option[String],
    SEGMENTDEFINITION: Option[String],
    PROVIDERID: Option[String],
    PROVIDERLOCATIONSUFFIX: Option[String],
    PRIMARYSPECIALTYCODE: Option[String],
    SERVICINGSTATEZIPCODE: Option[String],
    MEDICALCENTERNUMBER: Option[String],
    MEDICALCENTERNAME: Option[String],
    RISKENTITYNAME: Option[String],
    HOSPITALRISKCOHORT: Option[String],
    NETWORKSUBDELIVERYSYSTEMNAME: Option[String],
    SUBDELIVERYSYSTEMCODE: Option[String],
    SUBDELIVERYSYSTEMDESC: Option[String],
    DELIVERYSYSTEMCODE: Option[String],
    DELIVERYSYSTEMDESC: Option[String],
    ACOINDICATOR: Option[String],
    MEMBERCOUNTYROLLUP: Option[String],
    MEDICAIDREGION: Option[String],
    MEDICAIDPREMIUMGROUP: Option[String]
  )
}
