package cityblock.utilities.emblem

import cityblock.parsers.emblem.claims.FacilityClaimCohort.ParsedFacilityClaimCohort
import cityblock.parsers.emblem.claims.PharmacyClaimCohort.ParsedPharmacyClaimCohort
import cityblock.parsers.emblem.claims.ProfessionalClaimCohort.ParsedProfessionalClaimCohort
import cityblock.parsers.emblem.members.MemberMonth.ParsedMemberMonth
import cityblock.utilities.reference.tables

object Insurance {
  val DSNPBasePlanCodes = List("PEVSD1", "PEVSD2", "PEDAP3")

  def lineOfBusiness(baseCode: Option[String],
                     lobMap: Map[String, tables.LineOfBusiness]): Option[String] =
    baseCode
      .flatMap(lobMap.get(_))
      .map { _.lineOfBusiness1 }

  def subLineOfBusiness(baseCode: Option[String],
                        lobMap: Map[String, tables.LineOfBusiness]): Option[String] =
    baseCode
      .flatMap { pkgId =>
        lobMap.get(pkgId)
      }
      .map { _.lineOfBusiness2 }

  sealed trait EmblemInsuranceMapping[T] {
    def getLineOfBusiness(row: T, lobMap: Map[String, tables.LineOfBusiness]): Option[String] =
      Insurance.lineOfBusiness(packageCode(row), lobMap)
    def getSubLineOfBusiness(row: T, lobMap: Map[String, tables.LineOfBusiness]): Option[String] =
      Insurance.subLineOfBusiness(packageCode(row), lobMap)

    def packageCode(row: T): Option[String]
  }

  object MemberMonthMapping extends EmblemInsuranceMapping[ParsedMemberMonth] {
    override def packageCode(row: ParsedMemberMonth): Option[String] =
      row.BENEFITPLANID

    def getPlanDescription(row: ParsedMemberMonth): Option[String] =
      row.PRODUCTDESCRIPTION
  }

  object ProfessionalCohortMapping extends EmblemInsuranceMapping[ParsedProfessionalClaimCohort] {
    override def packageCode(row: ParsedProfessionalClaimCohort): Option[String] = row.BEN_PKG_ID
  }

  object FacilityCohortMapping extends EmblemInsuranceMapping[ParsedFacilityClaimCohort] {
    override def packageCode(row: ParsedFacilityClaimCohort): Option[String] = row.BEN_PKG_ID
  }

  /**
   * Business logic Feb 2020: concatenate BEN_PKG_ID to first 6 characters for pharmacy table only.
   */
  object PharmacyCohortMapping extends EmblemInsuranceMapping[ParsedPharmacyClaimCohort] {
    override def packageCode(row: ParsedPharmacyClaimCohort): Option[String] =
      row.BEN_PKG_ID.map(_.substring(0, 6))
  }
}
