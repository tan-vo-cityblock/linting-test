package cityblock.utilities.connecticare

import cityblock.models.ConnecticareSilverClaims.{Medical, Pharmacy}
import cityblock.models.connecticare.silver.Member.ParsedMember
import cityblock.utilities.Insurance.SubLineOfBusiness._
import cityblock.utilities.Insurance._
import cityblock.utilities.InsuranceMapping

object Insurance {
  private def lineOfBusiness(sub: Option[SubLineOfBusiness.Value]): Option[LineOfBusiness.Value] =
    sub match {
      case Some(FullyInsured)      => Some(LineOfBusiness.Commercial)
      case Some(Exchange)          => Some(LineOfBusiness.Commercial)
      case Some(SelfFunded)        => Some(LineOfBusiness.Commercial)
      case Some(MedicareAdvantage) => Some(LineOfBusiness.Medicare)
      case _                       => None
    }

  private def subLineOfBusiness(LOB2: Option[String]): Option[SubLineOfBusiness.Value] =
    LOB2 match {
      case Some("FI") => Some(FullyInsured)
      case Some("EX") => Some(Exchange)
      case Some("SF") => Some(SelfFunded)
      case Some("M")  => Some(MedicareAdvantage)
      case _          => None
    }

  private def planDescription(sub: Option[SubLineOfBusiness.Value]): Option[String] =
    sub match {
      case Some(FullyInsured)      => Some("Commercial - Fully Insured Off Exchange")
      case Some(Exchange)          => Some("Commercial - Exchange")
      case Some(SelfFunded)        => Some("Commercial - Self Funded")
      case Some(MedicareAdvantage) => Some("Medicare - Medicare Advantage")
      case _                       => None
    }

  private[Insurance] trait CCIInsuranceMapping[T] extends InsuranceMapping[T] {
    override def getLineOfBusiness(row: T): Option[LineOfBusiness.Value] = {
      val sub = getSubLineOfBusiness(row)
      lineOfBusiness(sub)
    }

    override def getPlanDescription(row: T): Option[String] = {
      val sub = getSubLineOfBusiness(row)
      planDescription(sub)
    }
  }

  object MemberMapping extends CCIInsuranceMapping[ParsedMember] {
    override def getSubLineOfBusiness(row: ParsedMember): Option[SubLineOfBusiness.Value] =
      subLineOfBusiness(row.LOB2)
  }

  object PharmacyMapping extends CCIInsuranceMapping[Pharmacy] {
    override def getSubLineOfBusiness(row: Pharmacy): Option[SubLineOfBusiness.Value] =
      subLineOfBusiness(row.pharmacy.LOB2)
  }

  object MedicalMapping extends CCIInsuranceMapping[Medical] {
    override def getSubLineOfBusiness(row: Medical): Option[SubLineOfBusiness.Value] =
      subLineOfBusiness(row.medical.LOB2)
  }
}
