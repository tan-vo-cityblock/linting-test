package cityblock.utilities

import cityblock.utilities.Insurance.{LineOfBusiness, SubLineOfBusiness}

// TODO docs
trait InsuranceMapping[T] {
  def getLineOfBusiness(row: T): Option[LineOfBusiness.Value]
  def getSubLineOfBusiness(row: T): Option[SubLineOfBusiness.Value]
  def getPlanDescription(row: T): Option[String]
}
