package cityblock.models.gold

import Claims.{Diagnosis, MemberIdentifier}
import cityblock.models.Identifier
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

/**
 * Gold model for pharmacy claims. The case classes are wrapped by an object because the
 * [[BigQueryType.toTable]] macro requires annotated classes to have a parent object.
 *
 * Includes enums specific to pharmacy claims.
 */
object PharmacyClaim {

  @BigQueryType.toTable
  case class Pharmacy(
    identifier: Identifier,
    memberIdentifier: MemberIdentifier,
    capitatedFlag: Option[Boolean],
    claimLineStatus: String,
    cobFlag: Option[Boolean],
    lineOfBusiness: Option[String],
    subLineOfBusiness: Option[String],
    pharmacy: Option[PharmacyProvider],
    prescriber: Option[PrescribingProvider],
    diagnosis: Option[Diagnosis],
    drug: Drug,
    amount: Amount,
    date: Date
  )

  @BigQueryType.toTable
  case class PharmacyProvider(
    id: Option[String],
    npi: Option[String],
    ncpdp: Option[String],
    inNetworkFlag: Option[Boolean]
  )

  @BigQueryType.toTable
  case class PrescribingProvider(
    id: String,
    npi: Option[String],
    specialty: Option[String],
    placeOfService: Option[String]
  )

  @BigQueryType.toTable
  case class Drug(
    ndc: Option[String],
    quantityDispensed: Option[BigDecimal],
    daysSupply: Option[Int],
    partnerPrescriptionNumber: Option[String],
    fillNumber: Option[Int],
    brandIndicator: String,
    ingredient: Option[String],
    strength: Option[String],
    dispenseAsWritten: Option[String],
    dispenseMethod: String,
    classes: List[DrugClass],
    formularyFlag: Boolean
  )

  @BigQueryType.toTable
  case class DrugClass(
    code: String,
    codeset: String
  )

  @BigQueryType.toTable
  case class Date(
    filled: Option[LocalDate],
    paid: Option[LocalDate],
    billed: Option[LocalDate]
  )

  object BrandIndicator extends Enumeration {
    val Brand, Generic, Unknown = Value
  }

  object DispenseMethod extends Enumeration {
    val Retail, Mail, Unknown, NotApplicable, Other = Value
  }

  object DrugClassCodeset extends Enumeration {
    val ATC, GC3, GCN, GPI, RXNORM = Value
  }
}
