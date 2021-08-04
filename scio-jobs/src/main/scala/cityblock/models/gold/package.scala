package cityblock.models

import cityblock.models.gold.Claims.Procedure
import cityblock.models.gold.PharmacyClaim.{DrugClass, DrugClassCodeset}
import cityblock.models.gold.enums.ProcedureTier
import cityblock.transforms.Transform.CodeSet
import cityblock.utilities.Conversions
import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

package object gold {

  @BigQueryType.toTable
  case class ProviderIdentifier(
    id: String,
    specialty: Option[String]
  )

  @BigQueryType.toTable
  case class Amount(
    allowed: Option[BigDecimal],
    billed: Option[BigDecimal],
    cob: Option[BigDecimal],
    copay: Option[BigDecimal],
    deductible: Option[BigDecimal],
    coinsurance: Option[BigDecimal],
    planPaid: Option[BigDecimal]
  )

  @BigQueryType.toTable
  case class TypeOfService(
    tier: Int,
    code: String
  )

  @BigQueryType.toTable
  case class Date(
    from: Option[LocalDate],
    to: Option[LocalDate]
  )

  @BigQueryType.toTable
  case class Address(
    address1: Option[String],
    address2: Option[String],
    city: Option[String],
    state: Option[String],
    county: Option[String],
    zip: Option[String],
    country: Option[String],
    email: Option[String],
    phone: Option[String]
  )

  /**
   * We have to have a separate object for alternate apply() methods of
   * [[BigQueryType]]-annotated functions because defining multiple apply methods
   * breaks the [[BigQueryType.toTable]] macro.
   */
  object Constructors {

    /**
     * Parse amounts as [[BigDecimal]]s before constructing [[Amount]].
     */
    def mkAmount(allowed: Option[String],
                 billed: Option[String],
                 COB: Option[String],
                 copay: Option[String],
                 deductible: Option[String],
                 coinsurance: Option[String],
                 planPaid: Option[String]): Amount =
      Amount(
        allowed.flatMap(Conversions.safeParse(_, BigDecimal(_))),
        billed.flatMap(Conversions.safeParse(_, BigDecimal(_))),
        COB.flatMap(Conversions.safeParse(_, BigDecimal(_))),
        copay.flatMap(Conversions.safeParse(_, BigDecimal(_))),
        deductible.flatMap(Conversions.safeParse(_, BigDecimal(_))),
        coinsurance.flatMap(Conversions.safeParse(_, BigDecimal(_))),
        planPaid.flatMap(Conversions.safeParse(_, BigDecimal(_)))
      )

    def mkDrugClass(code: String, codeset: DrugClassCodeset.Value): DrugClass =
      DrugClass(code, codeset.toString)

    def mkProcedure(surrogate: Surrogate,
                    tier: ProcedureTier,
                    codeset: CodeSet.Value,
                    code: String,
                    modifiers: List[String]): Procedure =
      Procedure(surrogate = surrogate,
                tier = tier.name,
                codeset = codeset.toString,
                code = code,
                modifiers = modifiers)
  }

  /**
   * Common data-access operations on gold partner objects.
   */
  object Helpers {
    def primaryTypeOfService(ts: List[TypeOfService]): Option[String] =
      ts.reduceOption(Ordering.by((_: TypeOfService).tier).min).map(_.code)

    /**
     * Sums optional amounts if there are any, otherwise returns `None`.
     *
     * Use this function when combining parsed decimal amounts in silver claims to
     * distinguish between a list of empty amounts and a list that sums to 0.
     */
    def sumAmount(amounts: List[Option[BigDecimal]]): Option[BigDecimal] =
      if (Conversions.isAllNones(amounts)) None else Some(amounts.flatten.sum)

    def sumAmounts(amounts: List[Amount]): Amount =
      Amount(
        sumAmount(amounts.map(_.allowed)),
        sumAmount(amounts.map(_.billed)),
        sumAmount(amounts.map(_.cob)),
        sumAmount(amounts.map(_.copay)),
        sumAmount(amounts.map(_.deductible)),
        sumAmount(amounts.map(_.coinsurance)),
        sumAmount(amounts.map(_.planPaid)),
      )

  }
}
