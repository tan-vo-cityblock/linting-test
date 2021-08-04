package cityblock.transforms.connecticare.gold

import cityblock.models.ConnecticareSilverClaims.Medical
import cityblock.models.Surrogate
import cityblock.models.gold.Claims.Procedure
import cityblock.models.gold.enums.ProcedureTier
import cityblock.models.gold.{Amount, Constructors, ProviderIdentifier}
import cityblock.transforms.Transform
import cityblock.utilities.Loggable
import cityblock.utilities.Conversions

/**
 * Helper functions for mapping CCI [[Medical]] claims to gold professional and facility
 * claims.
 */
trait MedicalMapping extends Loggable {
  def referringProviderKey(m: Medical): Option[ProviderKey] =
    m.medical.RefProvNum.map(ProviderKey(_))

  def servicingProviderKey(m: Medical): Option[ProviderKey] =
    m.medical.ServProvNum.map(ProviderKey(_))

  /**
   * Constructs a list of (Proc, ProcType) tuples from the multiple procedure codes in a CCI
   * [[Medical]] line.
   * @param m [[Medical]] line
   */
  def procCodesAndTypes(m: Medical): List[(Option[String], Option[String])] =
    List((m.medical.Proc1, m.medical.Proc1Type), (m.medical.Proc2, m.medical.Proc2Type))

  private def mkProcedure(surrogate: Surrogate,
                          m: Medical,
                          proc: Option[String],
                          procType: Option[String]): Option[Procedure] =
    for {
      code <- proc
      codeset <- procType
        .flatMap(ProcType.fromString)
        .flatMap(ProcType.toCodeSet)
    } yield
      Procedure(
        surrogate,
        tier = ProcedureTier.Secondary.name,
        codeset = codeset.toString,
        code = code,
        modifiers = (m.medical.Modifer1 ++ m.medical.Modifier2).toList
      )

  /**
   * Selects the primary procedure for the given `line` and converts it to a
   * [[cityblock.models.gold.Claims.Procedure]].
   *
   * CCI [[Medical]] rows potentially hold procedures in two columns: `Proc1` and `Proc2`. The
   * codeset of each column is given by `Proc1Type` and `Proc2Type` respectively. If either `Type`
   * field is "HCPC" or "CPT", then then code field indicates a procedure.
   *
   * If there are two codes present, we prioritize the code in `Proc1`.
   */
  def chooseFirstProcedure(surrogate: Surrogate, line: Medical): Option[Procedure] =
    procCodesAndTypes(line).flatMap {
      case (proc, procType) => mkProcedure(surrogate, line, proc, procType)
    }.headOption

  /**
   * Creates an [[Amount]] and assigns `allowed`, `billed`, and `planPaid`. CCI does not
   * provide values for other amounts on [[Medical]] claim lines.
   * @param line silver medical line
   * @return initialized [[Amount]]
   */
  def mkAmount(line: Medical): Amount =
    Constructors.mkAmount(
      allowed = line.medical.AmtAllow,
      billed = line.medical.AmtCharge,
      COB = None,
      copay = None,
      deductible = None,
      coinsurance = None,
      planPaid = line.medical.AmtPay
    )

  // Use servicing provider since CCI does not provide billing provider but downstream processes assume its existence.
  def mkBillingProvider(first: Medical): Option[ProviderIdentifier] =
    for (key <- servicingProviderKey(first))
      yield ProviderIdentifier(id = key.uuid.toString, specialty = first.medical.ServProvSpec1)

  def mkReferringProvider(first: Medical): Option[ProviderIdentifier] =
    for (key <- referringProviderKey(first))
      yield ProviderIdentifier(id = key.uuid.toString, specialty = None)

  def mkServicingProvider(line: Medical): Option[ProviderIdentifier] =
    for (key <- servicingProviderKey(line))
      yield ProviderIdentifier(id = key.uuid.toString, specialty = line.medical.ServProvSpec1)

  def inNetworkFlag(m: Medical): Option[Boolean] =
    m.medical.PaymentStatus_NetworkInOut.map {
      case "IN" => true
      case _    => false
    }

  def cobFlag(silver: Medical): Boolean =
    silver.medical.COCLevel2.contains("COB")

  def serviceQuantity(silver: Medical): Option[Int] =
    silver.medical.Qty.flatMap(q => Conversions.safeParse(q, _.toInt))

  private def logBadClmServNum(silver: Medical, reason: String): Unit = {
    val num = silver.medical.ClmServNum
    val id = silver.identifier.surrogateId
    logger.error(s"[surrogate: $id] Claim has invalid ClmServNum ($num) | $reason")
  }

  /**
   * Computes the gold line number for this [[Medical]] claim line.
   *
   * CCI specifies line numbers in hundreds (100, 200, ...), so we chop off the last two digits. However for Duals
   * members they use sequential integers for claim lines.
   * @param silver silver [[Medical]] claim
   * @return gold line number, or [[None]] if `ClmServNum` cannot be parsed as an [[Int]]
   */
  def lineNumber(silver: Medical): Option[Int] =
    Conversions.safeParse(silver.medical.ClmServNum, _.toInt) match {
      case Some(num) if num < 100         => Some(num)
      case Some(num) if (num / 100) < 100 => Some(num / 100)
      case _ =>
        logBadClmServNum(silver, "Not an integer.")
        None
    }

  object CCILineOrdering extends Ordering[Medical] {
    override def compare(x: Medical, y: Medical): Int = {
      val l = Conversions.safeParse(x.medical.ClmServNum, _.toInt)
      val r = Conversions.safeParse(y.medical.ClmServNum, _.toInt)

      Transform.ClaimLineOrdering.compare(l, r)
    }
  }
}
