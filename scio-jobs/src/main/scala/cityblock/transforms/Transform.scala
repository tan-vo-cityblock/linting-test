package cityblock.transforms

import java.util.UUID

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.models.Surrogate
import cityblock.models.gold.Claims.{Diagnosis, MemberIdentifier, Procedure}
import cityblock.models.gold.FacilityClaim.HeaderProcedure
import cityblock.models.gold.ProfessionalClaim
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.utilities.Strings
import com.spotify.scio.ScioContext
import com.spotify.scio.bigquery._
import com.spotify.scio.bigquery.types.BigQueryType.HasAnnotation
import com.spotify.scio.coders.Coder
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.{CreateDisposition, WriteDisposition}
import org.joda.time.LocalDate

import scala.concurrent.Future
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

/**
 * Contains core functions, traits, and classes for implementing integrations with external
 * data sources.
 */
object Transform {

  /** Constant value used to replace sensitive information (e.g., provider TINs). */
  final val ELIDED: String = "ELIDED"

  /** Datestamp format required by bigquery for automatic table sharding. */
  final val ShardNamePattern: String = "YYYYMMdd"

  final val ICD_VERSION_10: Int = 10
  final val ICD_VERSION_9: Int = 9

  object CodeSet extends Enumeration {
    type CodeSet = Value
    val ICD9Pcs, ICD9Cm, ICD10Pcs, ICD10Cm, CPT, HCPCS = Value
  }

  object DRGCodeSet extends Enumeration {
    type DRGCodeSet = Value
    val msDrg, aprDrg = Value
  }

  /** Generates a random [[java.util.UUID]] and return it as a [[String]]. */
  def generateUUID(): String = UUID.randomUUID().toString

  /**
   * Appends a date shard timestamp to the BigQuery table name
   *
   * @param table name of the table as it should appear in BigQuery
   * @param date date of ingestion
   * @return the correct BigQuery table name for the given date shard
   */
  def shardName(table: String, date: LocalDate): String =
    s"${table}_${date.toString(ShardNamePattern)}"

  /**
   * Persists a given [[SCollection]] to BigQuery, using [[com.spotify.scio]]'s type-safe BigQuery
   * abstraction. Requires an ingestion date so the collection can be written to the correct shard.
   *
   * Note: `shard` is available as a `val shard: LocalDate` within the load job
   * for the given partner.
   *
   * @return a future that resolves once the collection is persisted
   */
  def persist[T <: HasAnnotation: TypeTag: ClassTag: Coder](
    rows: SCollection[T],
    project: String,
    dataset: String,
    table: String,
    shard: LocalDate,
    wd: WriteDisposition,
    cd: CreateDisposition): Future[Tap[T]] = {
    val tableSpec = s"$project:$dataset.${shardName(table, shard)}"
    rows
      .withName(s"Saving to $tableSpec")
      .saveAsTypedBigQuery(tableSpec, wd, cd)
  }

  /**
   * Fetches all rows of the given table from BigQuery.
   *
   * @param sc execution context for the query
   * @param project project name
   * @param dataset dataset name
   * @param table table name
   * @param shard ingestion date
   * @tparam T result type
   * @return a collection containing all rows of the table
   *
   * {{{
   *   val shard = LocalDate.parse("2019-04-10")
   *   val members = Transform.fetchFromBigQuery(sc,
   *                                             "emblem-data",
   *                                             "silver_claims",
   *                                             "professional",
   *                                             shard)
   * }}}
   */
  def fetchFromBigQuery[T <: HasAnnotation: TypeTag: ClassTag: Coder](
    sc: ScioContext,
    project: String,
    dataset: String,
    table: String,
    shard: LocalDate): SCollection[T] = {
    val stampedTable = shardName(table, shard)
    sc.typedBigQuery[T](s"SELECT * FROM `$project.$dataset.$stampedTable`")
  }

  def fetchView[T <: HasAnnotation: TypeTag: ClassTag: Coder](
    sc: ScioContext,
    project: String,
    dataset: String,
    table: String
  ): SCollection[T] =
    sc.typedBigQuery[T](s"SELECT * FROM `$project.$dataset.$table`")

  /**
   * Extend this trait with a case class that contains [[com.spotify.scio.values.SCollection]]s.
   * [[transform]] maps input to a destination type [[T]] in the destination model.
   *
   * A case class extending [[Transformer]] should contain [[SCollection]]s of the silver tables
   * and gold indexes necessary to construct the gold table [[T]]. In typical usage, [[transform]]
   * contains the scio pipeline for transformations, but all of the business logic for mapping
   * fields is delegated to smaller functions in the companion object. Each subobject `Bar` in
   * [[T]] gets its own `mkBar` constructor. When mapping a field `baz` with non-trivial business
   * logic, create a separate method `baz` to encapsulate the conversion.
   *
   * Here's a mock implementation for a fictional gold table `Foo`.
   *
   * {{{
   *   val source: String = "applecare" // generally visible from package object
   *
   *   case class Foo(identifier: Identifier, bar: Bar)
   *   case class Bar(baz: Option[String], biz: Option[BigDecimal])
   *
   *   case class FooTransformer(silver: SCollection[SilverFoo]) extends Transformer[Foo] {
   *       def transform()(implicit pr: String): SCollection[Foo] =
   *           silver
   *               .map { s =>
   *                   Transform.addSurrogate(pr, "silver_claims", "SilverFoo", s
   *                   )(getSurrogateId(_))
   *               .map {
   *                   case (surrogate, silver) => mkFoo(surrogate, silver)
   *               }
   *   }
   *
   *   object FooTransformer {
   *       // constructors go first, in top down order
   *       def mkFoo(surrogate: Surrogate, silver: SilverFoo): Foo = Foo(
   *           identifier = Identifier(
   *             id = Transform.generateUUID(),
   *             source = source,
   *             surrogate = surrogate
   *           ),
   *           bar = mkBar(silver)
   *       )
   *       def mkBar(silver: SilverFoo): Bar = Bar(
   *          bar = bar(silver),
   *          baz = baz(silver)
   *       )
   *       // field mappings go second, in order of their appearance in constructors
   *       def baz(silver: SilverFoo): Option[String] = ...
   *       def biz(silver: SilverFoo): Option[BigDecimal] = ...
   *   }
   * }}}
   *
   * @param typeTag$T required by [[com.spotify.scio]] for type-safe BigQuery
   * @param classTag$T required by [[com.spotify.scio]] for type-safe BigQuery
   * @param coder$T required by [[com.spotify.scio]] for type-safe BigQuery
   * @tparam T destination type
   * @see For a real example, see [[cityblock.transforms.connecticare.gold.PharmacyTransformer]]
   */
  abstract class Transformer[T <: HasAnnotation: TypeTag: ClassTag: Coder] {

    /**
     * Transforms this [[Transformer]]'s source collections into a collection of the destination
     * type [[T]].
     *
     * The implicit String parameter is tech-debt. It should be moved to [[Model]].
     *
     * @param pr source project for the collections given as parameters to this [[Transformer]]
     * @return a collection of [[T]]s
     */
    def transform()(implicit pr: String): SCollection[T]
  }

  /**
   * Adds a [[cityblock.models.Surrogate]] to this row of a collection.
   *
   * Use this in a [[Transformer.transform()]] whose destination type includes a
   * [[cityblock.models.Identifier]]. Thi function hard-codes "silver_claims" as the dataset
   * because surrogate ids are only assigned in silver datasets.
   *
   * {{{
   *   val providers: SCollection[SilverProvider] = ...
   *   val withSurrogates = providers.map(addSurrogate("emblem-data", "silver_claims", "providers", _)
   *                                                  (_.identifier.surrogateId))
   * }}}
   *
   * @param project the source project of this row
   * @param dataset the dataset of this row
   * @param table the source BigQuery table of this row
   * @param row the row
   * @param getSurrogateId function to extract a surrogate id from the given row
   * @tparam T the row's type
   * @return a tuple containing the row and its surrogate
   */
  def addSurrogate[T](project: String, dataset: String, table: String, row: T)(
    implicit getSurrogateId: T => String): (Surrogate, T) =
    (Surrogate(id = getSurrogateId(row), project = project, dataset = dataset, table = table), row)

  /**
   * Empty trait extended by case classes that act as the left side in
   * [[com.spotify.scio.values.PairSCollectionFunctions]].
   *
   * This trait is used to ensure typesafe joins with collections that have complex keys or may be
   * indexed by multiple keys (e.g., a [[cityblock.models.EmblemSilverClaims.SilverMemberMonth]]
   * may be indexed by its Emblem member ID or its PCP id).
   *
   * Typically, a case class extends [[Key]] and provides in its companion object methods to
   * construct that key any collection elements that will be indexed with that [[Key]]. For
   * example,
   *
   * {{{
   *   case class MyKey(a: String, b: String) extends Key
   *
   *   object MyKey {
   *      def apply(f: Foo): MyKey = ...
   *      def apply(b: Bar): MyKey = ...
   *   }
   * }}}
   *
   * To join [[SCollection]]s of Foo and Bar,
   *
   * {{{
   *   val fs: SCollection[Foo] = ...
   *   val bs: SCollection[Bar] = ...
   *
   *   fs.keyBy(MyKey(_)).join(bs.keyBy(MyKey(_)))
   * }}}
   */
  trait Key

  /**
   * Extending this trait makes an class comparable by [[UUID]]. The extending class declares a set
   * of [[String]] fields that comprise a composite key. The class's implementer is responsible for
   * ensuring that the composite key accurate
   *
   * For example,
   * {{{
   *   case class Key(field1: String, field2: String, field3: Option[String])
   *     extends UUIDComparable {
   *       override protected val elements: Set[String] = Set(field1, field2) ++ field3
   *   }
   * }}}
   */
  // scalastyle:off equals.hash.code
  trait UUIDComparable {
    protected val elements: List[String] = List()
    // Add delimeter to avoid the situation where the concatenation of different strings are unintentionally equal.
    private val nullDelimiter: String = "\u0000"
    lazy val uuid: UUID = {
      require(elements.nonEmpty)
      UUID.nameUUIDFromBytes(elements.mkString(nullDelimiter).getBytes())
    }

    override def equals(obj: Any): Boolean = obj match {
      case that: UUIDComparable => this.uuid.equals(that.uuid)
      case _                    => false
    }
  }
  // scalastyle:on equals.hash.code

  /**
   * Finds matches rows for `keyFn` in the right side of an inner join and then indexes those
   * right-side values by a new key of type [[I]] for use in a later
   * [[com.spotify.scio.util.MultiJoin]].
   *
   * @param rows left side of join
   * @param right right side of join
   * @param keyFn creates a key of type [[K]] from the left side of the join
   * @param indexFn create a key of type [[I]] from the left side of the join that can be used in
   *                a later [[com.spotify.scio.util.MultiJoin]]
   * @tparam L type of left side of join
   * @tparam R type of right side of join
   * @tparam K type of join key
   * @tparam I type of index key
   * @return
   */
  def joinAndIndex[L: Coder, R: Coder, K: Coder, I: Coder](rows: SCollection[L],
                                                           right: SCollection[(K, R)],
                                                           keyFn: L => K,
                                                           indexFn: L => I): SCollection[(I, R)] =
    rows.keyBy(keyFn).join(right).map {
      case (_, (l, r)) => (indexFn(l), r)
    }

  /**
   * Functions exactly the same as [[joinAndIndex()]] except that it performs a left outer join
   * instead of an inner join.
   */
  def leftJoinAndIndex[L: Coder, R: Coder, K: Coder, I: Coder](
    rows: SCollection[L],
    right: SCollection[(K, R)],
    keyFn: L => K,
    indexFn: L => I): SCollection[(I, Option[R])] =
    rows.keyBy(keyFn).leftOuterJoin(right).map {
      case (_, (l, r)) => (indexFn(l), r)
    }

  /**
   * Maps the keys of an [[SCollection]] from [[K1]] to [[K2]].
   *
   * For some reason this doesn't exist in [[com.spotify.scio.values.PairSCollectionFunctions]].
   *
   * @param xs the collection
   * @param f function to map keys
   * @tparam K1 source key type
   * @tparam K2 destination key type
   * @tparam V value type
   * @return the collection with transformed keys
   */
  def mapKeys[K1: Coder, K2: Coder, V: Coder](xs: SCollection[(K1, V)])(
    f: K1 => K2): SCollection[(K2, V)] = xs.map {
    case (k, v) => (f(k), v)
  }

  /**
   * Compares `Option[Int]`s but treats [[None]] as the max value (instead of as the max).
   */
  object ClaimLineOrdering extends Ordering[Option[Int]] {
    private val o = implicitly[Ordering[Int]]

    /**
     * Compare `x` and `y` as [[Int]]s, but treat [[None]] as the max value.
     */
    override def compare(x: Option[Int], y: Option[Int]): Int =
      o.compare(x.getOrElse(Int.MaxValue), y.getOrElse(Int.MaxValue))
  }

  /**
   * Ordering for Option[LocalDate] that treats [[None]] as the minimum date.
   */
  val NoneMinOptionLocalDateOrdering: Ordering[Option[LocalDate]] =
    Ordering.fromLessThan[Option[LocalDate]] {
      case (Some(l), Some(r)) => l.compareTo(r) == -1
      case (Some(_), None)    => false
      case (None, Some(_))    => true
      case _                  => true
    }

  /**
   * Ordering for Option[LocalDate] that treats [[None]] as the maximum date.
   */
  val NoneMaxOptionLocalDateOrdering: Ordering[Option[LocalDate]] =
    Ordering.fromLessThan[Option[LocalDate]] {
      case (Some(l), Some(r)) => l.compareTo(r) == -1
      case (Some(_), None)    => true
      case (None, Some(_))    => false
      case _                  => true
    }

  val LocalDateOrdering: Ordering[LocalDate] = Ordering.fromLessThan(_ isBefore _)
  val ReverseLocalDateOrdering: Ordering[LocalDate] = Ordering.fromLessThan(_ isAfter _)

  val DiagnosisTierOrdering: Ordering[DiagnosisTier] = Ordering.by {
    case DiagnosisTier.Principal => 1
    case DiagnosisTier.Admit     => 2
    case DiagnosisTier.Secondary => 3
  }

  val ProcedureTierOrdering: Ordering[ProcedureTier] = Ordering.by {
    case ProcedureTier.Principal => 1
    case ProcedureTier.Secondary => 2
  }

  private def selectDistinct[A, B, K](xs: Iterable[A], keyFn: A => K, orderByFn: A => B)(
    implicit ordering: Ordering[B]): Iterable[A] =
    xs.groupBy(keyFn).values.map(_.reduce(Ordering.by(orderByFn).min))

  /**
   * Combines line and header diagnoses and groups by (code, codeset), resolving dupes with
   * [[DiagnosisTierOrdering]].
   * @param diagnoses list of diagnosis
   * @param lines list of [[ProfessionalClaim.Line]]s
   * @return unique diagnoses for the [[ProfessionalClaim.Header]]
   */
  def uniqueDiagnoses(diagnoses: Iterable[Diagnosis],
                      lines: Iterable[ProfessionalClaim.Line] = Iterable()): Iterable[Diagnosis] = {
    val dxs = diagnoses ++ lines.flatMap(_.diagnoses)
    selectDistinct(dxs,
                   (dx: Diagnosis) => (dx.code, dx.codeset),
                   (dx: Diagnosis) => DiagnosisTier.fromString(dx.tier))(DiagnosisTierOrdering)
  }

  /**
   * Groups header procedures by (code, codeset, modifiers) and resolves dupes with
   * [[ProcedureTierOrdering]].
   * @param procedures list of procedures
   * @return unique header procedures for the [[cityblock.models.gold.FacilityClaim.Header]]
   */
  def uniqueProcedures(procedures: Iterable[Procedure]): Iterable[HeaderProcedure] = {
    val headerProcedures: Iterable[HeaderProcedure] = {
      procedures.map(procedure => {
        HeaderProcedure(
          surrogate = procedure.surrogate,
          tier = procedure.tier,
          codeset = procedure.codeset,
          code = procedure.code
        )
      })
    }

    selectDistinct(
      headerProcedures,
      (px: HeaderProcedure) => (px.code, px.codeset),
      (px: HeaderProcedure) => ProcedureTier.fromString(px.tier)
    )(ProcedureTierOrdering)
  }

  def cleanDiagnosisCode(code: String): String =
    code.replace(".", "")

  def determineProcedureCodeset(code: String): String =
    if (Character.isLetter(code.charAt(0))) {
      CodeSet.HCPCS.toString
    } else {
      CodeSet.CPT.toString
    }

  def determineDiagnosisCodeset(code: String): String =
    if (Character.isLetter(code.charAt(0))) {
      CodeSet.ICD10Cm.toString
    } else {
      CodeSet.ICD9Cm.toString
    }

  def mkIdentifier(idField: String): String =
    UUID.nameUUIDFromBytes(idField.getBytes()).toString

  def addLeadingZero(inputField: Option[String]): Option[String] =
    inputField.map("0" + _)

  /**
   * Constructs a gold [[Procedure]] from a silver medical claim line of type [[T]] as long
   * as the claim specifies a procedure.
   * @param surrogate reference to silver claim line in BigQuery
   * @param line silver claim line
   * @param codeFn gets the claim line's procedure code
   * @param modifiersFn gets the claim line's procedure code modifiers
   * @tparam T type of claim line
   * @return gold [[Procedure]] for this claim line
   */
  def mkProcedure[T](
    surrogate: Surrogate,
    line: T,
    tier: ProcedureTier,
    codeFn: T => Option[String],
    modifiersFn: T => List[String]
  ): Option[Procedure] =
    for (code <- codeFn(line))
      yield {
        val codeset = if (Strings.startsWithCapitalLetter(code)) {
          CodeSet.HCPCS.toString
        } else {
          CodeSet.CPT.toString
        }

        Procedure(
          surrogate = surrogate,
          tier = tier.name,
          codeset = codeset,
          code = code,
          modifiers = modifiersFn(line)
        )
      }

  def mkMemberIdentifierForPatient(patient: Patient, partner: String): MemberIdentifier =
    MemberIdentifier(
      commonId = patient.source.commonId,
      partnerMemberId = patient.externalId,
      patientId = patient.patientId,
      partner = partner
    )
}
