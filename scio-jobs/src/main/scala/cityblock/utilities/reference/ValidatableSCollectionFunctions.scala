package cityblock.utilities.reference

import com.spotify.scio.coders.Coder
import com.spotify.scio.values.SCollection

object ValidatableSCollectionFunctions {

  implicit final class ValidatableSCollection[T: Coder](
    @transient private val self: SCollection[T]) {

    /**
     * For each element in this collection, nullify the field given by keyFn if its value is not in
     * `codes`.
     *
     * @param codes a set of unique codes that represent a valid codeset
     * @param keyFn gets the field from element to compare with codes
     * @param nullifyFn sets the field in element to None
     * @tparam C type of code (it is the caller's responsibility to ensure that `keyFn` returns the
     *           appropriate code given [[C]]
     * @return this collection, with invalid codes set to None
     */
    def nullifyInvalidCodes[C <: HasCode: Coder](codes: SCollection[C],
                                                 keyFn: T => Option[String],
                                                 nullifyFn: T => T): SCollection[T] = {
      val keyedCodes = codes.keyBy(_.code).asMapSideInput

      self
        .withSideInputs(keyedCodes)
        .map {
          case (claim, ctx) if keyFn(claim).fold(true)(ctx(keyedCodes).contains) => claim
          case (claim, _)                                                        => nullifyFn(claim)
        }
        .toSCollection
    }
  }
}
