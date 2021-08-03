package cityblock.utilities

object Strings {

  def startsWithCapitalLetter(s: String): Boolean =
    "^[A-Z]".r.findFirstIn(s).nonEmpty

  /**
   * Prepends zeros to `s` until it is of `padTo` length. This function is a no-op if `padTo < 0`
   * or `s.length >= padTo`.
   */
  def zeroPad(s: String, padTo: Int): String =
    // String.times produces an empty string if the Int argument is negative
    s"""${"0" * (padTo - s.length)}$s"""
}
