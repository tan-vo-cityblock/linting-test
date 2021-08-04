package cityblock.importers.generic

import com.spotify.scio.values.SCollection

case class ParseResult(
  validRows: SCollection[Map[String, String]],
  invalidRows: SCollection[FailedParseContainer.FailedParse]
)
