package cityblock.parsers.emblem.associations

import com.spotify.scio.bigquery.types.BigQueryType
import org.joda.time.LocalDate

object ProcedureAssociation {

  @BigQueryType.toTable
  case class ParsedProcedureAssociation(
    CLAIMNUMBER: String,
    SEQUENCENUMBER: Option[String],
    PROCEDURECODE: Option[String],
    PROCEDUREDATE: Option[LocalDate],
    ICDVERSION10ORHIGHER: Option[String],
    SITEID: Option[String]
  )
}
