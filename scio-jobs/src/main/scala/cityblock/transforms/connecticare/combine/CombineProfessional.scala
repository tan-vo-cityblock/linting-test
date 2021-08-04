package cityblock.transforms.connecticare.combine

import cityblock.models.gold.ProfessionalClaim.{Line, Professional}
import cityblock.transforms.Transform
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate

case class CombineProfessional(
  oldData: SCollection[Professional],
  newData: SCollection[Professional],
  oldShard: LocalDate,
  newShard: LocalDate,
) {
  def combine(): SCollection[Professional] =
    CombineProfessional.combine(oldData, newData, oldShard, newShard)
}

object CombineProfessional {
  def combine(
    oldData: SCollection[Professional],
    newData: SCollection[Professional],
    oldShard: LocalDate,
    newShard: LocalDate
  ): SCollection[Professional] = {
    val groupedHistorical = oldData.groupBy(_.claimId)
    val groupedNew = newData.groupBy(_.claimId)
    val fullData = groupedHistorical.fullOuterJoin(groupedNew)

    fullData.map {
      case (claimId, (oldClaims, newClaims)) =>
        val (header, memberIdentifier) = if (newClaims.isDefined) {
          (newClaims.get.head.header, newClaims.get.head.memberIdentifier)
        } else {
          (oldClaims.get.head.header, oldClaims.get.head.memberIdentifier)
        }

        def flattenLines(claims: Option[Iterable[Professional]]): Iterable[Line] =
          claims.toList.flatMap(_.flatMap(_.lines))

        val historicalLines: Iterable[Line] = flattenLines(oldClaims)
        val newLines: Iterable[Line] = flattenLines(newClaims)
        val allLines: Map[Int, Iterable[(Line, LocalDate)]] =
          (historicalLines.map(line => (line, oldShard)) ++ newLines.map(line => (line, newShard)))
            .groupBy(_._1.lineNumber)

        val linesToUse: List[Line] = allLines
          .map {
            case (_, linesAndDates) => linesAndDates.maxBy(_._2)(Transform.LocalDateOrdering)
          }
          .map { case (lines, _) => lines }
          .toList

        Professional(
          claimId = claimId,
          memberIdentifier = memberIdentifier,
          header = header,
          lines = linesToUse
        )
    }
  }
}
