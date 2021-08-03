package cityblock.transforms.connecticare.combine

import cityblock.models.gold.FacilityClaim.{Facility, Line}
import cityblock.transforms.Transform
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate

case class CombineFacility(
  oldData: SCollection[Facility],
  newData: SCollection[Facility],
  oldShard: LocalDate,
  newShard: LocalDate
) {
  def combine(): SCollection[Facility] =
    CombineFacility.combine(oldData, newData, oldShard, newShard)
}

object CombineFacility {
  def combine(
    oldData: SCollection[Facility],
    newData: SCollection[Facility],
    oldShard: LocalDate,
    newShard: LocalDate
  ): SCollection[Facility] = {
    val groupedHistorical: SCollection[(String, Iterable[Facility])] = oldData.groupBy(_.claimId)
    val groupedNew = newData.groupBy(_.claimId)
    val fullData = groupedHistorical.fullOuterJoin(groupedNew)

    fullData.map {
      case (claimId, (oldClaims, newClaims)) =>
        val (header, memberIdentifier) = if (newClaims.isDefined) {
          (newClaims.get.head.header, newClaims.get.head.memberIdentifier)
        } else {
          (oldClaims.get.head.header, oldClaims.get.head.memberIdentifier)
        }

        def flattenLines(claims: Option[Iterable[Facility]]): Iterable[Line] =
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

        Facility(
          claimId = claimId,
          memberIdentifier = memberIdentifier,
          header = header,
          lines = linesToUse
        )
    }
  }
}
