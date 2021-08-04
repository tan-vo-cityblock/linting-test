package cityblock.transforms.tufts.gold

import cityblock.models.gold.FacilityClaim.{Facility, Line, PreCombinedFacility, PreCombinedLine}
import com.spotify.scio.values.SCollection

case class MergeFacility(
  historicalFacility: SCollection[PreCombinedFacility],
  newFacility: SCollection[PreCombinedFacility]
) {
  def merge(): SCollection[Facility] =
    MergeFacility.merge(historicalFacility, newFacility)
}

object MergeFacility {
  def merge(
    historical: SCollection[PreCombinedFacility],
    newFacility: SCollection[PreCombinedFacility],
  ): SCollection[Facility] = {
    val groupedHistoricals = historical.groupBy(_.claimId)
    val groupedNews = newFacility.groupBy(_.claimId)

    val allFacilities = groupedHistoricals.fullOuterJoin(groupedNews)

    allFacilities.map(facility => {
      val claimId: String = facility._1
      val claims = facility._2
      val header = if (claims._2.isDefined) claims._2.get.head.header else claims._1.get.head.header
      val memberIdentifier = if (claims._2.isDefined) {
        claims._2.get.head.memberIdentifier
      } else {
        claims._1.get.head.memberIdentifier
      }

      val historicalLines: Option[Iterable[Iterable[PreCombinedLine]]] =
        claims._1.map(_.map(_.lines))
      val newLines: Option[Iterable[Iterable[PreCombinedLine]]] =
        claims._2.map(_.map(_.lines))
      val allLines =
        (historicalLines.toList.flatten.flatten ++ newLines.toList.flatten.flatten)
          .groupBy(_.lineNumber)
      val lines =
        allLines.values.toList.map(_.maxBy(_.Version_Number.map(_.toInt)))
      val mappedLines = lines.map(line => {
        Line(
          surrogate = line.surrogate,
          lineNumber = line.lineNumber,
          revenueCode = line.revenueCode,
          cobFlag = line.cobFlag,
          capitatedFlag = line.capitatedFlag,
          claimLineStatus = line.claimLineStatus,
          inNetworkFlag = line.inNetworkFlag,
          serviceQuantity = line.serviceQuantity,
          typesOfService = line.typesOfService,
          procedure = line.procedure,
          amount = line.amount
        )
      })

      Facility(
        claimId = claimId,
        memberIdentifier = memberIdentifier,
        header = header,
        lines = mappedLines
      )
    })
  }
}
