package cityblock.transforms.tufts.gold

import cityblock.models.gold.ProfessionalClaim.{
  Line,
  PreCombinedLine,
  PreCombinedProfessional,
  Professional
}
import com.spotify.scio.values.SCollection

case class MergeProfessional(
  historicalProfessional: SCollection[PreCombinedProfessional],
  newProfessional: SCollection[PreCombinedProfessional]
) {
  def merge(): SCollection[Professional] =
    MergeProfessional.merge(historicalProfessional, newProfessional)
}

object MergeProfessional {
  def merge(
    historical: SCollection[PreCombinedProfessional],
    newProfessional: SCollection[PreCombinedProfessional]
  ): SCollection[Professional] = {

    val groupedHistoricals = historical.groupBy(_.claimId)
    val groupedNews = newProfessional.groupBy(_.claimId)

    val allProfessionals = groupedHistoricals.fullOuterJoin(groupedNews)

    allProfessionals.map(professional => {
      val claimId: String = professional._1
      val claims = professional._2
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
          cobFlag = line.cobFlag,
          capitatedFlag = line.capitatedFlag,
          claimLineStatus = line.claimLineStatus,
          inNetworkFlag = line.inNetworkFlag,
          serviceQuantity = line.serviceQuantity,
          placeOfService = line.placeOfService,
          date = line.date,
          provider = line.provider,
          procedure = line.procedure,
          amount = line.amount,
          diagnoses = line.diagnoses,
          typesOfService = line.typesOfService
        )
      })

      Professional(
        claimId = claimId,
        memberIdentifier = memberIdentifier,
        header = header,
        lines = mappedLines
      )
    })
  }
}
