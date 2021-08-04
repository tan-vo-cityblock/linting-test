package cityblock.transforms.connecticare.combine

import cityblock.models.gold.PharmacyClaim.Pharmacy
import cityblock.transforms.Transform
import com.spotify.scio.values.SCollection
import org.joda.time.LocalDate

case class CombinePharmacy(
  oldData: SCollection[Pharmacy],
  newData: SCollection[Pharmacy],
  oldShard: LocalDate,
  newShard: LocalDate,
) {
  def combine(): SCollection[Pharmacy] =
    CombinePharmacy.combine(
      oldData,
      newData,
      oldShard,
      newShard
    )
}

object CombinePharmacy {
  def combine(
    oldData: SCollection[Pharmacy],
    newData: SCollection[Pharmacy],
    oldShard: LocalDate,
    newShard: LocalDate
  ): SCollection[Pharmacy] = {
    def groupAndTagShard(
      claims: SCollection[Pharmacy],
      date: LocalDate
    ): SCollection[(String, (Iterable[Pharmacy], LocalDate))] =
      claims.groupBy(_.identifier.id).map {
        case (claimId, claims) => (claimId, (claims, date))
      }

    val groupedOld = groupAndTagShard(oldData, oldShard)
    val groupedNew = groupAndTagShard(newData, newShard)
    val fullData = groupedOld.fullOuterJoin(groupedNew)

    fullData.map {
      case (_, (oldData, newData)) =>
        val moreRecent = List(oldData, newData).flatten.maxBy(_._2)(Transform.LocalDateOrdering)
        moreRecent._1.head
    }
  }
}
