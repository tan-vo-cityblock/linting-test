package cityblock.computedfields.common

import com.spotify.scio.ScioContext
import com.spotify.scio.values.SCollection
import ComputedFieldResults.NewQueryResult
import com.spotify.scio.bigquery._

trait SelectDbtResults {

  def buildQuery(projectName: String): String =
    s"""

     select 
      lcfr.patientId,
      lcfr.createdAt as time,
      lcfr.fieldValue as value,
      lcfr.fieldSlug
     from `cityblock-analytics.mrt_computed.latest_computed_field_results` lcfr
     left join `${projectName}.computed_fields.latest_results` lr
     using (patientId, fieldSlug)
     inner join `cbh-db-mirror-prod.commons_mirror.patient` p
     on lcfr.patientId = p.id
     where
      (lcfr.fieldValue != lr.value or
      lr.value is null) and
      lcfr.isProductionField is true

     """

  def compute(sc: ScioContext, projectName: String): SCollection[NewQueryResult] = {

    val query =
      this.buildQuery(projectName)
    sc.typedBigQuery[NewQueryResult](query)
  }

}
