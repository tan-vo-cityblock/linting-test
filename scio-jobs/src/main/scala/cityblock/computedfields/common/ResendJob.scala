package cityblock.computedfields.common

import cityblock.utilities.ResultSigner
import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.values.SCollection
import cityblock.computedfields.common.ComputedFieldResults._
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import com.spotify.scio.bigquery._

object ResendJob {

  def persist(latestValues: SCollection[NewQueryResult],
              jobName: String,
              projectName: String): Unit = {

    val jsonMessagesWithAttributes = latestValues.flatMap(row => {
      val result = NewComputedFieldResult(
        row.patientId.getOrElse(""),
        row.fieldSlug.getOrElse(""),
        row.value.getOrElse(""),
        jobName
      )

      val jsonResult = result.asJson.noSpaces
      val hmac = ResultSigner.signResult(jsonResult)
      Seq((jsonResult, Map("hmac" -> hmac, "topic" -> "computedField")))
    })

    // Take the json strings with attributes and save to pubsub
    jsonMessagesWithAttributes.saveAsPubsubWithAttributes[String](
      s"projects/${projectName}/topics/newComputedFieldResults")

  }

  def buildQuery(slug: String): String =
    s"""

      select 
        patientId,
        createdAt as time,
        fieldValue as value,
        fieldSlug
      from `cityblock-analytics.mrt_computed.latest_computed_field_results`
      where fieldSlug = '${slug}'
       
    """

  def compute(sc: ScioContext, projectName: String, slug: String): SCollection[NewQueryResult] = {

    val query = buildQuery(slug = slug)

    sc.typedBigQuery[NewQueryResult](query)

  }

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val jobName = sc.options.getJobName()
    val projectName = sc.optionsAs[GcpOptions].getProject()
    val slug = args("slug")
    val latestValues = compute(sc, projectName, slug)

    persist(latestValues, jobName, projectName)

    val result = sc.close().waitUntilFinish()
  }

}
