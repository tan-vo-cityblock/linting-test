package cityblock.computedfields.common

import cityblock.utilities.ResultSigner
import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.values.SCollection
import com.spotify.scio.bigquery.{CREATE_IF_NEEDED, WRITE_APPEND}
import ComputedFieldResults.{NewComputedFieldResult, NewQueryResult}
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import com.spotify.scio.bigquery._
import com.spotify.scio.coders.Coder

trait ComputedFieldJob {

  def compute(sc: ScioContext, projectName: String): SCollection[NewQueryResult]

  def persist[T](updatedValues: SCollection[NewQueryResult], jobName: String, projectName: String)(
    implicit c: Coder[NewQueryResult]): Unit = {

    // Export new calculations to BigQuery for future calculator runs
    updatedValues
      .saveAsTypedBigQuery("computed_fields.results", WRITE_APPEND, CREATE_IF_NEEDED)

    // // Notify Commons of new values
    val jsonMessagesWithAttributes = updatedValues.flatMap(row => {
      val result = NewComputedFieldResult(
        row.patientId.getOrElse(""),
        row.fieldSlug.getOrElse(""),
        row.value.getOrElse(""),
        jobName
      )

      val jsonResult = result.asJson.noSpaces
      val hmac = ResultSigner.signResult(jsonResult)
      val map: Map[String, String] =
        Map("hmac" -> hmac, "topic" -> "computedField")

      Seq((jsonResult, map))
    })

    // Take the json strings with attributes and save to pubsub
    jsonMessagesWithAttributes.saveAsPubsubWithAttributes[String](
      s"projects/${projectName}/topics/newComputedFieldResults")

  }

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val jobName = sc.options.getJobName()

    val projectName = sc.optionsAs[GcpOptions].getProject()
    val updatedValues = compute(sc, projectName)

    persist(jobName = jobName, updatedValues = updatedValues, projectName = projectName)

    val result = sc.close().waitUntilFinish()
  }

}
