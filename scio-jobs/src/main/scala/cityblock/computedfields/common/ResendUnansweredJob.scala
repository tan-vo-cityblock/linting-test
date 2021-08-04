package cityblock.computedfields.jobs

import cityblock.utilities.ResultSigner
import com.spotify.scio.{ContextAndArgs, ScioContext}
import com.spotify.scio.values.{SCollection}
import cityblock.computedfields.common.ComputedFieldResults._
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions
import com.spotify.scio.bigquery._

object ResendUnansweredJob {

  def persist(latestValues: SCollection[NewQueryResult],
              jobName: String,
              projectName: String,
              dataset: String): Unit = {

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

  def buildQuery(projectName: String, dataset: String): String =
    s"""
      select 
        lr.patientId, 
        lr.fieldSlug, 
        lr.value
      from `cityblock-data.computed_fields.latest_results` lr
      left join `${projectName}.${dataset}.builder_question` q
      on lr.fieldSlug = q.computedFieldSlug 
      left join `${projectName}.${dataset}.patient_answer` pa
      on 
        lr.patientId = pa.patientId and
        q.slug = pa.questionSlug and
        pa.deletedAt is null
      where lr.value != pa.answerValue
     """

  def compute(sc: ScioContext,
              projectName: String,
              dataset: String): SCollection[NewQueryResult] = {

    val query = buildQuery(projectName = projectName, dataset = dataset)

    sc.typedBigQuery[NewQueryResult](query)

  }

  def main(cmdlineArgs: Array[String]): Unit = {

    val (sc, args) = ContextAndArgs(cmdlineArgs)
    val jobName = sc.options.getJobName()
    val projectName = sc.optionsAs[GcpOptions].getProject()
    val dataset = args.getOrElse("dataset", "commons_mirror_staging")
    val latestValues = compute(sc, projectName, dataset)

    persist(latestValues, jobName, projectName, dataset)

    val result = sc.close().waitUntilFinish()
  }

}
