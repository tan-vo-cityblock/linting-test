package cityblock.aggregators.jobs

import cityblock.aggregators.models.PatientEligibilities.PatientEligibilities
import com.spotify.scio.testing.PipelineSpec
import cityblock.utilities.{Environment, PatientEligibilitiesMocks, Should}
import cityblock.transforms.Transform
import com.spotify.scio.bigquery.BigQueryIO
import com.spotify.scio.io.CustomIO

class PushPatientEligibilitiesTest extends PipelineSpec {
  "PushProgramEligibilities" should "produce aggregated JSON file with eligibilities" in {
    val project = Environment.Prod.projectName
    val sourceProject = "cbh-rohan-aletty"
    val sourceDataset = "test_dataset"
    val sourceTable = "test_table"
    val sourceLocation = s"$sourceProject.$sourceDataset.$sourceTable"
    val destinationBucket = Environment.Prod.projectName

    val id: String = Transform.generateUUID()

    val testEligibilities = PatientEligibilitiesMocks.eligibilities(Option(id))

    JobTest[PushPatientEligibilities.type]
      .args(
        s"--project=$project",
        s"--sourceProject=$sourceProject",
        s"--sourceDataset=$sourceDataset",
        s"--sourceTable=$sourceTable",
        s"--destinationBucket=$destinationBucket"
      )
      .input(
        BigQueryIO[PatientEligibilities](
          PushPatientEligibilities.eligibilitiesQuery(sourceLocation)
        ),
        Seq(testEligibilities)
      )
      .output(
        CustomIO[PatientEligibilities](PushPatientEligibilities.eligibilityOutputName)
      ) { as =>
        Should.satisfyIterable(as)(_.nonEmpty)
        Should.satisfySingle(as)(_.id.contains(id))
      }
      .run()
  }
}
