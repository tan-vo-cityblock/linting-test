package cityblock.importers.mirroring.config

import java.util.UUID

import cityblock.importers.generic.FailedParseContainer.FailedParse
import cityblock.importers.mirroring.DatabaseMirroringRunner
import cityblock.transforms.Transform
import com.spotify.scio.bigquery.{BigQueryIO, TableRow}
import com.spotify.scio.io.TextIO
import com.spotify.scio.testing.{DistCacheIO, PipelineSpec}
import org.joda.time.LocalDate

class DatabaseMirroringTest extends PipelineSpec {

  // TODO: running into some really weird temp location issues on test
  ignore should "output a mirrored table on BQ" in {
    val environment = "test"
    val project = "cityblock-data"
    val gcpTempLocation = "gs://internal-tmp-cbh/temp"
    val sourceBucket = "test-bucket"
    val outputProject = "cityblock-data"
    val schemaFilePath = "schema_file"
    val dataFilePath = "data_file"
    val instanceName = "test_instance"
    val databaseName = "database"
    val tableName = "table"

    val schemaFile = Seq(
      "id,uuid,NO",
      "field1,integer,NO",
      "field2,boolean,YES"
    )

    val dataFile = Seq(
      s"${UUID.randomUUID().toString},1,true",
      s"${UUID.randomUUID().toString},2,false",
      s"${UUID.randomUUID().toString},3,true",
      s"${UUID.randomUUID().toString},4"
    )

    val date = new LocalDate()
    val outputDatabaseName = s"cloud_sql_mirror_$instanceName"
    val outputTableName =
      Transform.shardName(s"${databaseName}_$tableName", date)
    val outputErrorsTableName =
      Transform.shardName(s"${databaseName}_${tableName}_errors", date)
    val successOutput = s"$outputProject:$outputDatabaseName.$outputTableName"
    val failureOutput =
      s"$outputProject:$outputDatabaseName.$outputErrorsTableName"

    JobTest[DatabaseMirroringRunner.type]
      .args(
        s"--environment=$environment",
        s"--project=$project",
        s"--tempLocation=$gcpTempLocation",
        s"--sourceBucket=$sourceBucket",
        s"--outputProject=$outputProject",
        s"--schemaFilePath=$schemaFilePath",
        s"--dataFilePath=$dataFilePath",
        s"--instanceName=$instanceName",
        s"--databaseName=$databaseName",
        s"--tableName=$tableName"
      )
      .input(
        TextIO(s"gs://$sourceBucket/$dataFilePath"),
        dataFile
      )
      .distCache(
        DistCacheIO(s"gs://$sourceBucket/$schemaFilePath"),
        schemaFile
      )
      .output(BigQueryIO[TableRow](successOutput))(
        elements => {
          elements should haveSize(4)
        }
      )
      .output(BigQueryIO[FailedParse](failureOutput))(
        elements => {
          elements should haveSize(0)
        }
      )
      .run()
  }

}
