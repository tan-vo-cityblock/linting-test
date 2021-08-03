package cityblock.utilities

import org.scalatest.{FlatSpec, Matchers}

class EnvironmentTest extends FlatSpec with Matchers {

  "apply()" should "load defined environment" in {
    val args = Array("--environment=test", "--otherFlag")
    Environment(args) should be(Environment.Test)
  }

  "apply()" should "override project" in {
    val args = Array("--environment=test", "--project=overrideProject")
    val environment = Environment(args)
    environment.name should be("test")
    environment.projectName should be("overrideProject")
    environment.patientDataBucket should be(Environment.Test.patientDataBucket)
  }

  "apply()" should "override patientDataBucket" in {
    val args = Array("--environment=test", "--patientDataBucket=overridePatientDataBucket")
    val environment = Environment(args)
    environment.name should be("test")
    environment.projectName should be(Environment.Test.projectName)
    environment.patientDataBucket should be("overridePatientDataBucket")
  }

  "apply()" should "override multiple values" in {
    val args = Array("--environment=test",
                     "--project=overrideProject",
                     "--patientDataBucket=overridePatientDataBucket")
    val environment = Environment(args)
    environment.name should be("test")
    environment.projectName should be("overrideProject")
    environment.patientDataBucket should be("overridePatientDataBucket")
  }
}
