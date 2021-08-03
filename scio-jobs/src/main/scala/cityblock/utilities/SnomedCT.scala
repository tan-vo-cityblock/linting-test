package cityblock.utilities

import scala.language.implicitConversions

import io.circe.Decoder

/**
 * Object holds Enum objects of various panels of SNOMED codes and names.
 * Enums contain attributes per scala-lang docs:  https://www.scala-lang.org/api/current/scala/Enumeration.html
 *
 * Note: Elation CCDA object only seems to return the Complaint Problem Type, while Problem object
 * does not indicate Problem Type. Follow-up with Elation.
 */
object SnomedCT {
  val codeSystem = "2.16.840.1.113883.6.96"
  val codeSystemName = "SNOMED CT"

  type ProblemType = ProblemType.Value

  object ProblemType extends Enumeration {

    protected case class Val(code: String, name: String) extends super.Val

    implicit def valueToProblemCategoryVal(x: Value): Val = x.asInstanceOf[Val]

    val Complaint = Val("409586006", "Complaint")
    val Problem = Val("55607006", "Problem")
    val Finding = Val("404684003", "Finding")
    val Diagnosis = Val("282291009", "Diagnosis")
    val Condition = Val("64572001", "Condition")
    val FunctionalLimitation = Val("248536006", "Functional Limitation")
    val Symptom = Val("418799008", "Symptom")
  }

  type ProblemStatus = ProblemStatus.Value

  object ProblemStatus extends Enumeration {
    protected case class Val(code: String, name: String) extends super.Val

    implicit def valueToProblemStatusVal(x: Value): Val = x.asInstanceOf[Val]

    val Active = Val("55561003", "Active")
    val Inactive = Val("73425007", "Inactive")
    val Resolved = Val("413322009", "Resolved")
    val Unknown = Val("Unknown", "Unknown")

    implicit val decodeProblemStatus: Decoder[ProblemStatus] =
      Decoder.decodeString.map {
        case "Active" =>
          ProblemStatus.Active
        case "Controlled" =>
          ProblemStatus.Inactive
        case "Resolved" =>
          ProblemStatus.Resolved
        case unknown =>
          ProblemStatus.Unknown
      }
  }
}
