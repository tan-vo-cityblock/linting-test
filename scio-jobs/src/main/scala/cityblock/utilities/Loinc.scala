package cityblock.utilities

import scala.language.implicitConversions

object Loinc {
  val codeSystem = "2.16.840.1.113883.6.1"
  val codeSystemName = "LOINC"

  type VitalSign = VitalSign.Value

  object VitalSign extends Enumeration {
    protected case class Val(code: String, elationName: String, name: String, unit: String)
        extends super.Val

    implicit def valueToVitalSignVal(x: Value): Val = x.asInstanceOf[Val]

    val BMI = Val("39156-5", "bmi", "Body mass index", "kg/m2")
    val Height = Val("8302-2", "height", "Body height", "cm")
    val Weight = Val("29463-7", "weight", "Body weight", "kg")
    val Oxygen = Val("59408-5", "oxygen", "Oxygen saturation", "%")
    val RespiratoryRate = Val("9279-1", "rr", "Respiratory rate", "/min")
    val HeartRate = Val("8867-4", "hr", "Heart rate", "/min")
    val HeadCircum = Val("8287-5", "hc", "Head circumference", "cm")
    val Temperature = Val("8310-5", "temperature", "Body temperature", "Cel")
    val SystolicBP =
      Val("8480-6", "systolic", "Systolic blood pressure", "mm[Hg]")
    val DiastolicBP =
      Val("8462-4", "diastolic", "Diastolic blood pressure", "mm[Hg]")

  }
}
