package cityblock.transforms.carefirst.gold

class MedicalMapping {
  def isFirstLine(lineNumber: Option[String]): Boolean = lineNumber.contains("1")
  def cleanDiagnosisCode(diagnosisCode: String): String = diagnosisCode.replace(".", "")
}
