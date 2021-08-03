package cityblock.member.attribution.transforms.common

trait NorthCarolinaTransforms {
  def ethnicityTransform(ethnicityCode: String): String = ethnicityCode match {
    case "H" | "M" | "P" => "Hispanic or Latino"
    case "N"             => "Not Hispanic or Latino"
    case _               => "Unknown Ethnicity"
  }
}
