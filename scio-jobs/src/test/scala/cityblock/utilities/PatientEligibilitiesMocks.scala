package cityblock.utilities

import cityblock.aggregators.models.PatientEligibilities._

object PatientEligibilitiesMocks {
  //noinspection ScalaStyle
  def eligibilities(
    patientId: Option[String],
    healthHomeState: Option[String] = Option("enrolled"),
    healthHomeName: Option[String] = Option("Some Health Home"),
    careManagementState: Option[String] = Option("enrolled"),
    careManagementName: Option[String] = Option("Some Association"),
    harpState: Option[String] = Option("eligible"),
    harpStateDescription: Option[String] = Option("eligible for program"),
    harpEligibility: Option[String] = Option("eligible"),
    hivSnpHarpState: Option[String] = Option("ineligible"),
    hcbsState: Option[String] = Option("ineligible")
  ): PatientEligibilities =
    PatientEligibilities(
      id = patientId,
      healthHomeState = healthHomeState,
      healthHomeName = healthHomeName,
      careManagementState = careManagementState,
      careManagementName = careManagementName,
      harpState = harpState,
      harpStateDescription = harpStateDescription,
      harpEligibility = harpEligibility,
      hivSnpHarpState = hivSnpHarpState,
      hcbsState = hcbsState
    )
}
