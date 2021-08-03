package cityblock.ehrmodels.elation.datamodelapi.patient

import io.circe.{Decoder, Encoder}

object Enums {
  object Gender extends Enumeration {
    type Gender = Value
    val Male, Female, Unknown = Value

    implicit val genderDecoder: Decoder[Gender.Value] =
      Decoder.enumDecoder(Gender)
    implicit val genderEncoder: Encoder[Gender.Value] =
      Encoder.enumEncoder(Gender)
  }

  object Race extends Enumeration {
    type Race = Value

    val Unknown = Value("No race specified")
    val Asian = Value("Asian")
    val AmericanIndian = Value("American Indian or Alaska Native")
    val African = Value("Black or African American")
    val PacificIslander = Value("Native Hawaiian or Other Pacific Islander")
    val White = Value("White")
    val Declined = Value("Declined to specify")

    implicit val RaceDecoder: Decoder[Race.Value] = Decoder.enumDecoder(Race)
    implicit val RaceEncoder: Encoder[Race.Value] = Encoder.enumEncoder(Race)
  }

  object Ethnicity extends Enumeration {
    type Ethnicity = Value

    val Unknown = Value("No ethnicity specified")
    val Latino = Value("Hispanic or Latino")
    val NonLatino = Value("Not Hispanic or Latino")
    val Declined = Value("Declined to specify")

    implicit val EthnicityDecoder: Decoder[Ethnicity.Value] =
      Decoder.enumDecoder(Ethnicity)
    implicit val EthnicityEncoder: Encoder[Ethnicity.Value] =
      Encoder.enumEncoder(Ethnicity)
  }

  object EContactRelationship extends Enumeration {
    type EContactRelationship = Value
    val Caregiver, Child, Friend, Grandparent, Guardian, Parent, Sibling, Spouse, Other = Value

    implicit val ECRDecoder: Decoder[EContactRelationship.Value] =
      Decoder.enumDecoder(EContactRelationship)
    implicit val ECREncoder: Encoder[EContactRelationship.Value] =
      Encoder.enumEncoder(EContactRelationship)
  }

  object InsuranceRank extends Enumeration {
    type InsuranceRank = Value
    val primary, secondary, tertiary = Value

    implicit val InsuranceRankDecoder: Decoder[InsuranceRank.Value] =
      Decoder.enumDecoder(InsuranceRank)
    implicit val InsuranceRankEncoder: Encoder[InsuranceRank.Value] =
      Encoder.enumEncoder(InsuranceRank)
  }

  object PhoneType extends Enumeration {
    type PhoneType = Value
    val Mobile, Home, Main, Work, Night, Fax, Other = Value

    implicit val PhoneTypeDecoder: Decoder[PhoneType.Value] =
      Decoder.enumDecoder(PhoneType)
    implicit val PhoneTypeEncoder: Encoder[PhoneType.Value] =
      Encoder.enumEncoder(PhoneType)
  }
}
