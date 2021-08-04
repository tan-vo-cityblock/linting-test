package cityblock.member.service.api

import io.circe.generic.JsonCodec

@JsonCodec
case class Demographics(firstName: Option[String],
                        middleName: Option[String],
                        lastName: Option[String],
                        dateOfBirth: Option[String],
                        dateOfDemise: Option[String],
                        sex: Option[String],
                        gender: Option[String],
                        ethnicity: Option[String],
                        race: Option[String],
                        language: Option[String],
                        maritalStatus: Option[String],
                        ssn: Option[String],
                        ssnLastFour: Option[String],
                        addresses: List[Address],
                        phones: List[Phone],
                        emails: List[Email],
                        updatedBy: Option[String])

@JsonCodec
case class Address(id: Option[String],
                   addressType: Option[String],
                   street1: Option[String],
                   street2: Option[String],
                   county: Option[String],
                   city: Option[String],
                   state: Option[String],
                   zip: Option[String],
                   spanDateStart: Option[String],
                   spanDateEnd: Option[String])

@JsonCodec
case class Email(id: Option[String], email: String)

@JsonCodec
case class Phone(id: Option[String], phone: String, phoneType: Option[String])
