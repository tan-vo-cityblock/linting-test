package cityblock.streaming.jobs.orders

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.bigquery.types.BigQueryType

object Orders {
  @BigQueryType.toTable
  case class ResultObservationMethod(
    Code: Option[String],
    Codeset: Option[String],
    Description: Option[String]
  )

  @BigQueryType.toTable
  case class ResultReferenceRange(
    Low: Option[String],
    High: Option[String],
    Text: Option[String]
  )

  @BigQueryType.toTable
  case class ResultPerformer(
    ID: Option[String],
    IDType: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    Credentials: List[String],
    Address: Option[Address],
    Location: Option[Location],
    PhoneNumber: Option[PhoneNumber]
  )

  @BigQueryType.toTable
  case class ResultProducer(
    ID: Option[String],
    Name: Option[String],
    IDType: Option[String],
    Address: Option[Address]
  )

  @BigQueryType.toTable
  case class ResultSpecimen(
    Source: Option[String],
    BodySite: Option[String],
    ID: Option[String]
  )

  @BigQueryType.toTable
  case class Result(
    Code: Option[String],
    Codeset: Option[String],
    Description: Option[String],
    Specimen: Option[ResultSpecimen],
    Value: String,
    ValueType: String,
    CompletionDateTime: Option[DateOrInstant],
    FileType: Option[String],
    Units: Option[String],
    Notes: List[String],
    AbnormalFlag: Option[String],
    Status: String,
    Producer: Option[ResultProducer],
    Performer: Option[ResultPerformer],
    ReferenceRange: Option[ResultReferenceRange],
    ObservationMethod: Option[ResultObservationMethod]
  )

  @BigQueryType.toTable
  case class PhoneNumber(
    Office: Option[String]
  )

  @BigQueryType.toTable
  case class Location(
    Type: Option[String],
    Facility: Option[String],
    Department: Option[String],
    Room: Option[String]
  )

  @BigQueryType.toTable
  case class Address(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  @BigQueryType.toTable
  case class Provider(
    NPI: Option[String],
    ID: Option[String],
    IDType: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    Credentials: List[String],
    Address: Option[Address],
    Location: Option[Location],
    PhoneNumber: Option[PhoneNumber]
  )

  @BigQueryType.toTable
  case class Procedure(
    Code: Option[String],
    Codeset: Option[String],
    Description: Option[String]
  )

  @BigQueryType.toTable
  case class Order(
    ID: String,
    ApplicationOrderID: Option[String],
    TransactionDateTime: Option[DateOrInstant],
    CollectionDateTime: Option[DateOrInstant],
    CompletionDateTime: Option[DateOrInstant],
    Notes: List[String],
    ResultsStatus: Option[String],
    Procedure: Option[Procedure],
    Provider: Option[Provider],
    ResultCopyProviders: List[Provider],
    Status: String,
    ResponseFlag: Option[String],
    Priority: Option[String],
    Results: List[Result]
  )

  @BigQueryType.toTable
  case class PatientOrder(
    messageId: String,
    patient: Patient,
    order: Order
  )
}
