package cityblock.streaming.jobs.notes

import cityblock.member.service.models.PatientInfo.Patient
import cityblock.utilities.time.DateOrInstant

object Notes {
  case class Notification(
    ID: Option[String],
    IDType: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    Credentials: List[String],
    Address: Option[Address],
    PhoneNumber: Option[PhoneNumber],
    Location: Option[Location]
  )

  case class Authenticator(
    ID: Option[String],
    IDType: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    Credentials: List[String],
    Address: Option[Address],
    PhoneNumber: Option[PhoneNumber],
    Location: Option[Location]
  )

  case class Location(
    Type: Option[String],
    Facility: Option[String],
    Department: Option[String],
    Room: Option[String]
  )

  case class PhoneNumber(Office: Option[String])

  case class Address(
    StreetAddress: Option[String],
    City: Option[String],
    State: Option[String],
    ZIP: Option[String],
    County: Option[String],
    Country: Option[String]
  )

  case class Provider(
    ID: String,
    IDType: Option[String],
    FirstName: Option[String],
    LastName: Option[String],
    Credentials: List[String],
    Address: Option[Address],
    PhoneNumber: Option[PhoneNumber],
    Location: Option[Location]
  )

  case class Component(
    ID: Option[String],
    Name: Option[String],
    Value: Option[String],
    Comments: Option[String]
  )

  case class Note(
    ContentType: String,
    FileName: Option[String],
    FileContents: Option[String],
    Components: List[Component],
    DocumentType: String,
    DocumentID: String,
    ServiceDateTime: Option[DateOrInstant],
    DocumentationDateTime: Option[DateOrInstant],
    Provider: Provider,
    Status: Option[String],
    Authenticator: Option[Authenticator],
    Availability: Option[String],
    Notifications: List[Notification]
  )

  case class NoteOrder(
    ID: Option[String],
    Name: Option[String]
  )

  case class VisitDischargeDateTime(dateTime: Option[DateOrInstant])

  case class VisitExtensions(`discharge-date-time`: Option[VisitDischargeDateTime])

  case class NoteVisit(
    VisitNumber: Option[String],
    AccountNumber: Option[String],
    VisitDateTime: Option[DateOrInstant],
    Extensions: Option[VisitExtensions]
  )

  case class RawNote(
    Visit: NoteVisit,
    Note: Note,
    Orders: List[NoteOrder]
  )

  case class RawPatientNote(
    messageId: String,
    patient: Patient,
    rawNote: RawNote
  )
}
