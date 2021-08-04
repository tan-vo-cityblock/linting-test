package cityblock.transforms.elationdb

import cityblock.ehrmodels.elation.datamodeldb.bill.BillTable.FullBill
import cityblock.ehrmodels.elation.datamodeldb.nonvisitnote.NonVisitNoteBulletTable.NonVisitNoteBullet
import cityblock.ehrmodels.elation.datamodeldb.nonvisitnote.NonVisitNoteDocumentTagTable.FullNonVisitNoteDocumentTag
import cityblock.ehrmodels.elation.datamodeldb.nonvisitnote.NonVisitNoteTable.FullNonVisitNote
import cityblock.ehrmodels.elation.datamodeldb.practice.PracticeTable.Practice
import cityblock.ehrmodels.elation.datamodeldb.user.UserTable.FullUser
import cityblock.ehrmodels.elation.datamodeldb.visitnote.VisitNoteBulletTable.VisitNoteBullet
import cityblock.ehrmodels.elation.datamodeldb.visitnote.VisitNoteTable.FullVisitNote
import cityblock.member.service.models.PatientInfo.Patient
import cityblock.streaming.jobs.aggregations.Encounters._
import cityblock.utilities.{Conversions, Loggable}
import org.joda.time.Instant
import org.joda.time.format.DateTimeFormat

object EncountersTransforms extends Loggable {
  final val US_PACIFIC_TIMEZONE_ID = "US/Pacific"
  def transformElationNotesToPatientEncounters(
    insertedAt: Instant,
    isStreaming: Boolean,
    patient: Patient,
    fullVisitNotes: Iterable[FullVisitNote],
    fullNonVisitNotes: Iterable[FullNonVisitNote]
  ): List[PatientEncounter] = {
    val formatter = DateTimeFormat.forPattern("yyyyMMddHHmmssS")
    val messageId = insertedAt.toDateTime.toString(formatter)
    val visitPatientEncounters = fullVisitNotes.map { fullVisitNote =>
      PatientEncounter(
        isStreaming = isStreaming,
        insertedAt = Some(insertedAt),
        messageId = messageId,
        patient = patient,
        encounter = makeEncounter(fullVisitNote)
      )
    }.toList

    val nonVisitPatientEncounters = fullNonVisitNotes.map { fullNonVisitNotes =>
      PatientEncounter(
        isStreaming = isStreaming,
        insertedAt = Some(insertedAt),
        messageId = messageId,
        patient = patient,
        encounter = makeEncounter(fullNonVisitNotes)
      )
    }.toList

    visitPatientEncounters ::: nonVisitPatientEncounters
  }

  private def makeEncounter(fullVisitNote: FullVisitNote): Encounter = {
    val encounterReasons = makeEncounterReasons(fullVisitNote.visitNoteBullets)
    val encounterIdVisitNote =
      Identifier(Some(fullVisitNote.visitNote.id.toString), Some("Visit Note"))
    val encounterProviders =
      makeEncounterProviders(fullVisitNote.fullUser)

    val dateTime =
      Conversions.mkDateOrInstant(fullVisitNote.visitNote.document_date, US_PACIFIC_TIMEZONE_ID)
    val endDateTime =
      Conversions.mkDateOrInstant(fullVisitNote.visitNote.signed_time, US_PACIFIC_TIMEZONE_ID)

    val (encounterType, encounterDiagnoses, encounterIds, encounterLocations) =
      fullVisitNote.fullBill.fold {
        logger.info(s"No bill exists for visit note with Id ${fullVisitNote.visitNote.id}")
        val encounterType = makeEncounterType(fullVisitNote)
        val encounterDiagnoses = List.empty[EncounterDiagnosis]
        val encounterIds = List(encounterIdVisitNote)
        val encounterLocations = List.empty[EncounterLocation]
        (encounterType, encounterDiagnoses, encounterIds, encounterLocations)
      } { fullBill =>
        val encounterType = makeEncounterType(fullVisitNote = fullVisitNote, fullBill = fullBill)
        val encounterDiagnoses = makeEncounterDiagnoses(fullBill = fullBill)
        val encounterIds = List(
          encounterIdVisitNote,
          Identifier(Some(fullBill.bill.id.toString), Some("Bill"))
        )
        val encounterLocations =
          makeEncounterLocations(fullBill)
        (encounterType, encounterDiagnoses, encounterIds, encounterLocations)
      }

    Encounter(
      identifiers = encounterIds,
      `type` = encounterType,
      dateTime = Some(dateTime),
      endDateTime = endDateTime,
      providers = encounterProviders,
      locations = encounterLocations,
      diagnoses = encounterDiagnoses,
      reasons = encounterReasons,
      draft = endDateTime.isEmpty
    )
  }

  private def makeEncounter(fullNonVisitNote: FullNonVisitNote): Encounter = {
    val encounterType = makeEncounterType(fullNonVisitNote)
    val encounterIds =
      List(Identifier(Some(fullNonVisitNote.nonVisitNote.id.toString), Some("Non-Visit Note")))

    val encounterReasons = makeEncounterReasons(
      fullNonVisitNote.nonVisitNoteBullets,
      fullNonVisitNote.fullNonVisitNoteDocumentTag
    )

    val dateTime =
      Conversions.mkDateOrInstant(
        fullNonVisitNote.nonVisitNote.document_date,
        US_PACIFIC_TIMEZONE_ID
      )
    val endDateTime =
      Conversions.mkDateOrInstant(fullNonVisitNote.nonVisitNote.signed_time, US_PACIFIC_TIMEZONE_ID)

    val encounterProviders = makeEncounterProviders(fullNonVisitNote.fullUser)

    Encounter(
      identifiers = encounterIds,
      `type` = encounterType,
      dateTime = Some(dateTime),
      endDateTime = endDateTime,
      providers = encounterProviders,
      locations = List.empty[EncounterLocation],
      diagnoses = List.empty[EncounterDiagnosis],
      reasons = encounterReasons,
      draft = endDateTime.isEmpty
    )
  }

  private def makeEncounterReasons(
    visitNoteBullets: Iterable[VisitNoteBullet]
  ): List[EncounterReason] =
    visitNoteBullets
      .groupBy(bullet => bullet.parent_bullet_id.getOrElse(bullet.id))
      .flatMap { tup =>
        tup._2.toList
          .sortWith(sortVisitNoteBullets)
          .map { bullet =>
            EncounterReason(
              code = None,
              codeSystem = None,
              codeSystemName = None,
              name = Some(bullet.category),
              notes = Some(bullet.text)
            )
          }
      }
      .toList

  private def sortVisitNoteBullets(bulletA: VisitNoteBullet, bulletB: VisitNoteBullet): Boolean =
    (bulletA.parent_bullet_id.isEmpty && bulletB.parent_bullet_id.nonEmpty) ||
      (bulletA.sequence < bulletB.sequence)

  private def makeEncounterReasons(
    nonVisitNoteBullets: Iterable[NonVisitNoteBullet],
    fullNonVisitNoteDocumentTags: Iterable[FullNonVisitNoteDocumentTag]
  ): List[EncounterReason] = {
    val noteBulletReasons = nonVisitNoteBullets.toList
      .map { bullet =>
        EncounterReason(
          code = None,
          codeSystem = None,
          codeSystemName = None,
          name = None,
          notes = Some(bullet.text)
        )
      }

    val documentTagReasons = fullNonVisitNoteDocumentTags.toList
      .map { tag =>
        EncounterReason(
          code = tag.documentTag.code,
          codeSystem = None,
          codeSystemName = tag.documentTag.code_type,
          name = Some(tag.documentTag.value),
          notes = tag.documentTag.description
        )
      }

    noteBulletReasons ::: documentTagReasons
  }

  private def makeEncounterProviders(fullUser: Option[FullUser]): List[EncounterProvider] = {
    val emptyAddressAndPhone = (Address(None, None, None, None, None), Option.empty[String])
    val (ids, firstName, lastName, credentials, address, phone, role) =
      fullUser.fold {
        (
          List[Identifier](),
          Option.empty[String],
          Option.empty[String],
          List[String](),
          emptyAddressAndPhone._1,
          emptyAddressAndPhone._2,
          Option.empty[EncounterProviderRole]
        )
      } { fullUser =>
        val npi = fullUser.user.npi.fold(List[Identifier]()) { npi =>
          List(Identifier(id = Some(npi), idType = Some("npi")))
        }
        val ids = npi ::: List(Identifier(id = Some(fullUser.user.email), idType = Some("email")))
        val role = Some(
          EncounterProviderRole(
            code = None,
            codeSystem = None,
            codeSystemName = None,
            name = fullUser.user.specialty
          )
        )
        val credentials = fullUser.user.credentials.fold(List[String]()) { credential =>
          List(credential)
        }
        val (providerAddress, providerPhone) = fullUser.practice.fold(emptyAddressAndPhone) {
          practice =>
            (
              Address(
                street1 = Some(practice.address_line1),
                street2 = practice.address_line2,
                city = Some(practice.city),
                state = Some(practice.state),
                zip = Some(practice.zip)
              ),
              practice.phone
            )
        }
        (
          ids,
          fullUser.user.first_name,
          fullUser.user.last_name,
          credentials,
          providerAddress,
          providerPhone,
          role
        )
      }
    List(
      EncounterProvider(
        identifiers = ids,
        firstName = firstName,
        lastName = lastName,
        credentials = credentials,
        address = Some(address),
        phone = phone,
        role = role
      )
    )
  }

  private def makeEncounterType(fullVisitNote: FullVisitNote, fullBill: FullBill): EncounterType =
    fullBill.fullBillItems.toList
      .sortWith(_.billItem.cpt > _.billItem.cpt) match {
      case fullBillItem :: _ if fullBillItem.billItem.cpt.nonEmpty =>
        EncounterType(
          code = fullBillItem.billItem.cpt,
          codeSystem = None,
          codeSystemName = Some("CPT"),
          name = fullVisitNote.visitNote.name
        )
      case _ =>
        makeEncounterType(fullVisitNote)
    }
  private def makeEncounterType(fullVisitNote: FullVisitNote): EncounterType =
    EncounterType(
      code = "Unknown",
      codeSystem = None,
      codeSystemName = None,
      name = fullVisitNote.visitNote.name
    )

  private def makeEncounterType(fullNonVisitNote: FullNonVisitNote): EncounterType =
    EncounterType(
      code = "Unknown",
      codeSystem = None,
      codeSystemName = None,
      name = fullNonVisitNote.nonVisitNote.note_type
    )

  private def makeEncounterDiagnoses(fullBill: FullBill): List[EncounterDiagnosis] =
    fullBill.fullBillItems.toList.flatMap { fullBillItem =>
      val cpt = EncounterDiagnosis(
        code = Some(fullBillItem.billItem.cpt),
        codeSystem = None,
        codeSystemName = Some("CPT"),
        name = None
      )

      val dxs = fullBillItem.diagnoses.toList.flatMap { dx =>
        val empty = List.empty[EncounterDiagnosis]
        val icd10Dx = dx.icd10.fold(empty) { icd10 =>
          List(
            EncounterDiagnosis(
              code = Some(icd10.code),
              codeSystem = None,
              codeSystemName = Some("ICD-10"),
              name = Some(icd10.description)
            )
          )
        }
        val icd9Dx = dx.icd9.fold(empty) { icd9 =>
          List(
            EncounterDiagnosis(
              code = Some(icd9.code),
              codeSystem = None,
              codeSystemName = Some("ICD-9"),
              name = Some(icd9.description)
            )
          )
        }
        icd10Dx ::: icd9Dx
      }

      List(cpt) ::: dxs
    }

  private def makeEncounterLocations(fullBill: FullBill): List[EncounterLocation] =
    fullBill.serviceLocation.fold {
      logger.error(
        s"""No service location was set for visit note bill with bill id ${fullBill.bill.id}""".stripMargin
      )
      val practiceLocation = makeEncounterLocations(fullBill.practice)
      if (practiceLocation.isEmpty) {
        logger.error(
          s"""No practice was set for visit note bill with bill id ${fullBill.bill.id}""".stripMargin
        )
      }
      practiceLocation
    } { serviceLocation =>
      val placeOfServiceCode = fullBill.bill.place_of_service.fold(Option.empty[String]) { place =>
        Some(place.toString)
      }
      val encounterLocationAddressType = EncounterLocationAddressType(
        code = placeOfServiceCode,
        codeSystem = None,
        codeSystemName = Some("CMS Place of Service Code Set"),
        name = None
      )
      val encounterLocationAddress = Address(
        street1 = Some(serviceLocation.address),
        street2 = serviceLocation.suite,
        city = Some(serviceLocation.city),
        state = Some(serviceLocation.state),
        zip = Some(serviceLocation.zip)
      )
      List(
        EncounterLocation(
          address = Some(encounterLocationAddress),
          `type` = Some(encounterLocationAddressType),
          name = None
        )
      )
    }
  private def makeEncounterLocations(practice: Option[Practice]): List[EncounterLocation] = {
    val empty = List.empty[EncounterLocation]
    practice.fold {
      empty
    } { practiceLocation =>
      val encounterLocationAddressType = EncounterLocationAddressType(
        code = None,
        codeSystem = None,
        codeSystemName = None,
        name = None
      )
      val encounterLocationAddress = Address(
        street1 = Some(practiceLocation.address_line1),
        street2 = practiceLocation.address_line2,
        city = Some(practiceLocation.city),
        state = Some(practiceLocation.state),
        zip = Some(practiceLocation.zip)
      )
      List(
        EncounterLocation(
          address = Some(encounterLocationAddress),
          `type` = Some(encounterLocationAddressType),
          name = Some(practiceLocation.name)
        )
      )
    }
  }
}
