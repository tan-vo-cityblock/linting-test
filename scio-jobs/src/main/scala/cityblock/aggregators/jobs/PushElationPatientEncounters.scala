package cityblock.aggregators.jobs

import cityblock.ehrmodels.elation.datamodeldb.bill.BillItemDxTable._
import cityblock.ehrmodels.elation.datamodeldb.bill.BillItemTable._
import cityblock.ehrmodels.elation.datamodeldb.bill.BillTable._
import cityblock.ehrmodels.elation.datamodeldb.documenttag.DocumentTagTable._
import cityblock.ehrmodels.elation.datamodeldb.icd.Icd10Table._
import cityblock.ehrmodels.elation.datamodeldb.icd.Icd9Table._
import cityblock.ehrmodels.elation.datamodeldb.nonvisitnote.NonVisitNoteBulletTable._
import cityblock.ehrmodels.elation.datamodeldb.nonvisitnote.NonVisitNoteDocumentTagTable._
import cityblock.ehrmodels.elation.datamodeldb.nonvisitnote.NonVisitNoteTable._
import cityblock.ehrmodels.elation.datamodeldb.practice.PracticeTable._
import cityblock.ehrmodels.elation.datamodeldb.practice.ServiceLocationTable._
import cityblock.ehrmodels.elation.datamodeldb.user.UserTable._
import cityblock.ehrmodels.elation.datamodeldb.visitnote.VisitNoteBulletTable._
import cityblock.ehrmodels.elation.datamodeldb.visitnote.VisitNoteTable._

import cityblock.member.service.io._
import cityblock.member.service.models.PatientInfo.Patient

import cityblock.streaming.jobs.aggregations.Encounters.PatientEncounter
import cityblock.transforms.elationdb.EncountersTransforms._
import cityblock.utilities.{EhrMedicalDataIO, Environment, SubscriberHelper}

import com.spotify.scio.bigquery._
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ScioContext, ScioResult}

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions
import org.apache.beam.sdk.PipelineResult

import org.joda.time.Instant

object PushElationPatientEncounters extends SubscriberHelper with EhrMedicalDataIO {
  val DATASOURCE = "elation"
  // used as "that" join key in leftOuterJoins where original "that" join key is Optional.empty[T]
  val FALSE_JOIN_KEY: Long = -1

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) =
      ScioContext.parseArguments[DataflowPipelineOptions](cmdlineArgs)

    implicit val environment: Environment = Environment(cmdlineArgs)

    val sc = ScioContext(opts)

    val isStreaming = opts.isStreaming
    val medicalDataset: String = args.required("medicalDataset")

    val icd9s = sc
      .withName(s"Fetch ${Icd9.table}")
      .typedBigQuery[Icd9](Icd9.queryAll)

    val icd10s = sc
      .withName(s"Fetch ${Icd10.table}")
      .typedBigQuery[Icd10](Icd10.queryAll)

    val billItemDxs = sc
      .withName(s"Fetch ${BillItemDx.table}")
      .typedBigQuery[BillItemDx](BillItemDx.queryAll)

    val billItems = sc
      .withName(s"Fetch ${BillItem.table}")
      .typedBigQuery[BillItem](BillItem.queryAll)

    val bills = sc
      .withName(s"Fetch ${Bill.table}")
      .typedBigQuery[Bill](Bill.queryAll)

    val visitNotes = sc
      .withName(s"Fetch ${VisitNote.table}")
      .typedBigQuery[VisitNote](VisitNote.queryAll)

    val visitNoteBullets = sc
      .withName(s"Fetch ${VisitNoteBullet.table}")
      .typedBigQuery[VisitNoteBullet](VisitNoteBullet.queryAll)

    val documentTags = sc
      .withName(s"Fetch ${DocumentTag.table}")
      .typedBigQuery[DocumentTag](DocumentTag.queryAll)

    val nonVisitNoteDocumentTags = sc
      .withName(s"Fetch ${NonVisitNoteDocumentTag.table}")
      .typedBigQuery[NonVisitNoteDocumentTag](NonVisitNoteDocumentTag.queryAll)

    val nonVisitNoteBullets = sc
      .withName(s"Fetch ${NonVisitNoteBullet.table}")
      .typedBigQuery[NonVisitNoteBullet](NonVisitNoteBullet.queryAll)

    val nonVisitNotes = sc
      .withName(s"Fetch ${NonVisitNote.table}")
      .typedBigQuery[NonVisitNote](NonVisitNote.queryAll)

    val serviceLocations = sc
      .withName(s"Fetch ${ServiceLocation.table}")
      .typedBigQuery[ServiceLocation](ServiceLocation.queryAll)

    val practices = sc
      .withName(s"Fetch ${Practice.table}")
      .typedBigQuery[Practice](Practice.queryAll)

    val users = sc
      .withName(s"Fetch ${User.table}")
      .typedBigQuery[User](User.queryAll)

    val fullUsers = makeFullUsers(users, practices)
    val fullBills =
      makeFullBills(icd10s, icd9s, practices, serviceLocations, billItemDxs, billItems, bills)

    val fullVisitNotes =
      makeFullVisitNotes(visitNotes, visitNoteBullets, fullBills, practices, fullUsers)

    val fullNonVisitNotes = makeFullNonVisitNotes(
      nonVisitNoteDocumentTags,
      documentTags,
      nonVisitNotes,
      nonVisitNoteBullets,
      practices,
      fullUsers
    )

    val patients = sc.readMrnPatients(carrier = Option(DATASOURCE))

    val patientEncounters: SCollection[List[PatientEncounter]] = transformElationNotes(
      patients = patients,
      fullVisitNotes = fullVisitNotes,
      fullNonVisitNotes = fullNonVisitNotes,
      practices = practices,
      isStreaming = isStreaming
    )

    savePatientEncounterListToGCS(patientEncounters)
    persistPatientMedicalData(
      patientEncounters,
      s"$medicalDataset.patient_encounters",
      WRITE_APPEND
    )

    val scioResult: ScioResult = sc.close().waitUntilFinish()
    // TODO: Abstract out below to be DRY
    // throwing explicitly to fail Airlfow DAGs that run this job if pipeline completes with non-DONE state.

    scioResult.state match {
      case PipelineResult.State.DONE =>
        logger.info("Batch job succeeded.")
      case _ =>
        val errorStr = "Batch job failed. See logs above."
        logger.error(errorStr)
        throw new Exception(errorStr)
    }
  }

  /**
   * Left join Icd10s and Icd9s to the BillItemDxs by the Icds' primary key id and make FullBillItemDxs.
   */
  def makeFullBillItemDxs(
    billItemDxs: SCollection[BillItemDx],
    icd10s: SCollection[Icd10],
    icd9s: SCollection[Icd9]
  ): SCollection[FullBillItemDx] =
    billItemDxs
      .keyBy(_.icd10_id)
      .leftOuterJoin(icd10s.keyBy(_.id))
      .collect {
        case (_, (billItemDx, icd10)) =>
          (billItemDx.icd9_id.getOrElse(FALSE_JOIN_KEY), (billItemDx, icd10))
      }
      .leftOuterJoin(icd9s.keyBy(_.id))
      .collect {
        case (_, ((billItemDx, icd10), icd9)) =>
          FullBillItemDx(
            billItemDx = billItemDx,
            icd10 = icd10,
            icd9 = icd9
          )
      }

  /**
   * Left join FullBillItemDxs to BillItems by primary key BillItem.id to make FullBillItems.
   */
  def makeFullBillItems(
    billItems: SCollection[BillItem],
    fullBillItemDxs: SCollection[FullBillItemDx]
  ): SCollection[FullBillItem] =
    billItems
      .keyBy(_.id)
      .leftOuterJoin(fullBillItemDxs.groupBy(_.billItemDx.bill_item_id))
      .collect {
        case (_, (billItem, optionalFullBillItemDxs)) =>
          val empty = Iterable.empty[FullBillItemDx]
          val fullBillItemDxs = optionalFullBillItemDxs.fold(empty) { iter =>
            iter
          }
          FullBillItem(billItem = billItem, diagnoses = fullBillItemDxs)
      }

  /**
   * Left join Practices by primary id key, ServiceLocations by primary id key, and FullBillItems
   * by bill_id foreign key to Bill model to make FullBills.
   */
  def makeFullBills(
    icd10s: SCollection[Icd10],
    icd9s: SCollection[Icd9],
    practices: SCollection[Practice],
    serviceLocations: SCollection[ServiceLocation],
    billItemDxs: SCollection[BillItemDx],
    billItems: SCollection[BillItem],
    bills: SCollection[Bill]
  ): SCollection[FullBill] = {
    val billItemIcds = makeFullBillItemDxs(billItemDxs, icd10s, icd9s)
    val fullBillItems = makeFullBillItems(billItems, billItemIcds)

    bills
      .keyBy(_.practice_id)
      .leftOuterJoin(practices.keyBy(_.id))
      .map(_._2)
      .keyBy(tup => tup._1.service_location_id.getOrElse(FALSE_JOIN_KEY))
      .leftOuterJoin(serviceLocations.keyBy(_.id))
      .collect {
        case (_, ((bill, practice), serviceLocation)) =>
          (bill, practice, serviceLocation)
      }
      .keyBy(_._1.id)
      .leftOuterJoin(fullBillItems.groupBy(_.billItem.bill_id))
      .collect {
        case (_, ((bill, practice, serviceLocation), optionalFullBillItems)) =>
          val empty = Iterable.empty[FullBillItem]
          val fullBillItems = optionalFullBillItems.fold(empty) { iter =>
            iter
          }
          FullBill(
            bill = bill,
            practice = practice,
            serviceLocation = serviceLocation,
            fullBillItems = fullBillItems
          )
      }
  }

  /**
   * Left join VisitNoteBullets and FullBills by foreign key visit_note_id to VisitNotes and
   * left join Practices and Users by their primary id keys to VisitNotes to make
   * FullVisitNotes
   */
  def makeFullVisitNotes(
    visitNotes: SCollection[VisitNote],
    visitNoteBullets: SCollection[VisitNoteBullet],
    fullBills: SCollection[FullBill],
    practices: SCollection[Practice],
    fullUsers: SCollection[FullUser]
  ): SCollection[FullVisitNote] =
    MultiJoin
      .left(
        visitNotes
          .keyBy(_.id),
        visitNoteBullets.groupBy(_.visit_note_id),
        fullBills.keyBy(_.bill.visit_note_id)
      )
      .collect {
        case (_, (visitNote, optionalBullets, fullBill)) =>
          val empty = Iterable.empty[VisitNoteBullet]
          val visitNoteBullets = optionalBullets.fold(empty) { iter =>
            iter
          }
          (visitNote, visitNoteBullets, fullBill)
      }
      .keyBy(_._1.practice_id)
      .leftOuterJoin(practices.keyBy(_.id))
      .collect {
        case (_, ((visitNote, visitNoteBullets, fullBill), practice)) =>
          (visitNote, visitNoteBullets, fullBill, practice)
      }
      .keyBy { tup =>
        val physicianId = tup._1.physician_user_id.getOrElse(FALSE_JOIN_KEY)
        tup._1.signed_by_user_id.getOrElse(physicianId)
      }
      .leftOuterJoin(fullUsers.keyBy(_.user.id))
      .collect {
        case (_, ((visitNote, visitNoteBullets, fullBill, practice), fullUser)) =>
          FullVisitNote(
            visitNote = visitNote,
            visitNoteBullets = visitNoteBullets,
            fullBill = fullBill,
            practice = practice,
            fullUser = fullUser
          )
      }

  /**
   * Inner join NonVisitNoteDocumentTags by foreign key document_tag_id to DocumentTags to
   * make FullNonVisitNoteDocumentTags
   */
  def makeFullNonVisitNoteDocumentTags(
    nonVisitNoteDocumentTags: SCollection[NonVisitNoteDocumentTag],
    documentTags: SCollection[DocumentTag]
  ): SCollection[FullNonVisitNoteDocumentTag] =
    nonVisitNoteDocumentTags
      .keyBy(_.document_tag_id)
      .join(documentTags.keyBy(_.id))
      .collect {
        case (_, (nonVisitNoteDocTag, docTag)) =>
          FullNonVisitNoteDocumentTag(
            nonVisitNoteDocumentTag = nonVisitNoteDocTag,
            documentTag = docTag
          )
      }

  /**
   * Left join NonVisitNoteBullets and FullNonVisitNoteDocumentTags by foreign key non_visit_note_id to NonVisitNotes.
   * And left join Practices and Users by their primary keys to NonVisitNotes to make FullNonVisitNotes
   */
  def makeFullNonVisitNotes(
    nonVisitNoteDocumentTags: SCollection[NonVisitNoteDocumentTag],
    documentTags: SCollection[DocumentTag],
    nonVisitNotes: SCollection[NonVisitNote],
    nonVisitNoteBullets: SCollection[NonVisitNoteBullet],
    practices: SCollection[Practice],
    fullUsers: SCollection[FullUser]
  ): SCollection[FullNonVisitNote] = {
    val fullNonVisitNoteDocumentTags =
      makeFullNonVisitNoteDocumentTags(nonVisitNoteDocumentTags, documentTags)

    MultiJoin
      .left(
        nonVisitNotes.keyBy(_.id),
        nonVisitNoteBullets.groupBy(_.non_visit_note_id),
        fullNonVisitNoteDocumentTags.groupBy(_.nonVisitNoteDocumentTag.non_visit_note_id)
      )
      .collect {
        case (_, (nonVisitNote, optionalNonVisitNoteBullets, optionalFullNonVisitNoteDocTags)) =>
          val emptyBullets = Iterable.empty[NonVisitNoteBullet]
          val nonVisitNoteBullets = optionalNonVisitNoteBullets.fold(emptyBullets) { iter =>
            iter
          }
          val emptyTags = Iterable.empty[FullNonVisitNoteDocumentTag]
          val fullNonVisitNoteDocTags = optionalFullNonVisitNoteDocTags.fold(emptyTags) { iter =>
            iter
          }

          (nonVisitNote, nonVisitNoteBullets, fullNonVisitNoteDocTags)
      }
      .keyBy(_._1.practice_id)
      .leftOuterJoin(practices.keyBy(_.id))
      .collect {
        case (_, ((nonVisitNote, nonVisitNoteBullets, fullNonVisitNoteDocumentTags), practice)) =>
          (nonVisitNote, nonVisitNoteBullets, fullNonVisitNoteDocumentTags, practice)
      }
      .keyBy(tup => tup._1.signed_by_user_id.getOrElse(FALSE_JOIN_KEY))
      .leftOuterJoin(fullUsers.keyBy(_.user.id))
      .collect {
        case (_, ((nonVisitNote, nonVisitNoteBullets, fullNonVisitNoteDocTags, practice), user)) =>
          FullNonVisitNote(
            nonVisitNote = nonVisitNote,
            nonVisitNoteBullets = nonVisitNoteBullets,
            fullNonVisitNoteDocumentTag = fullNonVisitNoteDocTags,
            practice = practice,
            fullUser = user
          )
      }
  }

  /**
   * Left join Practices by primary id key to Users to make FullUsers
   */
  def makeFullUsers(
    users: SCollection[User],
    practices: SCollection[Practice]
  ): SCollection[FullUser] =
    users
      .keyBy(_.practice_id)
      .leftOuterJoin(practices.keyBy(_.id))
      .collect {
        case (_, (user, practice)) =>
          FullUser(
            user = user,
            practice = practice
          )
      }

  /**
   * Co-group FullVisitNotes and NonVisitNotes by key elation patient_id.
   * Right join grouped notes by their patient_id (externalId) to Patients from MemberService.
   * Transform notes to PatientEncounters and use Instant.now as messageId.
   */
  def transformElationNotes(
    patients: SCollection[Patient],
    fullVisitNotes: SCollection[FullVisitNote],
    fullNonVisitNotes: SCollection[FullNonVisitNote],
    practices: SCollection[Practice],
    isStreaming: Boolean
  )(implicit environment: Environment): SCollection[List[PatientEncounter]] = {
    val insertedAt = Instant.now
    fullVisitNotes
      .keyBy(_.visitNote.patient_id)
      .cogroup(fullNonVisitNotes.keyBy(_.nonVisitNote.patient_id))
      .rightOuterJoin(patients.keyBy(_.externalId.toLong))
      .collect {
        case (_, (Some((fullVisitNotes, fullNonVisitNotes)), patient)) =>
          transformElationNotesToPatientEncounters(
            insertedAt = insertedAt,
            isStreaming = isStreaming,
            patient = patient,
            fullVisitNotes = fullVisitNotes,
            fullNonVisitNotes = fullNonVisitNotes
          )
      }
  }
}
