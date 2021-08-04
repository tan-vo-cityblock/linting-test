package cityblock.transforms.emblem.gold

import cityblock.models.EmblemSilverClaims.SilverPriorAuthorization
import cityblock.models.Identifier
import cityblock.models.emblem.silver.PriorAuthorization.ParsedPriorAuthorization
import cityblock.models.gold.Address
import cityblock.models.gold.Claims.MemberIdentifier
import cityblock.models.gold.PriorAuthorization.{
  Diagnosis,
  Facility,
  PriorAuthorization,
  Procedure,
  ProviderIdentifier
}
import cityblock.streaming.jobs.hie.Events.Provider
import cityblock.streaming.jobs.hie.HieMessagesHandler.PublishablePatientHieEvent
import cityblock.transforms.Transform
import cityblock.utilities.ResultSigner
import cityblock.utilities.time.DateOrInstant
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection
import org.joda.time.{DateTime, DateTimeZone, LocalDate, LocalTime}
import org.joda.time.format.DateTimeFormat
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import cityblock.streaming.jobs.hie.Events
import io.circe.generic.auto._

import scala.concurrent.Future

object PriorAuthToHIETransformer {

  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val bigqueryProject: String = args.required("bigqueryProject")
    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val deliveryDate: LocalDate = LocalDate.parse(args.required("deliveryDate"), dtf)
    val source: Option[String] = args.optional("source")
    val previousDeliveryDate: Option[LocalDate] =
      args.optional("previousDeliveryDate").map(LocalDate.parse(_, dtf))
    val sendToCommons: Boolean = args.boolean("sendToCommons")

    val priorAuths: SCollection[SilverPriorAuthorization] =
      Transform.fetchFromBigQuery[SilverPriorAuthorization](
        sc,
        bigqueryProject,
        "silver_claims",
        "prior_authorization",
        deliveryDate
      )

    val goldPriorAuths: SCollection[PriorAuthorization] = makePriorAuth(priorAuths, bigqueryProject)
    persistPriorAuth(goldPriorAuths, bigqueryProject, deliveryDate)

    for {
      previousDate <- previousDeliveryDate if sendToCommons
      existingData = Transform.fetchFromBigQuery[PriorAuthorization](
        sc,
        bigqueryProject,
        "gold_claims",
        "PriorAuthorization",
        previousDate
      )
      dataSource <- source
    } yield publishPriorAuthMessage(goldPriorAuths, existingData, dataSource)

    sc.close().waitUntilFinish()
  }

  def publishPriorAuthMessage(
    priorAuths: SCollection[PriorAuthorization],
    previousPriorAuths: SCollection[PriorAuthorization],
    source: String
  ): Unit = {
    val hieEventTopic = "projects/cityblock-data/topics/hieEventMessages"
    val priorAuthsForCommons: SCollection[PriorAuthorization] = {
      priorAuths
        .groupBy(_.authorizationId)
        .leftOuterJoin(previousPriorAuths.groupBy(_.authorizationId))
        .values
        // Only publish new authorizations. Emblem sends us full histories daily, so if we have already published the
        // authorization with a certain ID, we do not want to publish it again as it would be duplicated on the
        // Commons timeline.
        // TODO: Update this diffing logic to republish updates to an authorization.
        .filter(_._2.isEmpty)
        .map(_._1.head)
    }

    priorAuthsForCommons
      .filter(_.caseType.contains("Concurrent"))
      .filter(priorAuth =>
        priorAuth.careType.contains("Emergency") || priorAuth.careType.contains("Urgent"))
      .flatMap(priorAuth => {
        import io.circe.syntax._

        priorAuth.memberIdentifier.patientId match {
          case Some(patientId) =>
            val event = PublishablePatientHieEvent(
              patientId = patientId,
              source = source,
              eventDateTime = priorAuth.serviceStartDate.map(makeEstDate),
              dischargeDateTime = priorAuth.serviceEndDate.map(makeEstDate),
              notes = makeNoteForCommons(priorAuth),
              locationName = priorAuth.facilityIdentifier.facilityName.getOrElse("Unknown"),
              fullLocationName = None,
              eventType = if (isDischarge(priorAuth)) Some("Discharge") else Some("Admit"),
              visitType = priorAuth.careType,
              locationAddress = priorAuth.facilityIdentifier.facilityAddress.address1,
              diagnoses = priorAuth.diagnosis.map(
                diag =>
                  Events.Diagnosis(
                    codeset = None,
                    code = diag.diagnosisCode,
                    description = diag.diagnosisDescription
                )),
              procedures = priorAuth.procedures,
              facilityType = priorAuth.placeOfService,
              admissionSource = None,
              locationCity = priorAuth.facilityIdentifier.facilityAddress.city,
              locationZip = priorAuth.facilityIdentifier.facilityAddress.zip,
              locationState = priorAuth.facilityIdentifier.facilityAddress.state,
              provider = Some(
                Provider(
                  priorAuth.servicingProvider.providerId,
                  priorAuth.servicingProvider.providerName,
                  None
                )),
              messageId = None
            )
            val payload = event.asJson.noSpaces
            val hmac = ResultSigner.signResult(payload)

            Seq((payload, Map("topic" -> "hieEvent", "hmac" -> hmac)))
          case None => Seq()
        }
      })
      .withName("publishPriorAuthorizationsAsHieEventMessages")
      .saveAsPubsubWithAttributes[String](hieEventTopic)
  }

  def makeEstDate(date: LocalDate): DateOrInstant = {
    val estDateTime: DateTime =
      date.toLocalDateTime(LocalTime.MIDNIGHT).toDateTime(DateTimeZone.forID("America/New_York"))
    DateOrInstant(estDateTime.toString, None, None)
  }

  def isDischarge(priorAuthorization: PriorAuthorization): Boolean =
    priorAuthorization.dischargeDate.isDefined

  /**
   * This is temporary while we wait until Commons has the time to implement this on the front end, instead of doing
   * string manipulation here.
   *
   * @param priorAuthorization gold Prior Authorization
   * @return Option[String]
   */
  def makeNoteForCommons(priorAuthorization: PriorAuthorization): Option[String] = {
    val eventType: String =
      if (isDischarge(priorAuthorization)) "discharged from " else "admitted to "
    val diagnosisStr: Option[String] =
      priorAuthorization.diagnosis.headOption
        .flatMap(_.diagnosisDescription)
        .map(description => " with " + description.trim + ".")

    val facilityStr: Option[String] =
      priorAuthorization.facilityIdentifier.facilityAddress.phone.map(phone => {
        "The member was " + eventType + priorAuthorization.facilityIdentifier.facilityName
          .map(_.trim)
          .getOrElse(" ") + " - " + phone.trim
      })

    val providerStr: Option[String] = {
      val name: Option[String] = priorAuthorization.servicingProvider.providerName
      if (name.isDefined || priorAuthorization.servicingProvider.providerNPI.isDefined) {
        val providerName: Option[String] = if (name.exists(_.contains(","))) {
          val names = name.map(_.split(","))
          if (names.exists(_.length > 1))
            names.map(nameList => nameList(1).trim + " " + nameList(0).trim)
          else names.map(_.head.trim)
        } else {
          name.map(_.trim)
        }
        Some(
          "The servicing provider name is " + providerName.map(_.trim).getOrElse("") + " (NPI: " +
            priorAuthorization.servicingProvider.providerNPI.map(_.trim).getOrElse("") + ").")
      } else {
        None
      }
    }

    if (diagnosisStr.isDefined || facilityStr.isDefined || providerStr.isDefined) {
      Some(List(facilityStr, diagnosisStr, providerStr).flatten.mkString(" "))
    } else {
      None
    }
  }

  def persistPriorAuth(
    priorAuths: SCollection[PriorAuthorization],
    project: String,
    deliveryDate: LocalDate
  ): Future[Tap[PriorAuthorization]] =
    Transform.persist(
      priorAuths,
      project,
      "gold_claims",
      "PriorAuthorization",
      deliveryDate,
      WRITE_TRUNCATE,
      CREATE_IF_NEEDED
    )

  def makePriorAuth(
    priorAuths: SCollection[SilverPriorAuthorization],
    project: String
  ): SCollection[PriorAuthorization] =
    priorAuths
      .groupBy(_.data.CaseNumber)
      .values
      .flatMap(priorAuthorizations => {
        val mainPA = priorAuthorizations.head
        val priorAuth: ParsedPriorAuthorization = mainPA.data
        for {
          id <- priorAuth.CaseNumber
        } yield {
          PriorAuthorization(
            identifier = identifier(mainPA, project),
            memberIdentifier = memberIdentifier(mainPA, project),
            authorizationId = id,
            authorizationStatus = priorAuth.Case_Status,
            placeOfService = priorAuth.Place_of_Service,
            careType = priorAuth.Care_Type,
            caseType = priorAuth.Case_Type,
            admitDate = priorAuth.Admit_Dt,
            dischargeDate = priorAuth.Disch_Dt,
            serviceStartDate = priorAuth.Service_Start_Dt.map(LocalDate.parse),
            serviceEndDate = priorAuth.Service_End_Dt.map(LocalDate.parse),
            facilityIdentifier = facility(priorAuth),
            diagnosis = diagnoses(priorAuth),
            procedures = procedures(priorAuthorizations),
            servicingProvider = servicingProvider(priorAuth),
            requestingProvider = ProviderIdentifier(None, None, None, None, None),
            serviceStatus = priorAuth.Service_Status_Desc,
            serviceType = priorAuth.Service_Type,
            requestDate = priorAuth.Request_Recvd_Dt.map(LocalDate.parse),
            statusReason = priorAuth.Reason,
            recordCreatedDate = priorAuth.Create_Dt,
            recordUpdatedDate = priorAuth.Last_Updated.map(LocalDate.parse)
          )
        }
      })

  def identifier(silver: SilverPriorAuthorization, project: String): Identifier =
    Identifier(
      id = PriorAuthorizationKey(silver.data.CaseNumber.getOrElse("")).uuid.toString,
      partner = project.split("-").head,
      surrogate = Transform
        .addSurrogate(
          project,
          "silver_claims",
          "prior_authorization",
          silver
        )(_.identifier.surrogateId)
        ._1
    )

  def memberIdentifier(silver: SilverPriorAuthorization, project: String): MemberIdentifier =
    MemberIdentifier(
      commonId = silver.patient.source.commonId,
      partnerMemberId = silver.patient.externalId,
      patientId = silver.patient.patientId,
      partner = project.split("-").head
    )

  def diagnoses(priorAuth: ParsedPriorAuthorization): List[Diagnosis] = {
    val diagnosisInfo = priorAuth.DX_Code_DX_Desc.map(_.split("-"))
    List(
      Diagnosis(
        diagnosisCode = diagnosisInfo.flatMap(_.headOption),
        diagnosisDescription = diagnosisInfo.map(_.tail).toList.flatten.headOption
      ))
  }

  def procedures(priorAuths: Iterable[SilverPriorAuthorization]): List[Procedure] =
    priorAuths
      .map(priorAuth => {
        Procedure(
          code = priorAuth.data.Pxc_Code,
          modifiers = List.empty,
          serviceUnits = priorAuth.data.Aprv_Units,
          unitType = priorAuth.data.Unit_Type
        )
      })
      .toList

  def servicingProvider(priorAuth: ParsedPriorAuthorization): ProviderIdentifier =
    ProviderIdentifier(
      providerId = None,
      partnerProviderId = None,
      providerNPI = priorAuth.Treating_Prov_NPI,
      specialty = None,
      providerName = priorAuth.Treating_Prov_Name
    )

  def facility(priorAuth: ParsedPriorAuthorization): Facility =
    Facility(
      facilityNPI = priorAuth.Fac_NPI,
      facilityName = priorAuth.Fac_Name,
      facilityAddress = Address(
        address1 = priorAuth.Fac_Address,
        address2 = None,
        city = priorAuth.Fac_City,
        state = priorAuth.Fac_State,
        county = None,
        zip = priorAuth.Fac_Zip,
        country = None,
        email = None,
        phone = priorAuth.Fac_Phone
      )
    )
}
