package cityblock.aggregators.jobs

import cityblock.aggregators.models.ClaimsEncounters.{
  ClaimsEncounter,
  ClaimsEncounterAddress,
  PatientClaimsEncounters
}
import cityblock.models.gold.Claims.Diagnosis
import cityblock.models.gold.FacilityClaim.{Facility, HeaderProcedure}
import cityblock.models.gold.NewProvider.Provider
import cityblock.models.gold.ProfessionalClaim.Professional
import cityblock.models.gold.enums.{DiagnosisTier, ProcedureTier}
import cityblock.models.gold.{Address, Helpers, ProfessionalClaim}
import cityblock.transforms.Transform
import cityblock.transforms.Transform.CodeSet
import cityblock.utilities.Environment
import cityblock.utilities.reference.tables._
import com.spotify.scio.bigquery._
import com.spotify.scio.io.Tap
import com.spotify.scio.util.MultiJoin
import com.spotify.scio.values.SCollection
import com.spotify.scio.{ContextAndArgs, ScioContext}
import io.circe.Encoder

import scala.concurrent.Future

object PushPatientClaimsEncounters {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val sourceProject = args.required("sourceProject")
    val sourceDataset = args.getOrElse("sourceDataset", default = "gold_claims")
    val deliveryDate = args.required("deliveryDate")
    implicit val environment: Environment = Environment(argv)

    pushPatientClaimsEncounters(sc, sourceProject, sourceDataset, deliveryDate)

    sc.close().waitUntilDone()
  }

  def pushPatientClaimsEncounters(sc: ScioContext,
                                  sourceProject: String,
                                  sourceDataset: String,
                                  deliveryDate: String)(
    implicit environment: Environment): Future[Tap[PatientClaimsEncounters]] = {

    val providerIndex: SCollection[(Option[String], Provider)] = sc
      .withName("Fetch providers")
      .typedBigQuery[Provider](providerQuery(sourceProject, sourceDataset, deliveryDate))
      .groupBy(_.providerIdentifier.id)
      .values
      // NOTE: Set null date as the min so we always choose a provider with a date, if there is one.
      .map(_.maxBy(_.dateEffective.from)(Transform.NoneMinOptionLocalDateOrdering))
      .withName("Index by id")
      .keyBy(x => Option[String](x.providerIdentifier.id))

    val icd10Index: SCollection[(Option[String], ICD10Cm)] =
      ICD10Cm.fetchAll(sc).keyBy(x => Option[String](x.code))
    val hcpcsIndex: SCollection[(Option[String], HCPCS)] =
      HCPCS.fetchAll(sc).keyBy(x => Option[String](x.code))
    val posIndex: SCollection[(Option[String], PlaceOfService)] =
      PlaceOfService.fetchAll(sc).keyBy(x => Option[String](x.code))
    val tosIndex: SCollection[(Option[String], TypeOfService)] =
      TypeOfService.fetchAll(sc).keyBy(x => Option[String](x.code))

    val professional: SCollection[Professional] = sc
      .withName("Fetch professional claims with patient id")
      .typedBigQuery[Professional](professionalQuery(sourceProject, sourceDataset, deliveryDate))

    val facility: SCollection[Facility] = sc
      .withName("Fetch facility claims with patient id")
      .typedBigQuery[Facility](facilityQuery(sourceProject, sourceDataset, deliveryDate))

    val indexedFacility =
      indexFacilityEncounters(facility, icd10Index, hcpcsIndex, tosIndex, posIndex, providerIndex)

    val indexedProfessional =
      indexProfessionalEncounters(professional,
                                  icd10Index,
                                  hcpcsIndex,
                                  tosIndex,
                                  posIndex,
                                  providerIndex)

    import io.circe.generic.semiauto._
    implicit val e1: Encoder[ClaimsEncounterAddress] =
      deriveEncoder[ClaimsEncounterAddress]
    implicit val e2: Encoder[ClaimsEncounter] =
      deriveEncoder[ClaimsEncounter]

    val aggregated = indexedFacility
      .withName("Union facility and professional encounters")
      .union(indexedProfessional)
      .withName("Filter distinct")
      .distinct
      .withName("Aggregate by patient id")
      .groupByKey
      .withName("Wrap aggregated encounters")
      .map {
        case (patientId, encounters) =>
          PatientClaimsEncounters(patientId, encounters.toList)
      }

    PatientClaimsEncounters.saveToGCS(aggregated)
  }

  private[jobs] def professionalQuery(project: String,
                                      dataset: String,
                                      deliveryDate: String): String =
    s"""
       |WITH
       |  memberClaims AS (
       |  SELECT
       |    *
       |  FROM
       |    `$project.$dataset.Professional_$deliveryDate`
       |  WHERE
       |    memberIdentifier.patientId IS NOT NULL),
       |  headerInfo AS (
       |  SELECT
       |    claimId,
       |    memberIdentifier,
       |    header
       |  FROM
       |    memberClaims),
       |  cleanedLines AS (
       |  SELECT
       |    claimId,
       |    l.surrogate,
       |    l.lineNumber,
       |    l.cobFlag,
       |    l.capitatedFlag,
       |    l.claimLineStatus,
       |    l.inNetworkFlag,
       |    l.serviceQuantity,
       |    l.placeOfService,
       |    l.typesOfService,
       |    l.date,
       |    l.provider,
       |    l.procedure,
       |    l.diagnoses,
       |    STRUCT(NULL AS allowed,
       |      NULL AS billed,
       |      NULL AS cob,
       |      NULL AS copay,
       |      NULL AS deductible,
       |      NULL AS coinsurance,
       |      NULL AS planPaid) amount
       |  FROM
       |    memberClaims,
       |    UNNEST(lines) l),
       |  groupedLines AS (
       |  SELECT
       |    claimId,
       |    ARRAY_AGG(STRUCT(surrogate,
       |        lineNumber,
       |        cobFlag,
       |        capitatedFlag,
       |        claimLineStatus,
       |        inNetworkFlag,
       |        serviceQuantity,
       |        placeOfService,
       |        date,
       |        provider,
       |        `procedure`,
       |        diagnoses,
       |        typesOfService,
       |        amount)) lines
       |  FROM
       |    cleanedLines
       |  GROUP BY
       |    claimId)
       |SELECT
       |  claimId,
       |  memberIdentifier,
       |  header,
       |  lines
       |FROM
       |  headerInfo
       |JOIN
       |  groupedLines
       |USING
       |  (claimId)
     """.stripMargin

  private[jobs] def facilityQuery(project: String, dataset: String, deliveryDate: String): String =
    s"""
       |WITH
       |  memberClaims AS (
       |  SELECT
       |    *
       |  FROM
       |    `$project.$dataset.Facility_$deliveryDate`
       |  WHERE
       |    memberIdentifier.patientId IS NOT NULL),
       |  headerInfo AS (
       |  SELECT
       |    claimId,
       |    memberIdentifier,
       |    header
       |  FROM
       |    memberClaims),
       |  cleanedLines AS (
       |  SELECT
       |    claimId,
       |    l.surrogate,
       |    l.lineNumber,
       |    l.revenueCode,
       |    l.cobFlag,
       |    l.capitatedFlag,
       |    l.claimLineStatus,
       |    l.inNetworkFlag,
       |    l.serviceQuantity,
       |    l.typesOfService,
       |    l.procedure,
       |    STRUCT(NULL AS allowed,
       |      NULL AS billed,
       |      NULL AS cob,
       |      NULL AS copay,
       |      NULL AS deductible,
       |      NULL AS coinsurance,
       |      NULL AS planPaid) amount
       |  FROM
       |    memberClaims,
       |    UNNEST(lines) l),
       |  groupedLines AS (
       |  SELECT
       |    claimId,
       |    ARRAY_AGG(STRUCT(surrogate,
       |        lineNumber,
       |        revenueCode,
       |        cobFlag,
       |        capitatedFlag,
       |        claimLineStatus,
       |        inNetworkFlag,
       |        serviceQuantity,
       |        typesOfService,
       |        `procedure`,
       |        amount)) lines
       |  FROM
       |    cleanedLines
       |  GROUP BY
       |    claimId)
       |SELECT
       |  claimId,
       |  memberIdentifier,
       |  header,
       |  lines
       |FROM
       |  headerInfo
       |JOIN
       |  groupedLines
       |USING
       |  (claimId)
     """.stripMargin

  private[jobs] def providerQuery(project: String, dataset: String, deliveryDate: String): String =
    s"""
       |SELECT * FROM `$project.$dataset.Provider_$deliveryDate`
     """.stripMargin

  /**
   * Construct a single encounter for each facility claim, and key by patientId.
   * Claims are omitted if they lack a matching procedure or provider.
   */
  private def indexFacilityEncounters(facility: SCollection[Facility],
                                      icd10Index: SCollection[(Option[String], ICD10Cm)],
                                      hcpcsIndex: SCollection[(Option[String], HCPCS)],
                                      tosIndex: SCollection[(Option[String], TypeOfService)],
                                      posIndex: SCollection[(Option[String], PlaceOfService)],
                                      providerIndex: SCollection[(Option[String], Provider)])
    : SCollection[(String, ClaimsEncounter)] = {
    def indexFn(f: Facility): String = f.claimId

    val withICD10Dx = Transform.leftJoinAndIndex(
      facility,
      icd10Index,
      (f: Facility) => getPrincipalDiagnosis(f.header.diagnoses).map(_.code),
      indexFn)

    val withHCPCSPx = Transform.joinAndIndex(
      facility,
      hcpcsIndex,
      (f: Facility) => getPrincipalHeaderProcedure(f.header.procedures).map(_.code),
      indexFn)

    val withTOS =
      Transform.leftJoinAndIndex(facility, tosIndex, (f: Facility) => {
        val tos = f.lines.flatMap(_.typesOfService)
        if (tos.nonEmpty) { Some(tos.minBy(_.tier).code) } else { None }
      }, indexFn)

    val withProvider = Transform.joinAndIndex(facility,
                                              providerIndex,
                                              (f: Facility) => f.header.provider.billing.map(_.id),
                                              indexFn)

    MultiJoin
      .withName("Join encounter parameters")
      .apply(facility.keyBy(indexFn), withICD10Dx, withHCPCSPx, withTOS, withProvider)
      .withName("Construct encounters")
      .flatMap {
        case (_, (claim, dx, px, tos, provider)) =>
          for {
            address: Address <- clinicAddress(provider)
            patientId: String <- claim.memberIdentifier.patientId
            date: String <- encounterDate(claim)
          } yield {
            (patientId,
             ClaimsEncounter(
               provider = provider.name,
               summary = px.summary,
               date = Some(date),
               address = providerAddress(address),
               placeOfService = None,
               typeOfService = tos.map(_.description),
               diagnosis = dx.map(_.name),
               description = px.description
             ))
          }
      }
  }

  /**
   * Construct a single encounter for each professional line, indexed by patientId.
   * Claim lines are omitted if they lack a matching procedure or provider.
   */
  // scalastyle:off cyclomatic.complexity
  private def indexProfessionalEncounters(professional: SCollection[Professional],
                                          icd10Index: SCollection[(Option[String], ICD10Cm)],
                                          hcpcsIndex: SCollection[(Option[String], HCPCS)],
                                          tosIndex: SCollection[(Option[String], TypeOfService)],
                                          posIndex: SCollection[(Option[String], PlaceOfService)],
                                          providerIndex: SCollection[(Option[String], Provider)])
    : SCollection[(String, ClaimsEncounter)] =
    professional
      .withName("Flatten patient ids")
      .flatMap { claim =>
        for (patientId <- claim.memberIdentifier.patientId)
          yield (patientId, claim)
      }
      .withName("Explode claim into lines and index by principal dx code")
      .flatMap {
        case (patientId, claim) =>
          // For now, use claim's principal diagnosis for each encounter.
          val code = getPrincipalDiagnosis(claim.header.diagnoses).map(_.code)
          claim.lines.map(l => (code, (patientId, l)))
      }
      .withName("Join with ICD10 diagnosis index")
      .leftOuterJoin(icd10Index)
      .withName("Gather results")
      .map {
        case (_, ((patientId, line), diagnosis)) =>
          (patientId, line, diagnosis)
      }
      .withName("Index by procedure code")
      .keyBy {
        case (_, line, _) => line.procedure.map(_.code)
      }
      .withName("Join with HCPCS procedure index")
      .join(hcpcsIndex)
      .withName("Gather results")
      .map {
        case (_, ((patientId, line, diagnosis), procedure)) =>
          (patientId, line, diagnosis, procedure)
      }
      .withName("Index by servicing provider id")
      .keyBy {
        case (_, line, _, _) => line.provider.servicing.map(_.id)
      }
      .join(providerIndex)
      .withName("Gather results")
      .map {
        case (_, ((patientId, line, diagnosis, procedure), provider)) =>
          (patientId, line, diagnosis, procedure, provider)
      }
      .withName("Index by TOS code")
      .keyBy {
        case (_, line, _, _, _) =>
          Helpers.primaryTypeOfService(line.typesOfService)
      }
      .withName("Join with TOS index")
      .leftOuterJoin(tosIndex)
      .withName("Gather results")
      .map {
        case (_, ((patientId, line, diagnosis, procedure, provider), tos)) =>
          (patientId, line, diagnosis, procedure, provider, tos)
      }
      .withName("Index by POS code")
      .keyBy {
        case (_, line, _, _, _, _) => line.placeOfService
      }
      .withName("Join with POS index")
      .leftOuterJoin(posIndex)
      .withName("Construct encounters")
      .flatMap {
        case (_, ((patientId, line, diagnosis, procedure, provider, tos), pos)) =>
          for {
            address: Address <- clinicAddress(provider)
            date: String <- encounterDate(line)
          } yield
            (patientId,
             ClaimsEncounter(
               provider = provider.name,
               summary = procedure.summary,
               date = Some(date),
               address = providerAddress(address),
               placeOfService = pos.map(_.name),
               typeOfService = tos.map(_.description),
               diagnosis = diagnosis.map(_.name),
               description = procedure.description
             ))
      }
  // scalastyle:on cyclomatic.complexity

  /**
   * Get the principal ICD10 diagnosis from @a dxs. If there are is no principal
   * diagnosis, pick the first available.
   */
  private def getPrincipalDiagnosis(dxs: List[Diagnosis]): Option[Diagnosis] = {
    def isPrincipal(dx: Diagnosis): Boolean =
      dx.tier == DiagnosisTier.Principal.name
    def isICD10(dx: Diagnosis): Boolean = dx.codeset == CodeSet.ICD10Cm.toString

    dxs.find(dx => isPrincipal(dx) && isICD10(dx)) match {
      case Some(dx) => Some(dx)
      case _        => dxs.find(isICD10)
    }
  }

  /**
   * Get the principal header procedure from @a pxs. If there is no principal header procedure,
   * pick the first available.
   */
  private def getPrincipalHeaderProcedure(pxs: List[HeaderProcedure]): Option[HeaderProcedure] = {
    def isPrincipal(px: HeaderProcedure): Boolean =
      px.tier == ProcedureTier.Principal.name

    pxs.find(isPrincipal) match {
      case Some(px) => Some(px)
      case _ =>
        pxs match {
          case head :: _ => Some(head)
          case _         => None
        }
    }
  }

  private def clinicAddress(provider: Provider): Option[Address] =
    // TODO: Eventually find a more accurate way of selecting the correct address. Perhaps there's a way to join on
    // claims to see if a location there matches any of the Provider's locations?
    provider.locations.flatMap(_.clinic).headOption

  private def providerAddress(address: Address): ClaimsEncounterAddress =
    ClaimsEncounterAddress(
      street1 = address.address1,
      street2 = address.address2,
      city = address.city,
      state = address.state,
      zip = address.zip
    )

  /**
   * Get the encounter date for @a claim. Return the claim's 'admit' date if
   * present, otherwise fall back to the 'from' date.
   */
  private def encounterDate(claim: Facility): Option[String] = {
    val date = claim.header.date match {
      case d if d.admit.nonEmpty => d.admit
      case d                     => d.from
    }

    date.map(_.toString)
  }

  private def encounterDate(line: ProfessionalClaim.Line): Option[String] =
    line.date.from.map(_.toString)
}
