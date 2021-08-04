package cityblock.transforms.cardinal.gold

import java.util.UUID

import cityblock.models.CardinalSilverClaims.SilverPriorAuthorization
import cityblock.models.cardinal.silver.PriorAuthorization.ParsedPriorAuthorization
import cityblock.models.{Identifier, Surrogate}
import cityblock.models.gold.Address
import cityblock.models.gold.PriorAuthorization.{
  AuthorizationServiceStatus,
  Diagnosis,
  Facility,
  PriorAuthorization,
  Procedure,
  ProviderIdentifier
}
import cityblock.transforms.Transform
import cityblock.utilities.Conversions
import com.spotify.scio.ContextAndArgs
import com.spotify.scio.values.SCollection
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO.Write.WriteDisposition.WRITE_TRUNCATE
import org.joda.time.LocalDate
import org.joda.time.format.DateTimeFormat

object PriorAuthorizationTransformer {
  def main(argv: Array[String]): Unit = {
    val (sc, args) = ContextAndArgs(argv)

    val bigqueryProject: String = args.required("bigqueryProject")

    val dtf = DateTimeFormat.forPattern(Transform.ShardNamePattern)
    val deliveryDate: LocalDate = LocalDate.parse(args.required("deliveryDate"), dtf)

    val silverDataset = args.required("silverDataset")
    val silverTable = args.required("silverTable")

    val goldDataset = args.required("goldDataset")
    val goldTable = args.required("goldTable")

    val silverPriorAuthorizations: SCollection[SilverPriorAuthorization] =
      Transform.fetchFromBigQuery[SilverPriorAuthorization](
        sc,
        bigqueryProject,
        silverDataset,
        silverTable,
        deliveryDate
      )

    val goldPriorAuthorizations = silverPriorAuthorizations
      .withName("Add surrogates")
      .map {
        Transform.addSurrogate(
          bigqueryProject,
          silverDataset,
          silverTable,
          _
        )(_.identifier.surrogateId)
      }
      .withName("Construct prior authorizations")
      .map {
        case (surrogate, silver) =>
          PriorAuthorizationTransformer.mkPriorAuthorization(surrogate, silver)
      }

    Transform.persist(
      goldPriorAuthorizations,
      bigqueryProject,
      goldDataset,
      goldTable,
      deliveryDate,
      WRITE_TRUNCATE,
      CREATE_IF_NEEDED
    )

    sc.close().waitUntilFinish()
  }

  private def mkPriorAuthorization(surrogate: Surrogate,
                                   silver: SilverPriorAuthorization): PriorAuthorization =
    PriorAuthorization(
      identifier = mkIdentifier(surrogate, silver.data.AuthorizationID),
      memberIdentifier = mkMemberIdentifier(silver.patient),
      authorizationId = silver.data.AuthorizationID,
      authorizationStatus = None,
      placeOfService = silver.data.PlaceOfService,
      careType = silver.data.Priority,
      caseType = None,
      admitDate = silver.data.AdmitDate,
      dischargeDate = silver.data.DischargeDate,
      serviceStartDate = silver.data.ServiceStartDate,
      serviceEndDate = silver.data.ServiceEndDate,
      facilityIdentifier = mkFacilityIdentifier(silver.data),
      diagnosis = mkDiagnoses(silver.data),
      procedures = mkProcedures(silver.data),
      servicingProvider =
        mkProvider(silver.data.ServicingProviderNPI, silver.data.ServicingProviderName),
      requestingProvider =
        mkProvider(silver.data.ReferringProviderNPI, silver.data.ReferringProviderName),
      serviceStatus = mkServiceStatus(silver.data.AuthorizationStatus),
      serviceType = silver.data.TypeOfService,
      requestDate = mkRequestDate(silver.data.RequestDateTime),
      statusReason = silver.data.StatusReason,
      recordCreatedDate = None,
      recordUpdatedDate = None
    )

  private def mkIdentifier(surrogate: Surrogate, authorizationIdField: String): Identifier =
    Identifier(
      id = UUID.nameUUIDFromBytes(authorizationIdField.getBytes()).toString,
      partner = partner,
      surrogate = surrogate
    )

  private def mkFacilityIdentifier(data: ParsedPriorAuthorization): Facility =
    Facility(
      facilityNPI = data.FacilityNPI,
      facilityName = data.FacilityName,
      facilityAddress = Address(
        address1 = data.FacilityAddress1,
        address2 = data.FacilityAddress2,
        city = data.FacilityCity,
        state = data.FacilityState,
        county = None,
        zip = data.FacilityZip,
        country = None,
        email = None,
        phone = None
      )
    )

  private def mkDiagnoses(data: ParsedPriorAuthorization): List[Diagnosis] =
    List(
      Diagnosis(
        diagnosisCode = data.DiagnosisCode,
        diagnosisDescription = data.DiagnosisDescription
      )
    )

  private def mkProcedures(data: ParsedPriorAuthorization): List[Procedure] =
    List(
      Procedure(
        code = data.ProcedureCode,
        modifiers = List.empty,
        serviceUnits = data.ApprovedUnits,
        unitType = None
      )
    )

  private def mkProvider(
    providerNpiField: Option[String],
    providerNameField: Option[String]
  ): ProviderIdentifier =
    ProviderIdentifier(
      providerId = providerNpiField.map(mkProviderId),
      partnerProviderId = None,
      providerNPI = providerNpiField,
      specialty = None,
      providerName = providerNameField
    )

  private def mkServiceStatus(serviceStatusField: Option[String]): Option[String] = {
    val status = serviceStatusField match {
      case Some("1") => Some(AuthorizationServiceStatus.Approved)
      case _         => None
    }
    status.map(_.toString)
  }

  private def mkRequestDate(maybeDateTime: Option[String]): Option[LocalDate] =
    maybeDateTime.map(_.substring(0, 10)).flatMap(Conversions.safeParse(_, LocalDate.parse))
}
