package cityblock.member.service.io

import cityblock.member.service.models.PatientInfo._
import cityblock.member.service.api.{
  MemberCreateAndPublishRequest,
  MemberCreateRequest,
  MemberInsuranceRecord
}
import cityblock.utilities.{Environment, Loggable}
import com.spotify.scio.ScioContext
import com.spotify.scio.io.{EmptyTap, EmptyTapOf, ScioIO, Tap}
import com.spotify.scio.values.SCollection

import scala.concurrent.Future

sealed trait MemberIndexIO[T] extends ScioIO[T] with Loggable {
  override type ReadP = Unit
  override type WriteP = Unit

  final override val tapT = EmptyTapOf[T]

  override def tap(params: Unit): Tap[Nothing] = EmptyTap
}

object MemberIndexIO {

  /**
   * Loads all the members for the given partner, represented Patient records that only contain the
   * member id and the external id for the partner. These are keyed by the external id.
   */
  def readInsurancePatients(carrier: Option[String])(
    implicit environment: Environment): MemberIndexIO[(String, Patient)] =
    ReadInsurancePatients(carrier)

  def readMrnPatients(carrier: Option[String])(
    implicit environment: Environment): MemberIndexIO[Patient] =
    ReadMrnPatients(carrier)

  def readMemberInsuranceRecords(carrier: Option[String])(
    implicit environment: Environment): MemberIndexIO[MemberInsuranceRecord] =
    ReadMemberInsuranceRecords(carrier)

  def publishMembers(
    implicit environment: Environment): MemberIndexIO[(String, MemberCreateAndPublishRequest)] =
    PublishMembers()

  def writeCreateMembers(implicit environment: Environment): MemberIndexIO[MemberCreateRequest] =
    WriteCreateMembers()
}

final private case class ReadInsurancePatients(carrier: Option[String])(
  implicit environment: Environment
) extends MemberIndexIO[(String, Patient)] {
  override def write(data: SCollection[(String, Patient)], params: Unit): Future[Tap[Nothing]] =
    throw new IllegalStateException("ReadInsurancePatients is read-only")

  override def read(sc: ScioContext, params: Unit): SCollection[(String, Patient)] =
    MemberService.getInsurancePatients(carrier = carrier) match {
      case Right(patients) => sc.parallelize(patients).keyBy(p => p.externalId)
      case Left(apiError) =>
        throw new Exception(
          s"Failed to read Member Index for member records [carrier: ${carrier}, error: ${apiError.toString}]")
    }
}

final case class ReadMrnPatients(carrier: Option[String])(
  implicit environment: Environment
) extends MemberIndexIO[Patient] {
  override def write(data: SCollection[Patient], params: Unit): Future[Tap[Nothing]] =
    throw new IllegalStateException("ReadInsurancePatients is read-only")

  override def read(sc: ScioContext, params: Unit): SCollection[Patient] =
    MemberService.getMrnPatients(carrier = carrier) match {
      case Right(patients) => sc.parallelize(patients)
      case Left(apiError) =>
        throw new Exception(
          s"Failed to read Member Index for MRN records [carrier: ${carrier}, error: ${apiError.toString}]")
    }
}

final private case class WriteCreateMembers()(implicit environment: Environment)
    extends MemberIndexIO[MemberCreateRequest] {
  override def read(sc: ScioContext, params: Unit): SCollection[MemberCreateRequest] =
    throw new IllegalStateException("WriteCreateMembers is write-only")

  override def write(
    memberCreates: SCollection[MemberCreateRequest],
    params: Unit
  ): Future[Tap[Nothing]] = {
    // TODO: Handle errors better?
    memberCreates.map { createRequest =>
      {
        MemberService.createMember(createRequest) match {
          case Right(response) => response
          case Left(apiError) =>
            logger.error(s"Error creating member: $apiError")
        }
      }
    }

    Future.successful(EmptyTap)
  }
}

final private case class PublishMembers()(implicit environment: Environment)
    extends MemberIndexIO[(String, MemberCreateAndPublishRequest)] {
  override def read(
    sc: ScioContext,
    params: Unit
  ): SCollection[(String, MemberCreateAndPublishRequest)] =
    throw new IllegalStateException("PublishMembers is write-only")

  override def write(
    membersToPublish: SCollection[(String, MemberCreateAndPublishRequest)],
    params: Unit
  ): Future[Tap[Nothing]] = {
    membersToPublish.map { memberIdAndRequest =>
      val (memberId, publishRequest) = memberIdAndRequest
      MemberService
        .publishMemberAttribution(memberId, publishRequest)
        .left
        .map(apiError => logger.error(s"Error publishing member: $apiError"))
    }

    Future.successful(EmptyTap)
  }
}

final case class CreateAndPublishMembers()(implicit environment: Environment)
    extends MemberIndexIO[MemberCreateAndPublishRequest] {
  override def read(sc: ScioContext, params: Unit): SCollection[MemberCreateAndPublishRequest] =
    throw new IllegalStateException("PublishMembers is write-only")

  override def write(
    membersToPublish: SCollection[MemberCreateAndPublishRequest],
    params: Unit
  ): Future[Tap[Nothing]] = {
    membersToPublish.map { publishRequest: MemberCreateAndPublishRequest =>
      MemberService
        .createAndPublishMember(publishRequest)
        .left
        .map(apiError => logger.error(s"Error publishing member: $apiError"))
    }

    Future.successful(EmptyTap)
  }
}

final case class ReadMemberInsuranceRecords(carrier: Option[String])(
  implicit environment: Environment)
    extends MemberIndexIO[MemberInsuranceRecord] {

  override protected def read(
    sc: ScioContext,
    params: Unit
  ): SCollection[MemberInsuranceRecord] =
    MemberService.getMemberInsuranceMapping(carrier = carrier) match {
      case Right(memberInsuranceRecords) => sc.parallelize(memberInsuranceRecords)
      case Left(apiError) =>
        throw new Exception(
          s"Failed to read Member Index for insurance records [carrier: ${carrier}, error: ${apiError.toString}]"
        )
    }

  override protected def write(
    data: SCollection[MemberInsuranceRecord],
    params: ReadMemberInsuranceRecords.this.WriteP): Future[Tap[Nothing]] =
    throw new IllegalStateException("ReadInsurancePatients is read-only")
}
