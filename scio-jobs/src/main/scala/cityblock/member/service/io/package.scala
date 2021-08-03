package cityblock.member.service

import cityblock.member.service.api.{
  MemberCreateAndPublishRequest,
  MemberCreateRequest,
  MemberInsuranceRecord
}
import cityblock.member.service.models.PatientInfo._
import cityblock.utilities.Environment
import com.spotify.scio.ScioContext
import com.spotify.scio.io.Tap
import com.spotify.scio.values.SCollection

import scala.concurrent.Future

package object io {
  implicit final class PostgresContext(
    @transient private val self: ScioContext
  ) extends AnyVal {

    def readMemberInsuranceRecords(carrier: Option[String] = None)(
      implicit environment: Environment): SCollection[MemberInsuranceRecord] =
      self.read(MemberIndexIO.readMemberInsuranceRecords(carrier))

    def readInsurancePatients(carrier: Option[String] = None)(
      implicit environment: Environment
    ): SCollection[(String, Patient)] =
      self.read(MemberIndexIO.readInsurancePatients(carrier))

    def readMrnPatients(carrier: Option[String] = None)(
      implicit environment: Environment): SCollection[Patient] =
      self.read(MemberIndexIO.readMrnPatients(carrier))
  }

  implicit final class FullMemberSCollection(
    @transient private val self: SCollection[MemberCreateRequest]
  ) extends AnyVal {
    def writeToMemberIndex(implicit environment: Environment): Future[Tap[Nothing]] =
      self.write(MemberIndexIO.writeCreateMembers)
  }

  implicit final class PublishMemberSCollection(
    @transient private val self: SCollection[(String, MemberCreateAndPublishRequest)]
  ) extends AnyVal {
    def updateAndPublishToMemberService(implicit environment: Environment): Future[Tap[Nothing]] =
      self.write(MemberIndexIO.publishMembers)
  }

  implicit final class CreateAndPublishMemberSCollection(
    @transient private val self: SCollection[MemberCreateAndPublishRequest]
  ) extends AnyVal {
    def createAndPublishToMemberService(implicit environment: Environment): Future[Tap[Nothing]] =
      self.write(CreateAndPublishMembers())
  }
}
