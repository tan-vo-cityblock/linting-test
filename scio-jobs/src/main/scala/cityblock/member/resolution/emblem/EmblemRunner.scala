package cityblock.member.resolution.emblem

import java.util.UUID

import cityblock.member.resolution.emblem.io.EmblemMemberDiscovery._
import cityblock.member.resolution.emblem.io.{EmblemCrosswalk, EmblemMembers}
import cityblock.member.resolution.models.NodeEntity
import com.spotify.scio.ScioContext
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions

object EmblemRunner {

  def main(cmdlineArgs: Array[String]): Unit = {
    val (opts, args) = ScioContext.parseArguments[GcpOptions](cmdlineArgs)

    val sourceTable = args.required("sourceTable")
    val sourceDataset = args.required("sourceDataset")
    val deliveryDate = args.required("deliveryDate")

    val destinationTable = args.optional("destinationTable")
    val destinationDataset = args.optional("destinationDataset")
    val destinationProject = args.optional("destinationProject")

    val tableShard = s"${sourceTable}_$deliveryDate"

    /* The First Step here is to Collect ALL THE IO from the Respective Models */

    val knownIds: Iterator[(NodeEntity[String], UUID)] = EmblemMembers
      .fetchData(project = opts.getProject)
      .map(_.toNodeEntity)

    val crosswalkIds: Iterator[Seq[NodeEntity[String]]] = EmblemCrosswalk
      .fetchData(project = opts.getProject, dataset = sourceDataset, tableShard = tableShard)
      .map(_.toNodeEntity)

    /* Next we build out the resolver and load the Members */
    val resolver: EmblemResolver = new EmblemResolver()

    resolver
      .addKnownIds(knownIds)
      .addCrosswalk(crosswalkIds)
      .buildIndex()
      .map {
        case (memberId, carrier, externalId) =>
          MemberDiscovery(memberId = memberId, externalId = externalId, carrier = carrier)
      }
      .writeToBQ(project = destinationProject,
                 dataset = destinationDataset,
                 table = destinationTable,
                 shard = Some(deliveryDate))

  }
}
